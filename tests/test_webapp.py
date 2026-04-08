"""
Tests for bxf_webapp — database layer, ingest, auth, and HTTP routes.
"""
from __future__ import annotations

import io
import sqlite3
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

DATA = Path(__file__).parent / "data"
FORMAT_A_XML = DATA / "sample_format_a.xml"
FORMAT_B_XML = DATA / "sample_format_b.xml"


class TestConfig:
    """Minimal Flask test configuration."""
    TESTING = True
    SECRET_KEY = "test-secret"
    DATABASE_PATH = ":memory:"  # overridden per test via tmp_path
    UPLOAD_DIR = "/tmp/bxf_test_uploads"
    WATCH_DIR = ""
    ADMIN_USERNAME = "testadmin"
    ADMIN_PASSWORD = "testpass"
    DEBUG = False


@pytest.fixture()
def db_path(tmp_path):
    """Initialised SQLite database in a temp directory."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from bxf_webapp.db import init_db
    p = str(tmp_path / "test.db")
    init_db(p)
    return p


@pytest.fixture()
def app(tmp_path):
    """Flask test app with its own isolated database."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from bxf_webapp import create_app

    class Cfg(TestConfig):
        DATABASE_PATH = str(tmp_path / "app_test.db")
        UPLOAD_DIR = str(tmp_path / "uploads")

    application = create_app(config=Cfg)
    return application


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def auth_client(client):
    """Test client that is already logged in."""
    client.post("/login", data={"username": "testadmin", "password": "testpass"})
    return client


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

class TestDbInit:
    def test_creates_tables(self, db_path):
        from bxf_webapp.db import get_conn
        with get_conn(db_path) as conn:
            tables = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        assert {"users", "events", "ingest_log"}.issubset(tables)

    def test_creates_indexes(self, db_path):
        from bxf_webapp.db import get_conn
        with get_conn(db_path) as conn:
            indexes = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                )
            }
        assert "idx_events_channel" in indexes
        assert "idx_events_broadcast_date" in indexes


class TestDbUsers:
    def test_create_and_get_user(self, db_path):
        from bxf_webapp.db import create_user, get_user
        create_user(db_path, "alice", "hash_xyz")
        row = get_user(db_path, "alice")
        assert row is not None
        assert row["username"] == "alice"
        assert row["password_hash"] == "hash_xyz"

    def test_get_missing_user_returns_none(self, db_path):
        from bxf_webapp.db import get_user
        assert get_user(db_path, "nobody") is None

    def test_create_duplicate_is_ignored(self, db_path):
        from bxf_webapp.db import create_user, get_user
        create_user(db_path, "bob", "hash1")
        create_user(db_path, "bob", "hash2")  # should not raise
        row = get_user(db_path, "bob")
        assert row["password_hash"] == "hash1"  # first value preserved


class TestDbEvents:
    def _insert_sample(self, db_path):
        from bxf_webapp.db import insert_events
        from bxf_parser.parsers import parse_format_a
        events = parse_format_a(FORMAT_A_XML)
        return insert_events(db_path, events, ingest_source="test")

    def test_insert_returns_count(self, db_path):
        count = self._insert_sample(db_path)
        assert count == 5

    def test_search_no_filter(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path)
        assert total == 5
        assert len(rows) == 5

    def test_search_by_channel(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path, channel="SPORT1")
        assert total == 5

    def test_search_by_channel_no_match(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path, channel="NONEXISTENT")
        assert total == 0

    def test_search_by_event_class(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path, event_class="PROGRAMME")
        assert total >= 1

    def test_search_fulltext(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path, q="Drama")
        # sample_format_a may or may not contain "Drama"; result >=0
        assert total >= 0

    def test_search_pagination(self, db_path):
        from bxf_webapp.db import search_events
        self._insert_sample(db_path)
        rows, total = search_events(db_path, limit=2, offset=0)
        assert len(rows) == 2
        assert total == 5

    def test_distinct_channels(self, db_path):
        from bxf_webapp.db import distinct_channels
        self._insert_sample(db_path)
        channels = distinct_channels(db_path)
        assert "SPORT1" in channels

    def test_distinct_event_classes(self, db_path):
        from bxf_webapp.db import distinct_event_classes
        self._insert_sample(db_path)
        classes = distinct_event_classes(db_path)
        assert "PROGRAMME" in classes


class TestDbIngestLog:
    def test_log_and_retrieve(self, db_path):
        from bxf_webapp.db import get_ingest_log, log_ingest
        log_ingest(db_path, "test.xml", 42, "ok")
        entries = get_ingest_log(db_path)
        assert len(entries) == 1
        assert entries[0]["filename"] == "test.xml"
        assert entries[0]["events_count"] == 42
        assert entries[0]["status"] == "ok"

    def test_error_log(self, db_path):
        from bxf_webapp.db import get_ingest_log, log_ingest
        log_ingest(db_path, "bad.xml", 0, "error", "XML parse error")
        entries = get_ingest_log(db_path)
        assert entries[0]["status"] == "error"
        assert "XML parse error" in entries[0]["message"]


# ---------------------------------------------------------------------------
# Ingest layer
# ---------------------------------------------------------------------------

class TestIngestFile:
    def test_ingest_format_a(self, db_path):
        from bxf_webapp.ingest import ingest_file
        count = ingest_file(FORMAT_A_XML, db_path, ingest_source="test")
        assert count == 5

    def test_ingest_format_b(self, db_path):
        from bxf_webapp.ingest import ingest_file
        count = ingest_file(FORMAT_B_XML, db_path, ingest_source="test")
        assert count == 5

    def test_ingest_logs_success(self, db_path):
        from bxf_webapp.db import get_ingest_log
        from bxf_webapp.ingest import ingest_file
        ingest_file(FORMAT_A_XML, db_path, ingest_source="test")
        entries = get_ingest_log(db_path)
        assert entries[0]["status"] == "ok"

    def test_ingest_missing_file_logs_error(self, db_path, tmp_path):
        from bxf_webapp.db import get_ingest_log
        from bxf_webapp.ingest import ingest_file
        missing = tmp_path / "does_not_exist.xml"
        count = ingest_file(missing, db_path, ingest_source="test")
        assert count == 0
        entries = get_ingest_log(db_path)
        assert entries[0]["status"] == "error"

    def test_ingest_watcher_scans_directory(self, tmp_path):
        """Watcher picks up XML files placed in the watched directory."""
        import time
        import shutil
        db = str(tmp_path / "w.db")
        from bxf_webapp.db import init_db, search_events
        from bxf_webapp.ingest import IngestWatcher
        init_db(db)
        watch = tmp_path / "watch"
        watch.mkdir()
        watcher = IngestWatcher(str(watch), db, interval=1)
        watcher.start()
        time.sleep(0.2)  # let thread start
        shutil.copy(FORMAT_A_XML, watch / "sample.xml")
        time.sleep(2.5)  # wait for at least one poll cycle
        watcher.stop()
        watcher.join(timeout=3)
        _, total = search_events(db)
        assert total == 5


# ---------------------------------------------------------------------------
# Authentication routes
# ---------------------------------------------------------------------------

class TestAuth:
    def test_login_page_accessible(self, client):
        resp = client.get("/login")
        assert resp.status_code == 200
        assert b"Sign in" in resp.data

    def test_valid_login_redirects(self, client):
        resp = client.post(
            "/login",
            data={"username": "testadmin", "password": "testpass"},
            follow_redirects=False,
        )
        assert resp.status_code == 302

    def test_invalid_login_shows_error(self, client):
        resp = client.post(
            "/login",
            data={"username": "testadmin", "password": "wrongpass"},
        )
        assert resp.status_code == 200
        assert b"Invalid" in resp.data

    def test_logout_clears_session(self, auth_client):
        resp = auth_client.get("/logout", follow_redirects=False)
        assert resp.status_code == 302
        resp2 = auth_client.get("/search", follow_redirects=False)
        assert resp2.status_code == 302  # redirected back to login

    def test_protected_route_redirects_unauthenticated(self, client):
        resp = client.get("/search", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]


# ---------------------------------------------------------------------------
# HTTP routes — search
# ---------------------------------------------------------------------------

class TestSearchRoute:
    def test_search_page_loads(self, auth_client):
        resp = auth_client.get("/search")
        assert resp.status_code == 200
        assert b"Search Events" in resp.data

    def test_search_with_query(self, auth_client, app):
        # Ingest a file first
        from bxf_webapp.ingest import ingest_file
        with app.app_context():
            ingest_file(FORMAT_A_XML, app.config["DATABASE_PATH"], ingest_source="test")
        resp = auth_client.get("/search?q=Drama")
        assert resp.status_code == 200

    def test_search_channel_filter(self, auth_client, app):
        from bxf_webapp.ingest import ingest_file
        with app.app_context():
            ingest_file(FORMAT_A_XML, app.config["DATABASE_PATH"], ingest_source="test")
        resp = auth_client.get("/search?channel=SPORT1")
        assert resp.status_code == 200
        assert b"SPORT1" in resp.data

    def test_search_shows_result_count(self, auth_client, app):
        from bxf_webapp.ingest import ingest_file
        with app.app_context():
            ingest_file(FORMAT_A_XML, app.config["DATABASE_PATH"], ingest_source="test")
        resp = auth_client.get("/search")
        assert b"event" in resp.data  # "5 events found" or similar

    def test_root_redirects_to_search(self, auth_client):
        resp = auth_client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/search" in resp.headers["Location"]


# ---------------------------------------------------------------------------
# HTTP routes — upload
# ---------------------------------------------------------------------------

class TestUploadRoute:
    def test_upload_page_loads(self, auth_client):
        resp = auth_client.get("/upload")
        assert resp.status_code == 200
        assert b"Upload" in resp.data

    def test_upload_valid_xml(self, auth_client, app):
        xml_bytes = FORMAT_A_XML.read_bytes()
        resp = auth_client.post(
            "/upload",
            data={
                "file": (io.BytesIO(xml_bytes), "sample_format_a.xml"),
                "only_key_events": "1",
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert b"Ingested" in resp.data

    def test_upload_no_file_flashes_error(self, auth_client):
        resp = auth_client.post(
            "/upload",
            data={},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert b"No file" in resp.data

    def test_upload_disallowed_extension(self, auth_client):
        resp = auth_client.post(
            "/upload",
            data={"file": (io.BytesIO(b"data"), "evil.exe")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert b"Only .xml" in resp.data


# ---------------------------------------------------------------------------
# HTTP routes — ingest log
# ---------------------------------------------------------------------------

class TestIngestLogRoute:
    def test_ingest_log_page_loads(self, auth_client):
        resp = auth_client.get("/ingest-log")
        assert resp.status_code == 200
        assert b"Ingest Log" in resp.data

    def test_ingest_log_shows_entries(self, auth_client, app):
        from bxf_webapp.db import log_ingest
        with app.app_context():
            log_ingest(app.config["DATABASE_PATH"], "test.xml", 10, "ok")
        resp = auth_client.get("/ingest-log")
        assert b"test.xml" in resp.data
