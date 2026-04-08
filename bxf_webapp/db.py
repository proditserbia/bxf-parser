"""
Database layer — SQLite schema, connection helpers, and CRUD operations.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, List, Optional

# Column names mirroring bxf_parser.models.COLUMNS
_EVENT_COLS = [
    "source_file", "source_format", "channel", "broadcast_date",
    "event_id", "parent_event_id", "event_class", "event_type",
    "event_kind", "title", "secondary_title", "material_id", "job_id",
    "media_path", "device", "playout_device", "start_time", "end_time",
    "duration", "status", "onair_state", "transition",
    "is_graphics", "is_live", "is_main",
    "crit1", "crit2", "crit3", "crit4",
    "note", "raw_type", "raw_devtype", "raw_par_type", "raw_xml_summary",
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file   TEXT,
    source_format TEXT,
    channel       TEXT,
    broadcast_date TEXT,
    event_id      TEXT,
    parent_event_id TEXT,
    event_class   TEXT,
    event_type    TEXT,
    event_kind    TEXT,
    title         TEXT,
    secondary_title TEXT,
    material_id   TEXT,
    job_id        TEXT,
    media_path    TEXT,
    device        TEXT,
    playout_device TEXT,
    start_time    TEXT,
    end_time      TEXT,
    duration      TEXT,
    status        TEXT,
    onair_state   TEXT,
    transition    TEXT,
    is_graphics   INTEGER,
    is_live       INTEGER,
    is_main       INTEGER,
    crit1         TEXT,
    crit2         TEXT,
    crit3         TEXT,
    crit4         TEXT,
    note          TEXT,
    raw_type      TEXT,
    raw_devtype   TEXT,
    raw_par_type  TEXT,
    raw_xml_summary TEXT,
    ingested_at   TEXT,
    ingest_source TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_channel        ON events (channel);
CREATE INDEX IF NOT EXISTS idx_events_broadcast_date ON events (broadcast_date);
CREATE INDEX IF NOT EXISTS idx_events_event_class    ON events (event_class);
CREATE INDEX IF NOT EXISTS idx_events_title          ON events (title);
CREATE INDEX IF NOT EXISTS idx_events_source_file    ON events (source_file);

CREATE TABLE IF NOT EXISTS ingest_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT,
    ingested_at  TEXT,
    events_count INTEGER,
    status       TEXT,
    message      TEXT
);
"""


def init_db(db_path: str) -> None:
    """Create tables and indexes if they do not exist."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_SCHEMA)
        conn.commit()


@contextmanager
def get_conn(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """Yield a SQLite connection with row_factory set to Row."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------

def create_user(db_path: str, username: str, password_hash: str) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash) VALUES (?, ?)",
            (username, password_hash),
        )


def get_user(db_path: str, username: str) -> Optional[sqlite3.Row]:
    with get_conn(db_path) as conn:
        return conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()


# ---------------------------------------------------------------------------
# Event helpers
# ---------------------------------------------------------------------------

def insert_events(
    db_path: str,
    events: list,
    ingest_source: str = "upload",
) -> int:
    """Insert NormalizedEvent objects; return count inserted."""
    if not events:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    cols = _EVENT_COLS + ["ingested_at", "ingest_source"]
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT INTO events ({', '.join(cols)}) VALUES ({placeholders})"

    rows = []
    for ev in events:
        d = ev.as_dict()
        row = [
            int(d[c]) if c in ("is_graphics", "is_live", "is_main") else d[c]
            for c in _EVENT_COLS
        ] + [now, ingest_source]
        rows.append(row)

    with get_conn(db_path) as conn:
        conn.executemany(sql, rows)
    return len(rows)


def search_events(
    db_path: str,
    q: str = "",
    channel: str = "",
    broadcast_date: str = "",
    event_class: str = "",
    source_format: str = "",
    limit: int = 200,
    offset: int = 0,
) -> tuple[List[sqlite3.Row], int]:
    """
    Full-text + filter search.  Returns (rows, total_count).
    """
    conditions: list[str] = []
    params: list[Any] = []

    if q:
        conditions.append(
            "(title LIKE ? OR secondary_title LIKE ? OR material_id LIKE ? OR note LIKE ?)"
        )
        like = f"%{q}%"
        params += [like, like, like, like]
    if channel:
        conditions.append("channel = ?")
        params.append(channel)
    if broadcast_date:
        conditions.append("broadcast_date = ?")
        params.append(broadcast_date)
    if event_class:
        conditions.append("event_class = ?")
        params.append(event_class)
    if source_format:
        conditions.append("source_format = ?")
        params.append(source_format)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn(db_path) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM events {where}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM events {where} "
            "ORDER BY broadcast_date DESC, start_time DESC "
            "LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

    return rows, total


def distinct_channels(db_path: str) -> List[str]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT channel FROM events WHERE channel != '' ORDER BY channel"
        ).fetchall()
    return [r[0] for r in rows]


def distinct_event_classes(db_path: str) -> List[str]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT event_class FROM events ORDER BY event_class"
        ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Ingest log helpers
# ---------------------------------------------------------------------------

def log_ingest(
    db_path: str,
    filename: str,
    events_count: int,
    status: str,
    message: str = "",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO ingest_log (filename, ingested_at, events_count, status, message) "
            "VALUES (?, ?, ?, ?, ?)",
            (filename, now, events_count, status, message),
        )


def get_ingest_log(db_path: str, limit: int = 100) -> List[sqlite3.Row]:
    with get_conn(db_path) as conn:
        return conn.execute(
            "SELECT * FROM ingest_log ORDER BY ingested_at DESC LIMIT ?", (limit,)
        ).fetchall()
