"""
Ingest logic — parse an XML file and store events in SQLite.
Also provides IngestWatcher, a daemon thread that polls a watched directory.
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from bxf_parser.parsers import parse_file

from .db import insert_events, log_ingest

logger = logging.getLogger(__name__)

_XML_SUFFIXES = {".xml", ".sch"}


def ingest_file(
    path: Path,
    db_path: str,
    ingest_source: str = "upload",
    only_key_events: bool = True,
    flatten_graphics: bool = False,
    include_all_key: bool = False,
) -> int:
    """
    Parse *path* and persist the extracted events.

    Returns the number of events stored.
    Logs to ingest_log on both success and failure.
    """
    try:
        events = parse_file(
            path,
            only_key_events=only_key_events,
            flatten_graphics=flatten_graphics,
            include_all_key=include_all_key,
        )
        count = insert_events(db_path, events, ingest_source=ingest_source)
        log_ingest(db_path, path.name, count, "ok")
        logger.info("Ingested %d events from '%s'", count, path.name)
        return count
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        log_ingest(db_path, path.name, 0, "error", msg)
        logger.error("Ingest failed for '%s': %s", path.name, msg)
        return 0


class IngestWatcher(threading.Thread):
    """
    Background daemon thread that polls *watch_dir* every *interval* seconds
    and ingests any XML file that has not been seen before.
    """

    def __init__(self, watch_dir: str, db_path: str, interval: int = 30) -> None:
        super().__init__(daemon=True, name="IngestWatcher")
        self._watch_dir = Path(watch_dir)
        self._db_path = db_path
        self._interval = interval
        self._seen: set[Path] = set()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info(
            "IngestWatcher started — watching '%s' every %ds",
            self._watch_dir,
            self._interval,
        )
        while not self._stop_event.is_set():
            self._scan()
            self._stop_event.wait(timeout=self._interval)
        logger.info("IngestWatcher stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _scan(self) -> None:
        if not self._watch_dir.is_dir():
            logger.warning("Watch directory '%s' does not exist", self._watch_dir)
            return
        for candidate in sorted(self._watch_dir.iterdir()):
            if not candidate.is_file():
                continue
            suffix = candidate.suffix.lower()
            # Accept known XML suffixes or extensionless files (e.g. E2602280.TV-1)
            if suffix and suffix not in _XML_SUFFIXES:
                continue
            if candidate in self._seen:
                continue
            self._seen.add(candidate)
            ingest_file(candidate, self._db_path, ingest_source="watch")
