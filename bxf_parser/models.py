"""
Data model for normalized broadcast schedule events.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Optional


# Ordered list of CSV/XLSX column names (must match dataclass field names)
COLUMNS = [
    "source_file",
    "source_format",
    "channel",
    "broadcast_date",
    "event_id",
    "parent_event_id",
    "event_class",
    "event_type",
    "event_kind",
    "title",
    "secondary_title",
    "material_id",
    "job_id",
    "media_path",
    "device",
    "playout_device",
    "start_time",
    "end_time",
    "duration",
    "status",
    "onair_state",
    "transition",
    "is_graphics",
    "is_live",
    "is_main",
    "crit1",
    "crit2",
    "crit3",
    "crit4",
    "note",
    "raw_type",
    "raw_devtype",
    "raw_par_type",
    "raw_xml_summary",
]


@dataclass
class NormalizedEvent:
    """Single normalized playout / schedule event row."""

    source_file: str = ""
    source_format: str = ""
    channel: str = ""
    broadcast_date: str = ""
    event_id: str = ""
    parent_event_id: str = ""
    # PROGRAMME / GRAPHICS / LIVE_INPUT / PLAYOUT / OTHER / PROGRAMME_CONTAINER
    event_class: str = "OTHER"
    event_type: str = ""
    event_kind: str = ""
    title: str = ""
    secondary_title: str = ""
    material_id: str = ""
    job_id: str = ""
    media_path: str = ""
    device: str = ""
    playout_device: str = ""
    start_time: str = ""
    end_time: str = ""
    duration: str = ""
    status: str = ""
    onair_state: str = ""
    transition: str = ""
    is_graphics: bool = False
    is_live: bool = False
    is_main: bool = False
    crit1: str = ""
    crit2: str = ""
    crit3: str = ""
    crit4: str = ""
    note: str = ""
    raw_type: str = ""
    raw_devtype: str = ""
    raw_par_type: str = ""
    raw_xml_summary: str = ""

    def as_dict(self) -> dict:
        """Return ordered dict matching COLUMNS."""
        return {f.name: getattr(self, f.name) for f in fields(self)}
