"""
Utility helpers: timecode / duration parsing, format detection.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional
import xml.etree.ElementTree as ET


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def detect_format(root: ET.Element) -> str:
    """Return 'astra_diface_lite' or 'schedule_xml' or 'unknown'."""
    tag = root.tag or ""
    if tag.startswith("ASTRA_DIFACE_LITE"):
        return "astra_diface_lite"
    if tag == "Schedule":
        return "schedule_xml"
    # Fallback: look for characteristic child elements
    if root.find("Events/Event") is not None:
        # If Event has DevType child -> format A-like
        ev = root.find("Events/Event")
        if ev is not None and ev.find("DevType") is not None:
            return "astra_diface_lite"
        if ev is not None and ev.find("EventKind") is not None:
            return "schedule_xml"
    return "unknown"


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _t(elem: Optional[ET.Element]) -> str:
    """Safe text extraction from an Element, stripped."""
    if elem is None:
        return ""
    return (elem.text or "").strip()


def get_text(parent: ET.Element, tag: str) -> str:
    """Get stripped text of first child with *tag*, or ''."""
    return _t(parent.find(tag))


def get_param(fields_elem: Optional[ET.Element], name: str) -> str:
    """Get Value attribute of <Parameter Name=name> inside <Fields>."""
    if fields_elem is None:
        return ""
    for param in fields_elem.findall("Parameter"):
        if param.get("Name", "") == name:
            return (param.get("Value") or "").strip()
    return ""


def elem_summary(elem: ET.Element, max_attrs: int = 8) -> dict:
    """Return a compact dict of an element's attributes + key child texts."""
    summary: dict = dict(list(elem.attrib.items())[:max_attrs])
    for child in list(elem)[:max_attrs]:
        text = (child.text or "").strip()
        if text:
            summary[child.tag] = text[:120]
    return summary


# ---------------------------------------------------------------------------
# Duration / timecode parsing
# ---------------------------------------------------------------------------

# HH:MM:SS:FF  (frames last component)
_TC_HH_MM_SS_FF = re.compile(
    r"^(\d{1,3}):(\d{2}):(\d{2}):(\d{2})$"
)
# HH:MM:SS.mmm or HH:MM:SS
_TC_HH_MM_SS = re.compile(
    r"^(\d{1,3}):(\d{2}):(\d{2})(?:[.,](\d+))?$"
)
# YYYY-MM-DD HH:MM:SS.mmm
_DT_ISO = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d+))?"
)
# DD-MON-YYYY HH:MM:SS:FF  e.g. 05-JAN-2026 04:40:11:20
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_DT_ORACLE = re.compile(
    r"(\d{2})-([A-Z]{3})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})(?::(\d{2}))?"
)
# Pure integer frames (e.g. Astra sometimes stores frame counts)
_INT_FRAMES = re.compile(r"^(\d+)$")


def parse_timecode_to_seconds(value: str, fps: float = 25.0) -> Optional[float]:
    """
    Parse a timecode / timestamp string into total seconds.
    Returns None if unparseable.
    """
    if not value:
        return None
    value = value.strip()

    m = _TC_HH_MM_SS_FF.match(value)
    if m:
        h, mn, s, ff = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        return h * 3600 + mn * 60 + s + ff / fps

    m = _TC_HH_MM_SS.match(value)
    if m:
        h, mn, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
        frac = float("0." + m.group(4)) if m.group(4) else 0.0
        return h * 3600 + mn * 60 + s + frac

    m = _DT_ISO.search(value)
    if m:
        try:
            dt = datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6)),
            )
            return dt.timestamp()
        except ValueError:
            return None

    m = _DT_ORACLE.match(value)
    if m:
        day = int(m.group(1))
        mon = _MONTHS.get(m.group(2).upper(), 0)
        year = int(m.group(3))
        h, mn, s = int(m.group(4)), int(m.group(5)), int(m.group(6))
        ff = int(m.group(7)) if m.group(7) else 0
        if mon == 0:
            return None
        try:
            dt = datetime(year, mon, day, h, mn, s)
            return dt.timestamp() + ff / fps
        except ValueError:
            return None

    m = _INT_FRAMES.match(value)
    if m:
        return int(m.group(1)) / fps

    return None


def seconds_to_timecode(secs: float, fps: float = 25.0) -> str:
    """Convert seconds to HH:MM:SS:FF string."""
    secs = max(0.0, secs)
    total_frames = round(secs * fps)
    ff = total_frames % int(fps)
    total_secs = total_frames // int(fps)
    s = total_secs % 60
    total_mins = total_secs // 60
    mn = total_mins % 60
    h = total_mins // 60
    return f"{h:02d}:{mn:02d}:{s:02d}:{ff:02d}"


def derive_end_time(start: str, duration: str, fps: float = 25.0) -> str:
    """Return end timecode derived from start + duration, or '' on failure."""
    try:
        s_sec = parse_timecode_to_seconds(start, fps)
        d_sec = parse_timecode_to_seconds(duration, fps)
        if s_sec is None or d_sec is None:
            return ""
        # If start looks like a wall-clock timestamp (large number), produce ISO
        if s_sec > 1_000_000:
            end_dt = datetime.fromtimestamp(s_sec + d_sec)
            return end_dt.strftime("%Y-%m-%d %H:%M:%S")
        return seconds_to_timecode(s_sec + d_sec, fps)
    except Exception:
        return ""
