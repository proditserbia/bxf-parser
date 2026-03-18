"""
Parsers for Format A (ASTRA_DIFACE_LITE_v1_2) and Format B (Schedule XML).
"""
from __future__ import annotations

import json
import logging
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional

from .models import NormalizedEvent
from .utils import (
    detect_format,
    derive_end_time,
    elem_summary,
    get_param,
    get_text,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format A — classification keywords
# ---------------------------------------------------------------------------

_GRAPHICS_KEYWORDS = frozenset([
    "cg", "logo", "bug", "dsk", "txt", "inserter", "lower", "third",
    "graphic", "branding", "ident", "squeeze", "clock", "text",
    "caption", "ticker", "crawl", "overlay",
])

_LIVE_KEYWORDS = frozenset([
    "in", "input", "router", "live", "source", "feed", "ingest",
    "encoder", "decoder", "sdi", "ip", "cam", "camera",
])

_PROGRAMME_KEYWORDS = frozenset([
    "pro", "playout", "clip", "file", "media", "vtr", "server",
    "avid", "isis", "k2", "viz", "vcs",
])


def _contains_any(text: str, keywords: frozenset) -> bool:
    t = text.lower()
    return any(kw in t for kw in keywords)


# ---------------------------------------------------------------------------
# Format A parser
# ---------------------------------------------------------------------------

def classify_event_format_a(
    devtype: str,
    name: str,
    device: str,
    mat_path: str,
    par_type: str,
) -> tuple[str, str, bool, bool, bool]:
    """
    Returns (event_class, event_type, is_graphics, is_live, is_main).
    """
    dt = devtype.strip().upper()

    if dt == "PRO":
        return "PROGRAMME", "playout_clip", False, False, True
    if dt == "CG":
        return "GRAPHICS", "cg_graphic", True, False, False
    if dt == "IN":
        return "LIVE_INPUT", "live_input", False, True, False

    # Fallback heuristics
    for text in (name, device, mat_path, par_type):
        if _contains_any(text, _GRAPHICS_KEYWORDS):
            return "GRAPHICS", "graphic_heuristic", True, False, False
        if _contains_any(text, _LIVE_KEYWORDS):
            return "LIVE_INPUT", "live_heuristic", False, True, False
        if _contains_any(text, _PROGRAMME_KEYWORDS):
            return "PROGRAMME", "programme_heuristic", False, False, True

    return "OTHER", "unknown", False, False, False


def parse_format_a(
    path: Path,
    only_key_events: bool = True,
    flatten_graphics: bool = False,
) -> List[NormalizedEvent]:
    """Parse ASTRA_DIFACE_LITE_v1_2 XML file."""
    logger.info("Parsing Format A: %s", path.name)
    try:
        tree = ET.parse(str(path))
    except ET.ParseError as exc:
        logger.error("XML parse error in %s: %s", path.name, exc)
        return []

    root = tree.getroot()
    plan = root.find("Plan")
    channel = get_text(plan, "Channel") if plan is not None else ""
    broadcast_date = get_text(plan, "Date") if plan is not None else ""

    events_elem = root.find("Events")
    if events_elem is None:
        logger.warning("No <Events> element found in %s", path.name)
        return []

    results: List[NormalizedEvent] = []

    for event_elem in events_elem.findall("Event"):
        job_id = get_text(event_elem, "JobId")
        name = get_text(event_elem, "Name")
        name2 = get_text(event_elem, "Name2")
        devtype = get_text(event_elem, "DevType")
        par_type = get_text(event_elem, "ParType")
        device = get_text(event_elem, "Device")
        mat_path = get_text(event_elem, "MatPath")
        src_mat = get_text(event_elem, "SrcMatPath")
        real_start = get_text(event_elem, "RealStart")
        real_dur = get_text(event_elem, "RealDuration")
        astra_id = get_text(event_elem, "AstraId")
        signal_type = get_text(event_elem, "SignalType")
        movie_type = get_text(event_elem, "MovieType")
        in_addr = get_text(event_elem, "InAddr")
        out_addr = get_text(event_elem, "OutAddr")
        note = get_text(event_elem, "Note")
        onair_state = get_text(event_elem, "OnAirState")
        transition = get_text(event_elem, "Transition")
        clip_status = get_text(event_elem, "ClipStatus")
        err_status = get_text(event_elem, "ErrStatus")
        event_type_raw = get_text(event_elem, "EventType")
        start_type = get_text(event_elem, "StartType")
        crit1 = get_text(event_elem, "Crit1")
        crit2 = get_text(event_elem, "Crit2")
        crit3 = get_text(event_elem, "Crit3")
        crit4 = get_text(event_elem, "Crit4")

        # Key event filter
        dt_upper = devtype.upper()
        is_key = (
            dt_upper in {"PRO", "CG", "IN"}
            or bool(name or job_id or mat_path)
        )
        if only_key_events and not is_key:
            logger.debug("Skipping non-key event job_id=%s devtype=%s", job_id, devtype)
            continue

        event_class, ev_type, is_graphics, is_live, is_main = classify_event_format_a(
            devtype, name, device, mat_path, par_type
        )

        material_id = mat_path or src_mat
        event_id = astra_id or job_id or str(uuid.uuid4())

        end_time = derive_end_time(real_start, real_dur)
        status = err_status or clip_status

        raw_summary = json.dumps(
            elem_summary(event_elem), ensure_ascii=False
        )

        ev = NormalizedEvent(
            source_file=path.name,
            source_format="astra_diface_lite",
            channel=channel,
            broadcast_date=broadcast_date,
            event_id=event_id,
            parent_event_id="",
            event_class=event_class,
            event_type=ev_type,
            event_kind="",
            title=name,
            secondary_title=name2,
            material_id=material_id,
            job_id=job_id,
            media_path=mat_path,
            device=device,
            playout_device="",
            start_time=real_start,
            end_time=end_time,
            duration=real_dur,
            status=status,
            onair_state=onair_state,
            transition=transition,
            is_graphics=is_graphics,
            is_live=is_live,
            is_main=is_main,
            crit1=crit1,
            crit2=crit2,
            crit3=crit3,
            crit4=crit4,
            note=note,
            raw_type=event_type_raw,
            raw_devtype=devtype,
            raw_par_type=par_type,
            raw_xml_summary=raw_summary,
        )
        results.append(ev)

    logger.info("Format A: extracted %d events from %s", len(results), path.name)
    return results


# ---------------------------------------------------------------------------
# Format B — classification keywords
# ---------------------------------------------------------------------------

_B_GRAPHICS_TOKENS = frozenset([
    "bug", "dsk", "txt", "graphic", "inserter", "logo",
    "lower", "third", "text", "cg", "overlay", "ident",
    "caption", "ticker", "crawl", "clock", "branding",
])

_B_LIVE_TOKENS = frozenset([
    "live", "input", "router", "source", "feed", "ingest",
    "encoder", "decoder", "sdi", "ip", "cam", "camera",
])


def classify_event_format_b(
    event_kind: str,
    raw_type: str,
    fq_type: str,
    device: str,
    playout_device: str,
    include_all_key: bool = False,
) -> Optional[tuple[str, str, bool, bool, bool]]:
    """
    Returns (event_class, event_type, is_graphics, is_live, is_main) or None to skip.
    """
    ek = event_kind.strip()
    combined = " ".join([raw_type, fq_type, device, playout_device]).lower()

    if ek == "MainEvent":
        return "PROGRAMME_CONTAINER", "main_event", False, False, True

    if ek == "MaterialEvent":
        return "PROGRAMME", "material_event", False, False, True

    # Graphics heuristics
    if _contains_any(combined, _B_GRAPHICS_TOKENS):
        return "GRAPHICS", "graphic_heuristic", True, False, False

    # Live / playout heuristics
    if _contains_any(combined, _B_LIVE_TOKENS):
        return "LIVE_INPUT", "live_heuristic", False, True, False

    if include_all_key:
        return "OTHER", "normal_child", False, False, False

    return None  # skip


def parse_format_b(
    path: Path,
    only_key_events: bool = True,
    flatten_graphics: bool = False,
    include_all_key: bool = False,
) -> List[NormalizedEvent]:
    """Parse Schedule XML (Format B) file."""
    logger.info("Parsing Format B: %s", path.name)
    try:
        tree = ET.parse(str(path))
    except ET.ParseError as exc:
        logger.error("XML parse error in %s: %s", path.name, exc)
        return []

    root = tree.getroot()

    # Build parent->title map for flatten_graphics
    uid_to_title: dict[str, str] = {}

    results: List[NormalizedEvent] = []

    events_container = root.find("Events")
    if events_container is None:
        # root might directly be Events or Schedule has nested structure
        if root.tag == "Events":
            events_container = root
        else:
            logger.warning("No <Events> element found in %s", path.name)
            return []

    channel = events_container.get("Channel", "")
    broadcast_date = ""

    for event_elem in events_container.findall("Event"):
        uid = event_elem.get("Uid", "")
        raw_type = event_elem.get("Type", "")
        fq_type = event_elem.get("FullyQualifiedType", "")
        notional_start = event_elem.get("NotionalStartTime", "")
        notional_dur = event_elem.get("NotionalDuration", "")
        created = event_elem.get("Created", "")

        event_kind = get_text(event_elem, "EventKind")
        owner_uid = get_text(event_elem, "OwnerUid")
        schedule_name = get_text(event_elem, "ScheduleName")

        fields_elem = event_elem.find("Fields")

        device = get_param(fields_elem, "Device")
        duration_param = get_param(fields_elem, "Duration")
        event_name = get_param(fields_elem, "EventName")
        material_id_param = get_param(fields_elem, "MaterialId")
        media_id = get_param(fields_elem, "MediaID")
        playout_device = get_param(fields_elem, "PlayoutDevice")
        start_mode = get_param(fields_elem, "StartMode")
        as_run_start = get_param(fields_elem, "AsRunStartTime")
        as_run_dur = get_param(fields_elem, "AsRunDuration")
        as_run_end = get_param(fields_elem, "AsRunEndTime")
        kernel_status = get_param(fields_elem, "AsRunKernelStatusCode")
        as_run_started_onair = get_param(fields_elem, "AsRunStartedOnAir")
        as_run_finished_onair = get_param(fields_elem, "AsRunFinishedOnAir")
        transition_type = get_param(fields_elem, "TransitionType")
        transition_dur = get_param(fields_elem, "TransitionDuration")

        # Derive broadcast_date from first start time found
        if not broadcast_date:
            ts = as_run_start or notional_start
            if ts and len(ts) >= 10:
                broadcast_date = ts[:10]

        title = event_name or raw_type
        material_id = material_id_param or media_id
        start_time = as_run_start or notional_start
        duration = as_run_dur or duration_param or notional_dur
        end_time = as_run_end or derive_end_time(start_time, duration)

        onair_state_parts = []
        if as_run_started_onair:
            onair_state_parts.append(f"started={as_run_started_onair}")
        if as_run_finished_onair:
            onair_state_parts.append(f"finished={as_run_finished_onair}")
        onair_state = "; ".join(onair_state_parts)

        transition = ""
        if transition_type or transition_dur:
            transition = f"{transition_type}:{transition_dur}".strip(":")

        classification = classify_event_format_b(
            event_kind, raw_type, fq_type, device, playout_device,
            include_all_key=include_all_key,
        )

        if only_key_events and classification is None:
            logger.debug("Skipping non-key event uid=%s kind=%s", uid, event_kind)
            continue

        if classification is None:
            event_class, ev_type, is_graphics, is_live, is_main = (
                "OTHER", "skipped_but_included", False, False, False
            )
        else:
            event_class, ev_type, is_graphics, is_live, is_main = classification

        # Track UID->title for flatten_graphics
        if is_main or event_class in ("PROGRAMME_CONTAINER", "PROGRAMME"):
            uid_to_title[uid] = title

        # Flatten: inherit parent title
        effective_title = title
        if flatten_graphics and is_graphics and owner_uid:
            parent_title = uid_to_title.get(owner_uid, "")
            if parent_title:
                effective_title = f"{parent_title} / {title}" if title else parent_title

        # Validate owner_uid — blank if it refers to itself or is empty
        parent_event_id = owner_uid if owner_uid and owner_uid != uid else ""

        raw_summary = json.dumps(
            elem_summary(event_elem), ensure_ascii=False
        )

        ev = NormalizedEvent(
            source_file=path.name,
            source_format="schedule_xml",
            channel=channel,
            broadcast_date=broadcast_date,
            event_id=uid or str(uuid.uuid4()),
            parent_event_id=parent_event_id,
            event_class=event_class,
            event_type=ev_type,
            event_kind=event_kind,
            title=effective_title,
            secondary_title=fq_type,
            material_id=material_id,
            job_id="",
            media_path=material_id,
            device=device,
            playout_device=playout_device,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            status=kernel_status,
            onair_state=onair_state,
            transition=transition,
            is_graphics=is_graphics,
            is_live=is_live,
            is_main=is_main,
            crit1="",
            crit2="",
            crit3="",
            crit4="",
            note=schedule_name,
            raw_type=raw_type,
            raw_devtype="",
            raw_par_type=start_mode,
            raw_xml_summary=raw_summary,
        )
        results.append(ev)

    logger.info("Format B: extracted %d events from %s", len(results), path.name)
    return results


# ---------------------------------------------------------------------------
# Public dispatch
# ---------------------------------------------------------------------------

def parse_file(
    path: Path,
    only_key_events: bool = True,
    flatten_graphics: bool = False,
    include_all_key: bool = False,
) -> List[NormalizedEvent]:
    """Auto-detect format and parse the given file."""
    try:
        tree = ET.parse(str(path))
    except ET.ParseError as exc:
        logger.error("Cannot parse XML from %s: %s", path.name, exc)
        return []

    root = tree.getroot()
    fmt = detect_format(root)
    logger.info("Detected format '%s' for %s", fmt, path.name)

    if fmt == "astra_diface_lite":
        return parse_format_a(path, only_key_events, flatten_graphics)
    if fmt == "schedule_xml":
        return parse_format_b(path, only_key_events, flatten_graphics, include_all_key)

    logger.warning("Unknown format for %s — attempting both parsers", path.name)
    events = parse_format_a(path, only_key_events, flatten_graphics)
    if not events:
        events = parse_format_b(path, only_key_events, flatten_graphics, include_all_key)
    return events
