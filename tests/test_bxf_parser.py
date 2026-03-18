"""
Tests for bxf_parser package.
"""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

import pytest

# Ensure package is importable from repository root
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from bxf_parser.models import NormalizedEvent, COLUMNS
from bxf_parser.utils import (
    detect_format,
    parse_timecode_to_seconds,
    seconds_to_timecode,
    derive_end_time,
    get_text,
    get_param,
)
from bxf_parser.parsers import (
    classify_event_format_a,
    classify_event_format_b,
    parse_format_a,
    parse_format_b,
    parse_file,
)
from bxf_parser.exporters import export_csv, export_xlsx
from bxf_parser.bxf_parser import main as cli_main

DATA = Path(__file__).parent / "data"
FORMAT_A_XML = DATA / "sample_format_a.xml"
FORMAT_B_XML = DATA / "sample_format_b.xml"


# ---------------------------------------------------------------------------
# utils tests
# ---------------------------------------------------------------------------

class TestDetectFormat:
    def test_detects_format_a(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring("<ASTRA_DIFACE_LITE_v1_2/>")
        assert detect_format(root) == "astra_diface_lite"

    def test_detects_format_b(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring("<Schedule/>")
        assert detect_format(root) == "schedule_xml"

    def test_unknown_format(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring("<SomethingElse/>")
        assert detect_format(root) == "unknown"


class TestTimecodeParser:
    def test_hh_mm_ss_ff(self):
        secs = parse_timecode_to_seconds("01:00:00:00")
        assert secs == pytest.approx(3600.0)

    def test_hh_mm_ss_ff_with_frames(self):
        secs = parse_timecode_to_seconds("00:00:01:25", fps=25.0)
        assert secs == pytest.approx(2.0)

    def test_hh_mm_ss(self):
        secs = parse_timecode_to_seconds("00:30:00")
        assert secs == pytest.approx(1800.0)

    def test_iso_datetime(self):
        secs = parse_timecode_to_seconds("2026-01-05 20:00:05.000")
        assert secs is not None
        assert secs > 0

    def test_oracle_date(self):
        secs = parse_timecode_to_seconds("05-JAN-2026 04:40:11:20")
        assert secs is not None

    def test_empty_string(self):
        assert parse_timecode_to_seconds("") is None

    def test_garbage(self):
        assert parse_timecode_to_seconds("not_a_time") is None

    def test_seconds_to_timecode(self):
        assert seconds_to_timecode(3600.0, 25.0) == "01:00:00:00"

    def test_derive_end_time_timecode(self):
        end = derive_end_time("04:00:00:00", "01:30:00:00")
        assert end == "05:30:00:00"

    def test_derive_end_time_empty(self):
        assert derive_end_time("", "01:00:00:00") == ""
        assert derive_end_time("04:00:00:00", "") == ""


class TestGetHelpers:
    def test_get_text(self):
        import xml.etree.ElementTree as ET
        parent = ET.fromstring("<Parent><Child>  hello  </Child></Parent>")
        assert get_text(parent, "Child") == "hello"

    def test_get_text_missing(self):
        import xml.etree.ElementTree as ET
        parent = ET.fromstring("<Parent/>")
        assert get_text(parent, "Missing") == ""

    def test_get_param(self):
        import xml.etree.ElementTree as ET
        fields = ET.fromstring(
            '<Fields><Parameter Name="Device" Value="SERVER1"/></Fields>'
        )
        assert get_param(fields, "Device") == "SERVER1"

    def test_get_param_missing(self):
        import xml.etree.ElementTree as ET
        fields = ET.fromstring("<Fields/>")
        assert get_param(fields, "Device") == ""


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------

class TestClassifyFormatA:
    def test_pro_devtype(self):
        ec, et, ig, il, im = classify_event_format_a("PRO", "", "", "", "")
        assert ec == "PROGRAMME"
        assert im is True
        assert ig is False

    def test_cg_devtype(self):
        ec, et, ig, il, im = classify_event_format_a("CG", "", "", "", "")
        assert ec == "GRAPHICS"
        assert ig is True

    def test_in_devtype(self):
        ec, et, ig, il, im = classify_event_format_a("IN", "", "", "", "")
        assert ec == "LIVE_INPUT"
        assert il is True

    def test_fallback_graphic_keyword_in_name(self):
        ec, et, ig, il, im = classify_event_format_a("", "Channel Logo", "", "", "")
        assert ec == "GRAPHICS"

    def test_fallback_live_keyword_in_device(self):
        ec, et, ig, il, im = classify_event_format_a("", "", "live_camera_1", "", "")
        assert ec == "LIVE_INPUT"

    def test_fallback_other(self):
        ec, et, ig, il, im = classify_event_format_a("", "", "", "", "")
        assert ec == "OTHER"


class TestClassifyFormatB:
    def test_main_event(self):
        result = classify_event_format_b("MainEvent", "", "", "", "")
        assert result is not None
        ec, _, _, _, im = result
        assert ec == "PROGRAMME_CONTAINER"
        assert im is True

    def test_material_event(self):
        result = classify_event_format_b("MaterialEvent", "", "", "", "")
        assert result is not None
        ec, _, _, _, im = result
        assert ec == "PROGRAMME"

    def test_graphic_type(self):
        result = classify_event_format_b("Normal", "Graphic.BUG", "", "CG_INSERTER", "")
        assert result is not None
        ec, _, ig, _, _ = result
        assert ec == "GRAPHICS"
        assert ig is True

    def test_dsk_type(self):
        result = classify_event_format_b("Normal", "DSK.LowerThird", "", "", "")
        assert result is not None
        ec, _, ig, _, _ = result
        assert ec == "GRAPHICS"

    def test_skip_non_key(self):
        result = classify_event_format_b("Normal", "TechEvent", "", "SomeTechDevice", "")
        assert result is None

    def test_include_all_key_flag(self):
        result = classify_event_format_b(
            "Normal", "TechEvent", "", "SomeTechDevice", "", include_all_key=True
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Parser integration tests
# ---------------------------------------------------------------------------

class TestParseFormatA:
    def test_returns_list(self):
        events = parse_format_a(FORMAT_A_XML)
        assert isinstance(events, list)

    def test_extracts_key_events(self):
        events = parse_format_a(FORMAT_A_XML, only_key_events=True)
        # JOB005 has JobId="JOB005" (non-empty) so it IS included per spec
        # (Name/JobId/MatPath non-empty triggers key event inclusion)
        assert len(events) == 5

    def test_includes_all_when_not_key_only(self):
        events = parse_format_a(FORMAT_A_XML, only_key_events=False)
        assert len(events) == 5

    def test_source_format(self):
        events = parse_format_a(FORMAT_A_XML)
        assert all(e.source_format == "astra_diface_lite" for e in events)

    def test_channel_from_plan(self):
        events = parse_format_a(FORMAT_A_XML)
        assert all(e.channel == "SPORT1" for e in events)

    def test_broadcast_date(self):
        events = parse_format_a(FORMAT_A_XML)
        assert all(e.broadcast_date == "2026-01-05" for e in events)

    def test_pro_event_classified(self):
        events = parse_format_a(FORMAT_A_XML)
        pro_events = [e for e in events if e.raw_devtype == "PRO"]
        assert len(pro_events) == 2
        assert all(e.event_class == "PROGRAMME" for e in pro_events)
        assert all(e.is_main for e in pro_events)

    def test_cg_event_classified(self):
        events = parse_format_a(FORMAT_A_XML)
        cg_events = [e for e in events if e.raw_devtype == "CG"]
        assert len(cg_events) == 1
        assert cg_events[0].event_class == "GRAPHICS"
        assert cg_events[0].is_graphics

    def test_in_event_classified(self):
        events = parse_format_a(FORMAT_A_XML)
        in_events = [e for e in events if e.raw_devtype == "IN"]
        assert len(in_events) == 1
        assert in_events[0].event_class == "LIVE_INPUT"
        assert in_events[0].is_live

    def test_end_time_derived(self):
        events = parse_format_a(FORMAT_A_XML)
        pro = next(e for e in events if e.job_id == "JOB001")
        # start=04:00:00:00, dur=01:30:00:00 => end=05:30:00:00
        assert pro.end_time == "05:30:00:00"

    def test_raw_xml_summary_is_json(self):
        events = parse_format_a(FORMAT_A_XML)
        for e in events:
            assert json.loads(e.raw_xml_summary) is not None


class TestParseFormatB:
    def test_returns_list(self):
        events = parse_format_b(FORMAT_B_XML)
        assert isinstance(events, list)

    def test_extracts_key_events(self):
        events = parse_format_b(FORMAT_B_XML, only_key_events=True)
        # Should have: MainEvent(001), MaterialEvent(002), Graphic.BUG(003), DSK(004), MainEvent(005)
        assert len(events) == 5

    def test_source_format(self):
        events = parse_format_b(FORMAT_B_XML)
        assert all(e.source_format == "schedule_xml" for e in events)

    def test_channel_attribute(self):
        events = parse_format_b(FORMAT_B_XML)
        assert all(e.channel == "BBC1" for e in events)

    def test_main_event_classified(self):
        events = parse_format_b(FORMAT_B_XML)
        main_events = [e for e in events if e.event_class == "PROGRAMME_CONTAINER"]
        assert len(main_events) == 2  # EVT-001 and EVT-005

    def test_material_event_classified(self):
        events = parse_format_b(FORMAT_B_XML)
        mat_events = [e for e in events if e.event_class == "PROGRAMME"]
        assert len(mat_events) == 1  # EVT-002

    def test_graphic_classified(self):
        events = parse_format_b(FORMAT_B_XML)
        gfx = [e for e in events if e.is_graphics]
        assert len(gfx) == 2  # EVT-003 BUG and EVT-004 DSK

    def test_parent_event_id_set(self):
        events = parse_format_b(FORMAT_B_XML)
        mat = next(e for e in events if e.event_id == "EVT-002")
        assert mat.parent_event_id == "EVT-001"

    def test_title_from_parameter(self):
        events = parse_format_b(FORMAT_B_XML)
        ev = next(e for e in events if e.event_id == "EVT-001")
        assert ev.title == "The Drama Series EP01"

    def test_flatten_graphics(self):
        events = parse_format_b(FORMAT_B_XML, flatten_graphics=True)
        bug = next(e for e in events if e.event_id == "EVT-003")
        # Should inherit parent title
        assert "The Drama Series EP01" in bug.title

    def test_as_run_start_used(self):
        events = parse_format_b(FORMAT_B_XML)
        ev = next(e for e in events if e.event_id == "EVT-001")
        assert ev.start_time == "2026-01-05 20:00:05.123"

    def test_raw_xml_summary_is_json(self):
        events = parse_format_b(FORMAT_B_XML)
        for e in events:
            assert json.loads(e.raw_xml_summary) is not None


class TestParseFileDispatch:
    def test_auto_detect_format_a(self):
        events = parse_file(FORMAT_A_XML)
        assert len(events) > 0
        assert events[0].source_format == "astra_diface_lite"

    def test_auto_detect_format_b(self):
        events = parse_file(FORMAT_B_XML)
        assert len(events) > 0
        assert events[0].source_format == "schedule_xml"


# ---------------------------------------------------------------------------
# Exporter tests
# ---------------------------------------------------------------------------

class TestExportCsv:
    def test_creates_file(self, tmp_path):
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.csv"
        export_csv(events, out)
        assert out.exists()

    def test_has_header(self, tmp_path):
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.csv"
        export_csv(events, out)
        with open(out, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            assert set(COLUMNS).issubset(set(reader.fieldnames or []))

    def test_row_count(self, tmp_path):
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.csv"
        export_csv(events, out)
        with open(out, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == len(events)

    def test_boolean_values(self, tmp_path):
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.csv"
        export_csv(events, out)
        with open(out, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            assert row["is_graphics"] in ("TRUE", "FALSE")

    def test_empty_rows(self, tmp_path):
        out = tmp_path / "empty.csv"
        export_csv([], out)
        assert out.exists()


class TestExportXlsx:
    def test_creates_file(self, tmp_path):
        pytest.importorskip("openpyxl")
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.xlsx"
        export_xlsx(events, out)
        assert out.exists()

    def test_row_count(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.xlsx"
        export_xlsx(events, out)
        wb = openpyxl.load_workbook(str(out))
        ws = wb.active
        # row 1 = header, remaining = data
        assert ws.max_row == len(events) + 1

    def test_header_row(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        events = parse_format_a(FORMAT_A_XML)
        out = tmp_path / "output.xlsx"
        export_xlsx(events, out)
        wb = openpyxl.load_workbook(str(out))
        ws = wb.active
        headers = [ws.cell(row=1, column=i + 1).value for i in range(len(COLUMNS))]
        assert headers == COLUMNS


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestNormalizedEvent:
    def test_as_dict_has_all_columns(self):
        ev = NormalizedEvent()
        d = ev.as_dict()
        assert set(d.keys()) == set(COLUMNS)

    def test_defaults(self):
        ev = NormalizedEvent()
        assert ev.event_class == "OTHER"
        assert ev.is_graphics is False
        assert ev.is_live is False
        assert ev.is_main is False


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------

class TestCLIOutputFilenames:
    """Regression tests for per-channel output filename uniqueness."""

    def test_distinct_filenames_for_dot_suffixed_channels(self, tmp_path):
        """Each channel file (e.g. E2602280.TV-1, E2602280.TV-2) must produce
        a distinct XLSX file; they must not all overwrite the same E2602280.xlsx."""
        out_dir = tmp_path / "out"
        # Copy the same sample into two files that share a base name but differ
        # in their dot-separated channel suffix, mimicking E2602280.TV-1 / .TV-2
        src = FORMAT_A_XML
        ch1 = tmp_path / "E2602280.TV-1"
        ch2 = tmp_path / "E2602280.TV-2"
        ch1.write_bytes(src.read_bytes())
        ch2.write_bytes(src.read_bytes())

        rc = cli_main([str(tmp_path), "--out", str(out_dir), "--output-format", "xlsx"])
        assert rc == 0

        xlsx_files = sorted(out_dir.glob("*.xlsx"))
        stems = {p.name for p in xlsx_files}
        assert "E2602280.TV-1.xlsx" in stems, "per-channel file for TV-1 missing"
        assert "E2602280.TV-2.xlsx" in stems, "per-channel file for TV-2 missing"
        assert "E2602280.xlsx" not in stems, "colliding combined stem must not appear"
        assert "combined_all.xlsx" in stems

    def test_cli_done_summary_log_message(self, tmp_path, caplog):
        """The final summary log message must say 'Done — N total events written to DIR'."""
        out_dir = tmp_path / "out"
        ch1 = tmp_path / "E2602280.TV-1"
        ch1.write_bytes(FORMAT_A_XML.read_bytes())

        with caplog.at_level(logging.INFO, logger="bxf_parser"):
            cli_main([str(tmp_path), "--out", str(out_dir), "--output-format", "xlsx"])

        done_messages = [r.message for r in caplog.records if r.message.startswith("Done —")]
        assert done_messages, "Expected a 'Done — …' summary log message"
        assert str(out_dir) in done_messages[0]
