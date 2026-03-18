"""
bxf_parser — CLI entry point.

Usage examples:
  python -m bxf_parser.bxf_parser input.xml --out ./out
  python -m bxf_parser.bxf_parser ./schedules --out ./out --output-format both
  python -m bxf_parser.bxf_parser input.xml --out ./out --flatten-graphics-under-main
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

from .exporters import export_csv, export_xlsx
from .models import NormalizedEvent
from .parsers import parse_file

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("bxf_parser")

# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

_XML_GLOBS = ["*.xml", "*.sch", "*.TV-*", "*.*"]


def discover_files(input_path: Path) -> List[Path]:
    """Return list of candidate XML-like files from a path (file or folder)."""
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        seen: set[Path] = set()
        files: List[Path] = []
        for pattern in _XML_GLOBS:
            for p in sorted(input_path.glob(pattern)):
                if p.is_file() and p not in seen:
                    seen.add(p)
                    files.append(p)
        return files
    logger.error("Input path does not exist: %s", input_path)
    return []


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def write_outputs(
    rows: List[NormalizedEvent],
    out_path: Path,
    output_format: str,
) -> None:
    """Write CSV and/or XLSX to out_path (stem already set by caller)."""
    if output_format in ("csv", "both"):
        export_csv(rows, out_path.with_name(out_path.name + ".csv"))
    if output_format in ("xlsx", "both"):
        export_xlsx(rows, out_path.with_name(out_path.name + ".xlsx"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bxf_parser",
        description="Parse broadcast schedule XML/BXF-like files and export to CSV/XLSX.",
    )
    parser.add_argument(
        "input",
        help="Input file or folder containing schedule files.",
    )
    parser.add_argument(
        "--out",
        default="./bxf_output",
        help="Output directory (default: ./bxf_output).",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "xlsx", "both"],
        default="both",
        help="Output format: csv, xlsx, or both (default: both).",
    )
    parser.add_argument(
        "--only-key-events",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only extract operationally meaningful events (default: true).",
    )
    parser.add_argument(
        "--flatten-graphics-under-main",
        action="store_true",
        default=False,
        help="Inherit parent main event title in graphics rows.",
    )
    parser.add_argument(
        "--include-all-key",
        action="store_true",
        default=False,
        help="Include all Format B child events even if not obviously meaningful.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Enable debug-level logging.",
    )

    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = discover_files(input_path)
    if not files:
        logger.error("No files found at: %s", input_path)
        return 1

    logger.info("Found %d file(s) to process", len(files))

    all_rows: List[NormalizedEvent] = []

    for file_path in files:
        logger.info("Processing: %s", file_path)
        rows = parse_file(
            file_path,
            only_key_events=args.only_key_events,
            flatten_graphics=args.flatten_graphics_under_main,
            include_all_key=args.include_all_key,
        )
        if not rows:
            logger.warning("No events extracted from %s", file_path.name)
            continue

        all_rows.extend(rows)

        # Per-file output
        out_stem = out_dir / file_path.name
        write_outputs(rows, out_stem, args.output_format)
        logger.info("Wrote %d events for %s", len(rows), file_path.name)

    if all_rows:
        logger.info("Done — %d total events written to %s", len(all_rows), out_dir)
    else:
        logger.warning("No events were extracted from any file.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
