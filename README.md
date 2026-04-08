# bxf-parser

A Python 3 tool that parses two broadcast schedule XML / BXF-like file formats
and extracts operationally meaningful playout events.  Available as both a
**command-line tool** (CSV/XLSX export) and a **production-ready web application**
(SQLite storage, authentication, automatic ingest, and full-text search).

---

## Features

- Auto-detects the input format (Format A or Format B)
- Extracts only key playout / on-air / graphics events — not every XML row
- Classifies events: PROGRAMME, GRAPHICS, LIVE_INPUT, PLAYOUT, PROGRAMME_CONTAINER, OTHER
- Exports per-file outputs to CSV and / or XLSX (CLI)
- **Web app**: login-protected search UI, file upload, automatic directory ingest, ingest audit log
- Handles UTF-8 and Cyrillic text
- Robust to missing tags, empty values, and large files
- Full CLI with helpful flags

---

## Supported formats

### Format A — `ASTRA_DIFACE_LITE_v1_2`

Root tag: `<ASTRA_DIFACE_LITE_v1_2>`

Channel metadata lives in `<Plan>`.
Each `<Event>` inside `<Events>` has flat child elements such as
`<DevType>`, `<Name>`, `<MatPath>`, `<RealStart>`, `<RealDuration>` etc.

### Format B — Schedule XML

Root tag: `<Schedule>`

Channel attribute on `<Events Channel="...">`.
Each `<Event>` has XML attributes (`Uid`, `Type`, `FullyQualifiedType`,
`NotionalStartTime`, …) plus `<EventKind>` and `<Fields>` containing
`<Parameter Name="..." Value="..."/>` elements.

---

## Setup and install

```bash
# 1. Clone / download the repository
git clone https://github.com/proditserbia/bxf-parser.git
cd bxf-parser

# 2. (Optional) create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install runtime dependencies
pip install -r requirements.txt
```

`requirements.txt` contains `openpyxl` (XLSX export) and `flask` (web app).
Everything else uses the Python standard library.

---

## CLI usage

```bash
# Parse a single file, write CSV + XLSX to ./out
python -m bxf_parser.bxf_parser input.xml --out ./out

# Parse a whole folder, produce both formats
python -m bxf_parser.bxf_parser ./schedules --out ./out --output-format both

# Flatten graphics titles under the nearest parent programme
python -m bxf_parser.bxf_parser ./schedules --out ./out --flatten-graphics-under-main

# Include all child events (not just key ones)
python -m bxf_parser.bxf_parser ./schedules --out ./out --no-only-key-events

# Include even low-value technical children in Format B
python -m bxf_parser.bxf_parser ./schedules --out ./out --include-all-key

# Verbose / debug logging
python -m bxf_parser.bxf_parser input.xml --out ./out --verbose
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `input` | _(required)_ | File or folder to parse |
| `--out DIR` | `./bxf_output` | Output directory |
| `--output-format csv\|xlsx\|both` | `both` | Output format |
| `--only-key-events` / `--no-only-key-events` | `true` | Extract only meaningful events |
| `--flatten-graphics-under-main` | off | Prepend nearest parent programme title to graphics rows |
| `--include-all-key` | off | Include all Format B Normal children |
| `--verbose` / `-v` | off | Debug-level logging |

### Output files

For each input file `schedule.xml` the tool writes:

- `{out}/schedule.csv`
- `{out}/schedule.xlsx`

---

## Normalised output schema

| Column | Description |
|---|---|
| `source_file` | Input filename |
| `source_format` | `astra_diface_lite` or `schedule_xml` |
| `channel` | Plan/Channel or Events/@Channel |
| `broadcast_date` | Plan/Date or date derived from start time |
| `event_id` | AstraId → JobId → UUID (A) / Uid (B) |
| `parent_event_id` | blank (A) / OwnerUid (B) |
| `event_class` | PROGRAMME / GRAPHICS / LIVE_INPUT / PLAYOUT / OTHER / PROGRAMME_CONTAINER |
| `event_type` | Human-readable subtype |
| `event_kind` | Original EventKind (B only) |
| `title` | Name (A) / Parameter(EventName) or Type (B) |
| `secondary_title` | Name2 (A) / FullyQualifiedType (B) |
| `material_id` | MatPath or SrcMatPath (A) / MaterialId or MediaID (B) |
| `job_id` | JobId (A) |
| `media_path` | MatPath (A) / MaterialId or MediaID (B) |
| `device` | Device (A) / Parameter(Device) (B) |
| `playout_device` | blank (A) / Parameter(PlayoutDevice) (B) |
| `start_time` | RealStart (A) / AsRunStartTime → NotionalStartTime (B) |
| `end_time` | Derived from start + duration; blank if unparseable |
| `duration` | RealDuration (A) / AsRunDuration → Duration → NotionalDuration (B) |
| `status` | ErrStatus / ClipStatus (A) / AsRunKernelStatusCode (B) |
| `onair_state` | OnAirState (A) / AsRunStartedOnAir + AsRunFinishedOnAir (B) |
| `transition` | Transition (A) / TransitionType:TransitionDuration (B) |
| `is_graphics` | Boolean |
| `is_live` | Boolean |
| `is_main` | Boolean |
| `crit1`–`crit4` | Criteria fields (A only) |
| `note` | Note (A) / ScheduleName (B) |
| `raw_type` | EventType (A) / Type attribute (B) |
| `raw_devtype` | DevType (A) |
| `raw_par_type` | ParType (A) / StartMode (B) |
| `raw_xml_summary` | Compact JSON of original attributes + key children |

---

## Classification heuristics

### Format A

| DevType | event_class |
|---|---|
| `PRO` | `PROGRAMME` |
| `CG` | `GRAPHICS` |
| `IN` | `LIVE_INPUT` |
| anything else | Heuristic keyword scan of Name, Device, MatPath, ParType |

Keyword sets used:

- **GRAPHICS**: `cg`, `logo`, `bug`, `dsk`, `txt`, `inserter`, `lower`, `third`, `graphic`, `branding`, `ident`, `squeeze`, `clock`, `text`, `caption`, `ticker`, `crawl`, `overlay`
- **LIVE_INPUT**: `in`, `input`, `router`, `live`, `source`, `feed`, `ingest`, `encoder`, `decoder`, `sdi`, `ip`, `cam`, `camera`
- **PROGRAMME**: `pro`, `playout`, `clip`, `file`, `media`, `vtr`, `server`, `avid`, `isis`, `k2`, `viz`, `vcs`

### Format B

| EventKind | event_class |
|---|---|
| `MainEvent` | `PROGRAMME_CONTAINER` |
| `MaterialEvent` | `PROGRAMME` |
| `Normal` with graphics keyword | `GRAPHICS` |
| `Normal` with live keyword | `LIVE_INPUT` |
| `Normal` (no match, `--include-all-key`) | `OTHER` |
| `Normal` (no match, default) | _skipped_ |

Keyword sets used:

- **GRAPHICS**: `bug`, `dsk`, `txt`, `graphic`, `inserter`, `logo`, `lower`, `third`, `text`, `cg`, `overlay`, `ident`, `caption`, `ticker`, `crawl`, `clock`, `branding`
- **LIVE_INPUT**: `live`, `input`, `router`, `source`, `feed`, `ingest`, `encoder`, `decoder`, `sdi`, `ip`, `cam`, `camera`

---

## Web application

```bash
# Start the web server (default: http://0.0.0.0:5000)
python run_webapp.py
```

Log in with the bootstrap credentials (`admin` / `changeme` by default) and change
the password via environment variables in production.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `SECRET_KEY` | random | Flask session signing key — **must be set in production** |
| `DATABASE_PATH` | `bxf_web.db` | SQLite database file path |
| `UPLOAD_DIR` | `/tmp/bxf_uploads` | Staging directory for uploaded files |
| `WATCH_DIR` | _(disabled)_ | Directory to watch for auto-ingest |
| `WATCH_INTERVAL` | `30` | Auto-ingest poll interval in seconds |
| `ADMIN_USERNAME` | `admin` | Bootstrap admin username |
| `ADMIN_PASSWORD` | `changeme` | Bootstrap admin password — **change in production** |
| `DEBUG` | `false` | Enable Flask debug mode |

### Web app features

- **Search** — full-text search on title/material/note with filters for channel, date, event class, and format
- **Upload** — upload a single XML file for immediate ingest with configurable parse options
- **Auto-ingest** — set `WATCH_DIR` to a network share or drop folder; any new XML file is parsed automatically every `WATCH_INTERVAL` seconds
- **Ingest log** — audit trail of every ingest operation with status and event count
- **Authentication** — all routes require login; session-based with `werkzeug` password hashing

---

## Project structure

```
bxf_parser/
    __init__.py       Package marker
    models.py         NormalizedEvent dataclass + COLUMNS list
    utils.py          Format detection, timecode parsing, XML helpers
    parsers.py        parse_format_a, parse_format_b, classify_* functions
    exporters.py      export_csv, export_xlsx
    bxf_parser.py     CLI entry point
    gui.py            Tkinter desktop GUI

bxf_webapp/
    __init__.py       App factory export
    app.py            Flask app factory + admin bootstrap
    config.py         Config class (env-var driven)
    db.py             SQLite schema, connection helpers, CRUD
    auth.py           Login/logout blueprint + @login_required decorator
    ingest.py         ingest_file() + IngestWatcher background thread
    routes.py         Main blueprint (search, upload, ingest-log)
    templates/        Jinja2 HTML templates
    static/           CSS

run_webapp.py         Web server entry point

tests/
    test_bxf_parser.py   CLI/parser pytest suite (66 tests)
    test_webapp.py        Web app pytest suite (37 tests)
    data/
        sample_format_a.xml
        sample_format_b.xml

requirements.txt
README.md
```

---

## Running tests

```bash
pip install pytest openpyxl flask
pytest tests/ -v
```

---

## Known limitations

1. **No full BXF schema support** — this is an operational extraction tool, not an archival parser.
2. **Timecode arithmetic** — end-time derivation requires both start and duration to be parseable. If either is in an unrecognised format the `end_time` column is left blank (no crash).
3. **Large files** — the parser loads the whole file into memory with `xml.etree.ElementTree`. For very large files (> 500 MB) consider streaming with `iterparse`.
4. **Format auto-detection** — based on root tag. Files with non-standard root tags fall back to heuristic child-element detection; unrecognised files are attempted with both parsers and logged as warnings.
5. **Namespace-prefixed XML** — if files use XML namespaces (e.g. `<ns:Schedule>`) you may need to strip or register namespaces. See `utils.py` for where to add that logic.

---

## Extending heuristics for additional variants

To support a new BXF / broadcast XML variant:

1. **Add a detection rule** in `utils.detect_format()` — match on root tag or a characteristic child element.
2. **Add a new parser function** `parse_format_c(path, ...) -> List[NormalizedEvent]` in `parsers.py` following the same pattern as `parse_format_a` / `parse_format_b`.
3. **Add a classify function** `classify_event_format_c(...)` returning `(event_class, event_type, is_graphics, is_live, is_main)`.
4. **Wire it up** in `parsers.parse_file()`.
5. **Add test data** in `tests/data/` and tests in `tests/test_bxf_parser.py`.

No changes to the exporter or CLI are needed — they work with the normalised `NormalizedEvent` objects regardless of source format.
