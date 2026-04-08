"""
Main blueprint — dashboard, search, upload, ingest-log routes.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.utils import secure_filename

from .auth import login_required
from .db import (
    distinct_channels,
    distinct_event_classes,
    get_ingest_log,
    search_events,
)
from .ingest import ingest_file

logger = logging.getLogger(__name__)

main_bp = Blueprint("main", __name__)

_ALLOWED_EXTENSIONS = {".xml", ".sch"}
_PAGE_SIZE = 50


def _allowed_file(filename: str) -> bool:
    suffix = Path(filename).suffix.lower()
    # Also allow extensionless files or TV-* style names
    return suffix in _ALLOWED_EXTENSIONS or suffix == ""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@main_bp.route("/")
@login_required
def index():
    return redirect(url_for("main.search"))


@main_bp.route("/search")
@login_required
def search():
    db_path = current_app.config["DATABASE_PATH"]

    q = request.args.get("q", "").strip()
    channel = request.args.get("channel", "").strip()
    broadcast_date = request.args.get("broadcast_date", "").strip()
    event_class = request.args.get("event_class", "").strip()
    source_format = request.args.get("source_format", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    offset = (page - 1) * _PAGE_SIZE

    rows, total = search_events(
        db_path,
        q=q,
        channel=channel,
        broadcast_date=broadcast_date,
        event_class=event_class,
        source_format=source_format,
        limit=_PAGE_SIZE,
        offset=offset,
    )

    total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)

    channels = distinct_channels(db_path)
    event_classes = distinct_event_classes(db_path)

    return render_template(
        "search.html",
        rows=rows,
        total=total,
        page=page,
        total_pages=total_pages,
        q=q,
        channel=channel,
        broadcast_date=broadcast_date,
        event_class=event_class,
        source_format=source_format,
        channels=channels,
        event_classes=event_classes,
    )


@main_bp.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename:
            flash("No file selected.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        if not _allowed_file(filename):
            flash("Only .xml and .sch files are accepted.", "error")
            return redirect(request.url)

        upload_dir = Path(current_app.config["UPLOAD_DIR"])
        upload_dir.mkdir(parents=True, exist_ok=True)
        dest = upload_dir / filename

        file.save(str(dest))

        db_path = current_app.config["DATABASE_PATH"]
        count = ingest_file(
            dest,
            db_path,
            ingest_source="upload",
            only_key_events=request.form.get("only_key_events") != "0",
            flatten_graphics=request.form.get("flatten_graphics") == "1",
            include_all_key=request.form.get("include_all_key") == "1",
        )

        flash(f"Ingested {count} events from '{filename}'.", "success")
        return redirect(url_for("main.search"))

    return render_template("upload.html")


@main_bp.route("/ingest-log")
@login_required
def ingest_log():
    db_path = current_app.config["DATABASE_PATH"]
    entries = get_ingest_log(db_path, limit=200)
    return render_template("ingest_log.html", entries=entries)
