"""
Configuration for bxf_webapp.

All values can be overridden via environment variables.
"""
from __future__ import annotations

import os
import secrets


class Config:
    # Flask
    SECRET_KEY: str = os.environ.get("SECRET_KEY", secrets.token_hex(32))
    DEBUG: bool = os.environ.get("DEBUG", "false").lower() == "true"

    # SQLite
    DATABASE_PATH: str = os.environ.get("DATABASE_PATH", "bxf_web.db")

    # File uploads
    UPLOAD_DIR: str = os.environ.get("UPLOAD_DIR", "/tmp/bxf_uploads")
    MAX_UPLOAD_BYTES: int = int(os.environ.get("MAX_UPLOAD_BYTES", str(64 * 1024 * 1024)))  # 64 MB

    # Auto-ingest watcher (disabled when WATCH_DIR is empty)
    WATCH_DIR: str = os.environ.get("WATCH_DIR", "")
    WATCH_INTERVAL: int = int(os.environ.get("WATCH_INTERVAL", "30"))

    # Bootstrap admin account (used only on first startup)
    ADMIN_USERNAME: str = os.environ.get("ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD: str = os.environ.get("ADMIN_PASSWORD", "changeme")
