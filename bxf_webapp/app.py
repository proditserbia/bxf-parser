"""
Flask application factory.
"""
from __future__ import annotations

import logging

from flask import Flask
from werkzeug.security import generate_password_hash

from .auth import auth_bp
from .config import Config
from .db import create_user, init_db
from .ingest import IngestWatcher
from .routes import main_bp

logger = logging.getLogger(__name__)


def create_app(config: object = None) -> Flask:
    """Create and configure the Flask app."""
    app = Flask(__name__, instance_relative_config=False)

    # Load default config
    app.config.from_object(Config)

    # Apply overrides (useful in tests)
    if config is not None:
        app.config.from_object(config)

    # Enforce a real secret key in production
    if not app.config.get("TESTING") and app.config["SECRET_KEY"].startswith("dev"):
        logger.warning(
            "SECRET_KEY appears to be a development value. "
            "Set the SECRET_KEY environment variable in production."
        )

    # Initialise database
    db_path = app.config["DATABASE_PATH"]
    init_db(db_path)

    # Bootstrap admin user (only if it does not exist yet)
    _bootstrap_admin(app)

    # Register blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    # Start directory watcher if configured
    watch_dir = app.config.get("WATCH_DIR", "")
    if watch_dir:
        watcher = IngestWatcher(
            watch_dir=watch_dir,
            db_path=db_path,
            interval=app.config.get("WATCH_INTERVAL", 30),
        )
        watcher.start()
        app.extensions["ingest_watcher"] = watcher
        logger.info("IngestWatcher started for '%s'", watch_dir)

    return app


def _bootstrap_admin(app: Flask) -> None:
    """Create the admin user on first startup if it does not already exist."""
    db_path = app.config["DATABASE_PATH"]
    username = app.config["ADMIN_USERNAME"]
    password = app.config["ADMIN_PASSWORD"]

    from .db import get_user  # local import to avoid circularity at module level
    if get_user(db_path, username) is None:
        create_user(db_path, username, generate_password_hash(password))
        logger.info("Admin user '%s' created.", username)
