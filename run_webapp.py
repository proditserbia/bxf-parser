"""
Run the BXF AsRun web application.

Usage:
    python run_webapp.py

Environment variables:
    SECRET_KEY        Flask secret key (required in production)
    DATABASE_PATH     Path to SQLite database file (default: bxf_web.db)
    UPLOAD_DIR        Directory for uploaded files (default: /tmp/bxf_uploads)
    WATCH_DIR         Directory to watch for auto-ingest (default: disabled)
    WATCH_INTERVAL    Polling interval in seconds (default: 30)
    ADMIN_USERNAME    Bootstrap admin username (default: admin)
    ADMIN_PASSWORD    Bootstrap admin password (default: changeme)
    DEBUG             Set to 'true' to enable Flask debug mode
"""
import sys
from pathlib import Path

# Ensure bxf_parser is importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent))

from bxf_webapp import create_app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=app.config.get("DEBUG", False))
