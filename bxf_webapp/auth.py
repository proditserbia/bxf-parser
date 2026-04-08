"""
Authentication blueprint — login / logout + @login_required decorator.
"""
from __future__ import annotations

import functools
import logging
from typing import Callable

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
from urllib.parse import urlsplit
from werkzeug.security import check_password_hash

from .db import get_user

logger = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def login_required(view: Callable) -> Callable:
    """Redirect to login if the user is not authenticated."""
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("username"):
            return redirect(url_for("auth.login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if session.get("username"):
        return redirect(url_for("main.index"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db_path = current_app.config["DATABASE_PATH"]

        user = get_user(db_path, username)
        if user is None or not check_password_hash(user["password_hash"], password):
            error = "Invalid username or password."
            logger.warning("Failed login attempt for user '%s'", username)
        else:
            session.clear()
            session["username"] = user["username"]
            logger.info("User '%s' logged in", username)
            next_page = request.args.get("next", "")
            # Only allow relative paths to prevent open-redirect attacks
            parsed = urlsplit(next_page)
            if not next_page or parsed.scheme or parsed.netloc:
                next_page = url_for("main.index")
            return redirect(next_page)

    return render_template("login.html", error=error)


@auth_bp.route("/logout")
def logout():
    username = session.get("username")
    session.clear()
    if username:
        logger.info("User '%s' logged out", username)
    return redirect(url_for("auth.login"))
