from __future__ import annotations

import secrets

from fastapi import Request

from bot.config import Config

SESSION_USER_KEY = "web_user"


def current_user(request: Request) -> str | None:
    value = request.session.get(SESSION_USER_KEY)
    return str(value) if value else None


def is_authenticated(request: Request) -> bool:
    return current_user(request) is not None


def authenticate(config: Config, username: str, password: str) -> bool:
    expected_username = config.web_admin_username or ""
    expected_password = config.web_admin_password or ""
    return secrets.compare_digest(username, expected_username) and secrets.compare_digest(
        password,
        expected_password,
    )


def login_user(request: Request, username: str) -> None:
    request.session[SESSION_USER_KEY] = username


def logout_user(request: Request) -> None:
    request.session.clear()
