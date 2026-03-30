from __future__ import annotations

import secrets

from fastapi import Request

from bot.config import Config

SESSION_USER_KEY = "web_user"


def _credentials_match(
    username: str,
    password: str,
    expected_username: str | None,
    expected_password: str | None,
) -> bool:
    if not expected_username or not expected_password:
        return False
    return secrets.compare_digest(username, expected_username) and secrets.compare_digest(
        password,
        expected_password,
    )


def current_user(request: Request) -> str | None:
    value = request.session.get(SESSION_USER_KEY)
    return str(value) if value else None


def is_authenticated(request: Request) -> bool:
    return current_user(request) is not None


def authenticate(config: Config, username: str, password: str) -> bool:
    if _credentials_match(username, password, config.web_admin_username, config.web_admin_password):
        return True
    return _credentials_match(username, password, config.web_user_username, config.web_user_password)


def is_web_admin(request: Request, config: Config) -> bool:
    user = current_user(request)
    expected = config.web_admin_username or ""
    if not user or not expected:
        return False
    return secrets.compare_digest(user, expected)


def login_user(request: Request, username: str) -> None:
    request.session[SESSION_USER_KEY] = username


def logout_user(request: Request) -> None:
    request.session.clear()
