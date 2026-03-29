from __future__ import annotations

import secrets
import time
from collections import defaultdict, deque
from pathlib import Path

from fastapi import HTTPException, Request, UploadFile, status

CSRF_SESSION_KEY = "csrf_token"


class InMemoryRateLimiter:
    def __init__(self, limit: int, window_seconds: int = 60) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> None:
        now = time.time()
        window_start = now - self.window_seconds
        hits = self._hits[key]
        while hits and hits[0] < window_start:
            hits.popleft()
        if len(hits) >= self.limit:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Слишком много запросов. Попробуйте чуть позже.",
            )
        hits.append(now)


def ensure_csrf_token(request: Request) -> str:
    token = request.session.get(CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(24)
        request.session[CSRF_SESSION_KEY] = token
    return token


def validate_csrf_token(request: Request, token: str | None) -> None:
    expected = request.session.get(CSRF_SESSION_KEY)
    if not token or not expected or not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Проверка формы не пройдена. Обновите страницу и попробуйте снова.",
        )


def client_ip(request: Request) -> str:
    real_ip = request.headers.get("x-real-ip", "").strip()
    if real_ip:
        return real_ip
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def enforce_rate_limit(request: Request, limiter: InMemoryRateLimiter) -> None:
    limiter.check(client_ip(request))


def verify_admin_credentials(
    username: str | None,
    password: str | None,
    *,
    expected_username: str | None,
    expected_password: str | None,
) -> bool:
    if not expected_username or not expected_password:
        return False
    return secrets.compare_digest(username or "", expected_username) and secrets.compare_digest(
        password or "",
        expected_password,
    )


async def save_upload_file(
    upload: UploadFile,
    target_path: Path,
    *,
    max_bytes: int,
) -> int:
    total_size = 0
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("wb") as output:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total_size += len(chunk)
            if total_size > max_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="Файл слишком большой для обработки через сайт.",
                )
            output.write(chunk)
    await upload.close()
    return total_size
