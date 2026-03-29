from __future__ import annotations

from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates

from bot.config import Config
from bot.runtime import AppRuntime
from bot.services.processing import ProcessingService
from web.auth import current_user
from web.security import ensure_csrf_token

TEMPLATES = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent / "templates")
)


def get_runtime(request: Request) -> AppRuntime:
    return request.app.state.runtime


def get_config(request: Request) -> Config:
    return get_runtime(request).config


def get_processing_service(request: Request) -> ProcessingService:
    return request.app.state.processing_service


def template_context(request: Request, **extra: object) -> dict[str, object]:
    config = get_config(request)
    context: dict[str, object] = {
        "request": request,
        "config": config,
        "current_user": current_user(request),
        "site_name": "AnniLand",
        "public_url": config.public_base_url or "https://AnniLand.ru",
        "csrf_token": ensure_csrf_token(request),
    }
    context.update(extra)
    return context
