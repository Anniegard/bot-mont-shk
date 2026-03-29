from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from bot.services.excel import (
    EXPORT_ONLY_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
    EXPORT_WITH_TRANSFERS,
)
from web.auth import authenticate, current_user, is_authenticated, login_user, logout_user
from web.dependencies import TEMPLATES, get_config, template_context
from web.security import validate_csrf_token

router = APIRouter()


def _redirect_if_guest(request: Request) -> RedirectResponse | None:
    if is_authenticated(request):
        return None
    return RedirectResponse(url="/login", status_code=303)


def _read_log_tail(log_path: Path, max_lines: int = 200) -> str:
    if not log_path.exists():
        return "Логи пока не созданы."
    lines = log_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return "(логи пусты)"
    return "\n".join(lines[-max_lines:])


@router.get("/", response_class=HTMLResponse)
async def landing_page(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse(
        request,
        "landing.html",
        template_context(request),
    )


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse(url="/app", status_code=303)
    return TEMPLATES.TemplateResponse(
        request,
        "login.html",
        template_context(request),
    )


@router.post("/login", response_class=HTMLResponse)
async def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(...),
) -> HTMLResponse:
    validate_csrf_token(request, csrf_token)
    config = get_config(request)
    if authenticate(config, username=username, password=password):
        login_user(request, username)
        return RedirectResponse(url="/app", status_code=303)
    return TEMPLATES.TemplateResponse(
        request,
        "login.html",
        template_context(
            request,
            auth_error="Неверный логин или пароль.",
        ),
        status_code=401,
    )


@router.post("/logout")
async def logout_action(
    request: Request,
    csrf_token: str = Form(...),
) -> RedirectResponse:
    validate_csrf_token(request, csrf_token)
    logout_user(request)
    return RedirectResponse(url="/", status_code=303)


@router.get("/app", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    redirect = _redirect_if_guest(request)
    if redirect:
        return redirect
    return TEMPLATES.TemplateResponse(
        request,
        "dashboard.html",
        template_context(
            request,
            no_move_modes=[
                ("С передачами", EXPORT_WITH_TRANSFERS),
                ("Без передач", EXPORT_WITHOUT_TRANSFERS),
                ("Только передачи", EXPORT_ONLY_TRANSFERS),
            ],
        ),
    )


@router.get("/app/help", response_class=HTMLResponse)
async def help_page(request: Request) -> HTMLResponse:
    redirect = _redirect_if_guest(request)
    if redirect:
        return redirect
    config = get_config(request)
    return TEMPLATES.TemplateResponse(
        request,
        "help.html",
        template_context(
            request,
            yandex_dirs={
                "no_move": config.yandex_no_move_dir,
                "h24": config.yandex_24h_dir,
                "warehouse_delay": config.yandex_warehouse_delay_dir,
            },
        ),
    )


@router.get("/app/admin", response_class=HTMLResponse)
async def admin_page(request: Request) -> HTMLResponse:
    redirect = _redirect_if_guest(request)
    if redirect:
        return redirect
    log_path = Path(__file__).resolve().parents[2] / "logs" / "bot.log"
    return TEMPLATES.TemplateResponse(
        request,
        "admin.html",
        template_context(
            request,
            log_tail=_read_log_tail(log_path),
            username=current_user(request),
        ),
    )
