from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from bot.runtime import AppRuntime, build_runtime
from bot.services.processing import ProcessingService
from web.rate_limit import RateLimitMiddleware
from web.routes.actions import router as actions_router
from web.routes.pages import router as pages_router


def create_app(
    *,
    runtime: AppRuntime | None = None,
    processing_service: ProcessingService | None = None,
) -> FastAPI:
    root_dir = Path(__file__).resolve().parents[1]
    resolved_runtime = runtime or build_runtime(
        require_telegram_token=False,
        require_web_auth=True,
    )
    app = FastAPI(
        title="AnniLand",
        description="Web-дубль Telegram-бота Bot_Mont_SHK",
    )
    if not resolved_runtime.config.web_secret_key:
        raise ValueError("WEB_SECRET_KEY is required for the web application.")
    app.state.runtime = resolved_runtime
    app.state.processing_service = processing_service or ProcessingService(
        config=resolved_runtime.config,
        gspread_client=resolved_runtime.gspread_client,
        workdir=root_dir,
        block_ids_path=root_dir / "data" / "block_ids.txt",
        snapshot_path=root_dir / "data" / "last_24h_snapshot.json",
        snapshot_meta_path=root_dir / "data" / "last_24h_meta.json",
        no_move_map_path=root_dir / "data",
    )

    app.add_middleware(
        SessionMiddleware,
        secret_key=resolved_runtime.config.web_secret_key,
        same_site="lax",
        https_only=bool(
            resolved_runtime.config.public_base_url
            and resolved_runtime.config.public_base_url.startswith("https://")
        ),
    )
    app.add_middleware(
        RateLimitMiddleware,
        requests_per_minute=resolved_runtime.config.web_rate_limit_per_minute,
    )

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(pages_router)
    app.include_router(actions_router)
    return app
