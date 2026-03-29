from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from bot.services.excel import (
    EXPORT_ONLY_TRANSFERS,
    EXPORT_WITH_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
)
from bot.services.processing import (
    EXPECTED_24H,
    EXPECTED_NO_MOVE,
    EXPECTED_WAREHOUSE_DELAY_MULTIPLE,
    EXPECTED_WAREHOUSE_DELAY_SINGLE,
    SourceFileInfo,
    WorkflowError,
)
from web.auth import is_authenticated
from web.dependencies import TEMPLATES, get_processing_service, template_context
from web.security import validate_csrf_token

router = APIRouter()
VALID_NO_MOVE_EXPORT_MODES = {
    EXPORT_WITH_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
    EXPORT_ONLY_TRANSFERS,
}


def _redirect_if_guest(request: Request) -> RedirectResponse | None:
    if is_authenticated(request):
        return None
    return RedirectResponse(url="/login", status_code=303)


def _render_result(
    request: Request,
    *,
    level: str,
    title: str,
    message: str,
    sheet_url: str | None = None,
) -> HTMLResponse:
    return TEMPLATES.TemplateResponse(
        request,
        "partials/result_panel.html",
        template_context(
            request,
            result={
                "level": level,
                "title": title,
                "message": message,
                "sheet_url": sheet_url,
            },
        ),
    )


def _resolve_expected(workflow: str) -> str:
    mapping = {
        "no_move": EXPECTED_NO_MOVE,
        "h24": EXPECTED_24H,
        "warehouse_delay_single": EXPECTED_WAREHOUSE_DELAY_SINGLE,
        "warehouse_delay_multiple": EXPECTED_WAREHOUSE_DELAY_MULTIPLE,
    }
    try:
        return mapping[workflow]
    except KeyError as exc:
        raise WorkflowError("Неизвестный web-сценарий.") from exc


async def _save_upload_to_temp(
    upload: UploadFile,
    *,
    temp_path: Path,
    max_bytes: int,
) -> int:
    written = 0
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    with temp_path.open("wb") as buffer:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                raise WorkflowError("Файл превышает допустимый размер для web-загрузки.")
            buffer.write(chunk)
    await upload.close()
    return written


@router.post("/app/actions/process", response_class=HTMLResponse)
async def process_action(
    request: Request,
    workflow: str = Form(...),
    source_kind: str = Form(...),
    csrf_token: str = Form(...),
    export_mode: str | None = Form(None),
    url: str | None = Form(None),
    upload: UploadFile | None = File(None),
) -> HTMLResponse:
    redirect = _redirect_if_guest(request)
    if redirect:
        return redirect

    expected = _resolve_expected(workflow)
    service = get_processing_service(request)
    config = request.app.state.runtime.config
    validate_csrf_token(request, csrf_token)

    if service.lock.locked():
        return _render_result(
            request,
            level="warning",
            title="Очередь занята",
            message="Сейчас выполняется другая обработка. Повторите чуть позже.",
        )

    temp_path: Path | None = None
    try:
        async with service.processing_slot():
            if expected == EXPECTED_NO_MOVE and export_mode not in VALID_NO_MOVE_EXPORT_MODES:
                raise WorkflowError("Выберите корректный тип выгрузки для режима «Без движения».")
            if source_kind == "file":
                if upload is None or not upload.filename:
                    raise WorkflowError("Выберите файл для загрузки.")
                temp_path = service.make_temp_path(
                    "web_upload",
                    Path(upload.filename).suffix or ".xlsx",
                )
                file_size = await _save_upload_to_temp(
                    upload,
                    temp_path=temp_path,
                    max_bytes=config.web_max_upload_mb * 1024 * 1024,
                )
                outcome = await service.process_local_source(
                    expected,
                    temp_path,
                    SourceFileInfo(
                        filename=upload.filename,
                        size=file_size,
                        source="web_upload",
                        source_path=str(temp_path),
                    ),
                    no_move_export_mode=export_mode,
                )
            elif source_kind == "url":
                if not (url or "").strip():
                    raise WorkflowError("Вставьте ссылку на файл.")
                outcome = await service.process_url_source(
                    expected,
                    (url or "").strip(),
                    no_move_export_mode=export_mode,
                )
            elif source_kind == "yadisk_latest":
                outcome = await service.process_latest_yadisk_file(
                    expected,
                    no_move_export_mode=export_mode,
                )
            elif source_kind == "yadisk_folder":
                if expected != EXPECTED_WAREHOUSE_DELAY_MULTIPLE:
                    raise WorkflowError("Папочный запуск доступен только для задержки склада.")
                outcome = await service.process_warehouse_delay_multiple()
            else:
                raise WorkflowError("Неизвестный источник данных.")
    except WorkflowError as exc:
        return _render_result(
            request,
            level="error",
            title="Ошибка обработки",
            message=str(exc),
        )
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    return _render_result(
        request,
        level=outcome.level,
        title=outcome.title,
        message=outcome.message,
        sheet_url=outcome.sheet_url,
    )
