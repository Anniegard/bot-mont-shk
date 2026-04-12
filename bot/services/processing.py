from __future__ import annotations

import asyncio
import logging
import os
import time
import zipfile
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from bot.config import Config
from bot.services.block_ids import load_block_ids
from bot.services.excel import process_file
from bot.services.excel_24h import build_24h_table, load_snapshot, process_24h_file, save_snapshot
from bot.services.file_sources import maybe_extract_zip
from bot.services.no_move_map import load_no_move_map, save_no_move_map
from bot.services.sheets import update_tables, update_warehouse_delay_sheet
from bot.services.warehouse_delay import (
    WarehouseDelayAggregationResult,
    WarehouseDelayError,
    build_warehouse_delay_sheet_matrix,
    process_warehouse_delay_consolidated_file,
    aggregate_warehouse_delay_files,
)
from bot.services.yadisk import yadisk_download_file, yadisk_list_files, yadisk_list_latest

logger = logging.getLogger(__name__)

EXPECTED_NO_MOVE = "no_move"
EXPECTED_24H = "24h"
EXPECTED_WAREHOUSE_DELAY_SINGLE = "warehouse_delay_single"
EXPECTED_WAREHOUSE_DELAY_MULTIPLE = "warehouse_delay_multiple"
WAREHOUSE_DELAY_TZ = ZoneInfo("Europe/Moscow")
GLOBAL_LOCK_STALE_SECONDS = 6 * 60 * 60

# Optional progress for web UI: (step_title, detail_message, duration_ms_for_previous_phase_or_none).
ProgressCallback = Callable[[str, str, int | None], None]


class WorkflowError(Exception):
    pass


class ProcessingBusyError(WorkflowError):
    pass


@dataclass(frozen=True)
class SourceFileInfo:
    filename: str
    size: int | None = None
    source: str = "local"
    source_path: str | None = None


@dataclass(frozen=True)
class WorkflowOutcome:
    title: str
    message: str
    source_name: str | None = None
    sheet_url: str | None = None
    level: str = "success"
    parse_mode: str | None = "HTML"
    disable_web_page_preview: bool = True
    payload: dict[str, Any] = field(default_factory=dict)


class ProcessingService:
    def __init__(
        self,
        config: Config,
        gspread_client: Any,
        *,
        workdir: Path,
        block_ids_path: Path,
        snapshot_path: Path,
        snapshot_meta_path: Path,
        no_move_map_path: Path,
    ) -> None:
        self.config = config
        self.gc = gspread_client
        self.workdir = workdir
        self.block_ids_path = block_ids_path
        self.snapshot_path = snapshot_path
        self.snapshot_meta_path = snapshot_meta_path
        self.no_move_map_path = no_move_map_path
        self.lock = asyncio.Lock()
        self.global_lock_path = self.workdir / "data" / "processing.lock"

    @staticmethod
    def _notify_progress(
        progress_cb: ProgressCallback | None,
        step: str,
        message: str,
        duration_ms: int | None = None,
    ) -> None:
        if progress_cb is not None:
            progress_cb(step, message, duration_ms)

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return int((time.perf_counter() - start) * 1000)

    def make_temp_path(self, prefix: str, suffix: str) -> Path:
        temp_dir = self.workdir / "data" / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
        return temp_dir / f"{prefix}_{int(time.time() * 1000)}_{uuid4().hex[:8]}{normalized_suffix}"

    def build_yadisk_help_text(self) -> str:
        return (
            "Как работать с Яндекс.Диском:\n"
            "1) Загрузите файл в нужную папку.\n"
            "2) Для одиночного запуска нажмите «Взять с Я.Диска (последний файл)».\n"
            "3) Для пакетной задержки склада используйте режим «Из нескольких файлов».\n\n"
            f"Папка «Без движения»: {self.config.yandex_no_move_dir}\n"
            f"Папка «24 часа»: {self.config.yandex_24h_dir}\n"
            f"Папка «Задержка склада»: {self.config.yandex_warehouse_delay_dir}"
        )

    @asynccontextmanager
    async def processing_slot(self):
        async with self.lock:
            release = await asyncio.to_thread(self._acquire_global_lock)
            try:
                yield
            finally:
                await asyncio.to_thread(release)

    async def process_local_source(
        self,
        expected: str,
        file_path: str | Path,
        file_info: SourceFileInfo,
        *,
        no_move_export_mode: str | None = None,
    ) -> WorkflowOutcome:
        original_path = Path(file_path)
        prepared_path = await asyncio.to_thread(
            maybe_extract_zip, str(file_path), str(Path(file_path).parent)
        )
        prepared_path_obj = Path(prepared_path)
        try:
            return await self._process_prepared_source(
                expected,
                prepared_path_obj,
                file_info,
                no_move_export_mode=no_move_export_mode,
                progress_cb=None,
            )
        finally:
            if prepared_path_obj != original_path:
                self._cleanup_temp_artifacts(prepared_path_obj)

    async def process_latest_yadisk_file(
        self,
        expected: str,
        *,
        no_move_export_mode: str | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> WorkflowOutcome:
        token = self._require_yadisk_token()
        folder = self._folder_for_expected(expected)
        t_phase = time.perf_counter()
        logger.info(
            "yadisk flow: resolved folder expected=%s folder=%s",
            expected,
            folder,
        )
        self._notify_progress(
            progress_cb,
            "Папка на Я.Диске",
            f"Режим {expected}, каталог: {folder}",
            self._elapsed_ms(t_phase),
        )

        t_phase = time.perf_counter()
        self._notify_progress(
            progress_cb,
            "Список файлов",
            "Запрашиваем последний подходящий файл в папке…",
            None,
        )
        latest = await yadisk_list_latest(token, folder, self.config.yandex_allowed_exts)
        yname = latest.get("name") or "?"
        ypath = latest.get("path") or "?"
        logger.info(
            "yadisk flow: list_latest done expected=%s file=%s path=%s ms=%s",
            expected,
            yname,
            ypath,
            self._elapsed_ms(t_phase),
        )
        self._notify_progress(
            progress_cb,
            "Найден последний файл",
            f"«{yname}» ({ypath})",
            self._elapsed_ms(t_phase),
        )

        suffix = self._suffix_from_name(latest.get("name") or "source.xlsx")
        temp_path = self.make_temp_path("yadisk_latest", suffix)
        extracted_path: Path | None = None
        try:
            t_phase = time.perf_counter()
            self._notify_progress(
                progress_cb,
                "Скачивание",
                f"Начато скачивание во временный файл: {temp_path}",
                None,
            )
            logger.info(
                "yadisk flow: download start expected=%s local=%s yadisk_path=%s",
                expected,
                temp_path,
                ypath,
            )
            download_info = await yadisk_download_file(
                token,
                latest["path"],
                str(temp_path),
                max_bytes=self.config.yandex_max_mb * 1024 * 1024,
            )
            sz = download_info.get("size")
            logger.info(
                "yadisk flow: download done expected=%s bytes=%s ms=%s",
                expected,
                sz,
                self._elapsed_ms(t_phase),
            )
            self._notify_progress(
                progress_cb,
                "Скачивание завершено",
                f"Сохранено {sz} байт → {temp_path}",
                self._elapsed_ms(t_phase),
            )
            file_info = SourceFileInfo(
                filename=latest["name"],
                size=download_info.get("size"),
                source="yandex_disk_oauth",
                source_path=latest["path"],
            )
            t_phase = time.perf_counter()
            self._notify_progress(
                progress_cb,
                "Распаковка",
                "При необходимости распаковываем ZIP…",
                None,
            )
            try:
                prepared_path = await asyncio.to_thread(
                    maybe_extract_zip,
                    download_info["path"],
                    str(Path(download_info["path"]).parent),
                )
            except (ValueError, zipfile.BadZipFile, OSError) as exc:
                logger.exception(
                    "yadisk flow: zip/excel extract failed expected=%s file=%s",
                    expected,
                    yname,
                )
                raise WorkflowError(
                    "Не удалось открыть архив или в нём нет файла Excel (.xlsx/.xls). "
                    "Проверьте, что загружен корректный ZIP."
                ) from exc
            extracted_path = Path(prepared_path)
            logger.info(
                "yadisk flow: extract done expected=%s prepared=%s ms=%s",
                expected,
                extracted_path,
                self._elapsed_ms(t_phase),
            )
            self._notify_progress(
                progress_cb,
                "Распаковка завершена",
                f"Готовый путь: {extracted_path}",
                self._elapsed_ms(t_phase),
            )

            t_phase = time.perf_counter()
            self._notify_progress(
                progress_cb,
                "Обработка файла",
                "Запуск сценария обработки (парсинг / таблица)…",
                None,
            )
            outcome = await self._process_prepared_source(
                expected,
                extracted_path,
                file_info,
                no_move_export_mode=no_move_export_mode,
                progress_cb=progress_cb,
            )
            logger.info(
                "yadisk flow: prepared source done expected=%s ms=%s",
                expected,
                self._elapsed_ms(t_phase),
            )
            self._notify_progress(
                progress_cb,
                "Обработка завершена",
                f"Итог: {outcome.title}",
                self._elapsed_ms(t_phase),
            )
            return WorkflowOutcome(
                title=outcome.title,
                message=outcome.message,
                source_name=latest["name"],
                sheet_url=outcome.sheet_url,
                level=outcome.level,
                parse_mode=outcome.parse_mode,
                disable_web_page_preview=outcome.disable_web_page_preview,
                payload={**outcome.payload, "yadisk_path": latest["path"]},
            )
        finally:
            self._cleanup_temp_artifacts(temp_path)
            if extracted_path is not None and extracted_path != temp_path:
                self._cleanup_temp_artifacts(extracted_path)

    async def process_warehouse_delay_multiple(self) -> WorkflowOutcome:
        token = self._require_yadisk_token()
        files = await yadisk_list_files(
            token,
            self.config.yandex_warehouse_delay_dir or "disk:/BOT_UPLOADS/warehouse_delay/",
            self.config.yandex_allowed_exts,
        )
        aggregation = await self.download_and_aggregate_warehouse_delay_files(files)
        return await self.build_warehouse_delay_multiple_outcome(aggregation)

    async def download_and_aggregate_warehouse_delay_files(
        self, files: list[dict[str, Any]]
    ) -> WarehouseDelayAggregationResult:
        token = self._require_yadisk_token()
        with TemporaryDirectory(prefix="warehouse_delay_") as temp_dir:
            workdir = Path(temp_dir)
            local_files: list[tuple[str, str | Path]] = []
            for index, file_meta in enumerate(files, start=1):
                filename = file_meta["name"]
                suffix = self._suffix_from_name(filename)
                local_path = workdir / f"{index:03d}_{Path(filename).stem}{suffix}"
                await yadisk_download_file(
                    token,
                    file_meta["path"],
                    str(local_path),
                    max_bytes=self.config.yandex_max_mb * 1024 * 1024,
                )
                prepared_path = await asyncio.to_thread(
                    maybe_extract_zip,
                    str(local_path),
                    str(workdir),
                )
                local_files.append((filename, prepared_path))
            return await asyncio.to_thread(aggregate_warehouse_delay_files, local_files)

    async def build_warehouse_delay_multiple_outcome(
        self, aggregation: WarehouseDelayAggregationResult
    ) -> WorkflowOutcome:
        sheet_rows = await asyncio.to_thread(
            build_warehouse_delay_sheet_matrix,
            aggregation,
            datetime.now(WAREHOUSE_DELAY_TZ).date(),
        )
        await asyncio.to_thread(
            update_warehouse_delay_sheet,
            self.gc,
            self.config.spreadsheet_id,
            self.config.warehouse_delay_worksheet_name or "Выгрузка задержка склада",
            sheet_rows,
        )
        message = (
            "Сводная по задержке склада обновлена."
            f"\nОбработано файлов: {aggregation.processed_files_count}."
            f"\nПропущено файлов: {aggregation.skipped_files_count}."
            f"\nТоп без задания: {len(aggregation.top_without_assignment)}."
        )
        if aggregation.skipped_files:
            skipped = ", ".join(escape(name) for name in aggregation.skipped_files[:5])
            message += f"\nПропущенные файлы: {skipped}."
        return WorkflowOutcome(
            title="Задержка склада",
            message=message,
            source_name=self.config.yandex_warehouse_delay_dir,
            sheet_url=self.sheet_url(),
            payload={
                "processed_files_count": aggregation.processed_files_count,
                "skipped_files_count": aggregation.skipped_files_count,
                "top_without_assignment": len(aggregation.top_without_assignment),
            },
        )

    def sheet_link_html(self) -> str | None:
        sheet_url = self.sheet_url()
        if not sheet_url:
            return None
        return f'<a href="{sheet_url}">Ссылка на таблицу</a>'

    def sheet_url(self) -> str | None:
        if not self.config.spreadsheet_id:
            return None
        return f"https://docs.google.com/spreadsheets/d/{escape(self.config.spreadsheet_id)}/edit"

    async def _process_prepared_source(
        self,
        expected: str,
        prepared_path: Path,
        file_info: SourceFileInfo,
        *,
        no_move_export_mode: str | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> WorkflowOutcome:
        if expected == EXPECTED_NO_MOVE:
            return await self._process_no_move(prepared_path, file_info, no_move_export_mode)
        if expected == EXPECTED_24H:
            return await self._process_24h(prepared_path, file_info, progress_cb=progress_cb)
        if expected == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            return await self._process_warehouse_delay_single(prepared_path, file_info)
        raise WorkflowError("Неизвестный режим обработки.")

    async def _process_no_move(
        self,
        file_path: Path,
        file_info: SourceFileInfo,
        export_mode: str | None,
    ) -> WorkflowOutcome:
        if not export_mode:
            raise WorkflowError(
                "Сначала выберите тип выгрузки для режима «Без движения»."
            )
        rows, unknown_summary, stats = await asyncio.to_thread(
            process_file, file_path, export_mode
        )
        id_to_tary = stats.get("id_to_tary", {})
        await asyncio.to_thread(
            save_no_move_map,
            id_to_tary,
            {
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "source_filename": file_info.filename,
                "identifiers_count": len(id_to_tary),
                "groups_count": len(rows),
            },
            self.no_move_map_path,
        )
        snapshot, meta = await asyncio.to_thread(
            load_snapshot,
            self.snapshot_path,
            self.snapshot_meta_path,
        )
        right_rows = build_24h_table(snapshot, id_to_tary) if snapshot else []
        try:
            await asyncio.to_thread(
                update_tables,
                self.gc,
                self.config.spreadsheet_id,
                self.config.worksheet_name,
                rows,
                right_rows,
                meta,
                False,
                False,
            )
        except Exception as exc:
            logger.exception(
                "Google Sheets update failed (no_move) file=%s",
                file_info.filename,
            )
            raise WorkflowError(
                "Не удалось обновить Google Таблицу. Проверьте доступ и лимиты API; "
                "подробности в логах сервера."
            ) from exc
        message = (
            "Готово. Левая таблица обновлена."
            f"\nСтрок для выгрузки: {len(rows)}."
            f"\nСтрок 24ч: {len(right_rows)}."
        )
        if unknown_summary["count"] > 0:
            values = ", ".join(unknown_summary["values"])
            message += (
                f"\nНестандартные Гофры (порог > 2000): {unknown_summary['count']} шт."
                f"\nЗначения: {escape(values)}"
            )
        return WorkflowOutcome(
            title="Без движения",
            message=message,
            source_name=file_info.filename,
            sheet_url=self.sheet_url(),
            payload={
                "rows": len(rows),
                "right_rows": len(right_rows),
                "product_ids_count": len(stats.get("product_ids", set())),
            },
        )

    async def _process_24h(
        self,
        file_path: Path,
        file_info: SourceFileInfo,
        *,
        progress_cb: ProgressCallback | None = None,
    ) -> WorkflowOutcome:
        t_phase = time.perf_counter()
        self._notify_progress(
            progress_cb,
            "Парсинг 24ч",
            f"Читаем Excel: {file_path}",
            None,
        )
        block_ids = await asyncio.to_thread(load_block_ids, self.block_ids_path)
        snapshot, meta = await asyncio.to_thread(process_24h_file, file_path, block_ids)
        logger.info(
            "24h: process_24h_file done file=%s ms=%s",
            file_info.filename,
            self._elapsed_ms(t_phase),
        )
        self._notify_progress(
            progress_cb,
            "Парсинг 24ч завершён",
            f"Строк в исходнике: {meta.rows_total}; валидных: {meta.rows_valid}.",
            self._elapsed_ms(t_phase),
        )

        t_phase = time.perf_counter()
        self._notify_progress(
            progress_cb,
            "Снимок 24ч",
            "Сохраняем снимок на диск…",
            None,
        )
        try:
            await asyncio.to_thread(
                save_snapshot,
                snapshot,
                meta,
                self.snapshot_path,
                self.snapshot_meta_path,
            )
        except Exception as exc:
            logger.exception("save_snapshot failed file=%s", file_info.filename)
            raise WorkflowError(
                "Не удалось сохранить снимок выгрузки 24ч на сервере. Проверьте права на каталог data/."
            ) from exc
        logger.info(
            "24h: save_snapshot done file=%s ms=%s",
            file_info.filename,
            self._elapsed_ms(t_phase),
        )
        self._notify_progress(
            progress_cb,
            "Снимок сохранён",
            str(self.snapshot_path),
            self._elapsed_ms(t_phase),
        )

        t_phase = time.perf_counter()
        self._notify_progress(
            progress_cb,
            "Карта «Без движения»",
            "Загружаем сохранённую карту ID тары…",
            None,
        )
        id_to_tary, _ = await asyncio.to_thread(load_no_move_map, self.no_move_map_path)
        logger.info(
            "24h: load_no_move_map done file=%s map_size=%s ms=%s",
            file_info.filename,
            len(id_to_tary),
            self._elapsed_ms(t_phase),
        )
        self._notify_progress(
            progress_cb,
            "Карта «Без движения» загружена",
            f"Записей в карте: {len(id_to_tary)}.",
            self._elapsed_ms(t_phase),
        )

        if not id_to_tary:
            self._notify_progress(
                progress_cb,
                "Нет карты no_move",
                "Сначала выполните выгрузку «Без движения».",
                None,
            )
            return WorkflowOutcome(
                title="24 часа",
                level="warning",
                source_name=file_info.filename,
                message=(
                    "Файл 24ч обновлён, но нет сохранённой карты ID тары."
                    "\nСначала выполните выгрузку «Без движения»."
                ),
                payload={"rows_valid": meta.rows_valid},
            )
        right_rows = build_24h_table(snapshot, id_to_tary)
        t_phase = time.perf_counter()
        self._notify_progress(
            progress_cb,
            "Google Таблица",
            "Записываем правый блок (24ч)…",
            None,
        )
        try:
            await asyncio.to_thread(
                update_tables,
                self.gc,
                self.config.spreadsheet_id,
                self.config.worksheet_name,
                [],
                right_rows,
                meta.__dict__,
                True,
                False,
            )
        except Exception as exc:
            logger.exception(
                "Google Sheets update failed (24h) file=%s",
                file_info.filename,
            )
            raise WorkflowError(
                "Не удалось обновить Google Таблицу (блок 24ч). Проверьте доступ и лимиты API; "
                "подробности в логах сервера."
            ) from exc
        logger.info(
            "24h: update_tables done file=%s right_rows=%s ms=%s",
            file_info.filename,
            len(right_rows),
            self._elapsed_ms(t_phase),
        )
        self._notify_progress(
            progress_cb,
            "Google Sheets обновлён",
            f"Выгружено строк в правый блок: {len(right_rows)}.",
            self._elapsed_ms(t_phase),
        )
        message = (
            "Файл 24ч обновлён и загружен в таблицу."
            f"\nСтрок в исходнике: {meta.rows_total}."
            f"\nПосле фильтров: {meta.rows_after_filter}."
            f"\nСохранено уникальных товаров: {meta.rows_valid}."
            f"\nСтрок выгружено в правый блок: {len(right_rows)}."
        )
        return WorkflowOutcome(
            title="24 часа",
            message=message,
            source_name=file_info.filename,
            sheet_url=self.sheet_url(),
            payload={
                "rows_total": meta.rows_total,
                "rows_after_filter": meta.rows_after_filter,
                "rows_valid": meta.rows_valid,
                "right_rows": len(right_rows),
            },
        )

    async def _process_warehouse_delay_single(
        self,
        file_path: Path,
        file_info: SourceFileInfo,
    ) -> WorkflowOutcome:
        try:
            aggregation = await asyncio.to_thread(
                process_warehouse_delay_consolidated_file,
                file_path,
                file_info.filename,
            )
        except WarehouseDelayError as exc:
            raise WorkflowError(str(exc)) from exc
        sheet_rows = await asyncio.to_thread(
            build_warehouse_delay_sheet_matrix,
            aggregation,
            datetime.now(WAREHOUSE_DELAY_TZ).date(),
        )
        await asyncio.to_thread(
            update_warehouse_delay_sheet,
            self.gc,
            self.config.spreadsheet_id,
            self.config.warehouse_delay_worksheet_name or "Выгрузка задержка склада",
            sheet_rows,
        )
        file_stats = aggregation.processed_files[0] if aggregation.processed_files else None
        skipped_blocks = file_stats.skipped_unknown_rows if file_stats else 0
        message = (
            "Сводная по задержке склада обновлена из одного файла."
            f"\nОбработано строк: {file_stats.processed_rows if file_stats else 0}."
            f"\nБез задания: {file_stats.no_assignment_rows if file_stats else 0}."
            f"\nТоп без задания: {len(aggregation.top_without_assignment)}."
        )
        if skipped_blocks:
            message += f"\nПропущено строк с неизвестным блоком: {skipped_blocks}."
        return WorkflowOutcome(
            title="Задержка склада",
            message=message,
            source_name=file_info.filename,
            sheet_url=self.sheet_url(),
            payload={
                "processed_rows": file_stats.processed_rows if file_stats else 0,
                "no_assignment_rows": file_stats.no_assignment_rows if file_stats else 0,
                "top_without_assignment": len(aggregation.top_without_assignment),
            },
        )

    def _folder_for_expected(self, expected: str) -> str:
        if expected == EXPECTED_NO_MOVE:
            return self.config.yandex_no_move_dir or "disk:/BOT_UPLOADS/no_move/"
        if expected == EXPECTED_24H:
            return self.config.yandex_24h_dir or "disk:/BOT_UPLOADS/24h/"
        if expected == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            return (
                self.config.yandex_warehouse_delay_dir
                or "disk:/BOT_UPLOADS/warehouse_delay/"
            )
        raise WorkflowError("Для выбранного режима нет папки Яндекс.Диска.")

    def _require_yadisk_token(self) -> str:
        if not self.config.yandex_oauth_token:
            raise WorkflowError("YANDEX_OAUTH_TOKEN не настроен.")
        return self.config.yandex_oauth_token

    def _suffix_from_name(self, value: str) -> str:
        suffix = Path(value).suffix.lower()
        return suffix if suffix else ".xlsx"

    def _cleanup_temp_artifacts(self, path: Path) -> None:
        if path.is_dir():
            if not path.exists():
                return
            for child in sorted(path.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink(missing_ok=True)
                else:
                    child.rmdir()
            path.rmdir()
            return
        path.unlink(missing_ok=True)

    def _acquire_global_lock(self):
        self.global_lock_path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        if self.global_lock_path.exists():
            try:
                age_seconds = now - self.global_lock_path.stat().st_mtime
                if age_seconds > GLOBAL_LOCK_STALE_SECONDS:
                    self.global_lock_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to inspect or cleanup stale processing lock")

        try:
            fd = os.open(self.global_lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ProcessingBusyError(
                "Сейчас выполняется другая обработка. Повторите чуть позже."
            ) from exc

        with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(f"pid={os.getpid()}\ncreated_at={int(now)}\n")

        def _release() -> None:
            try:
                self.global_lock_path.unlink(missing_ok=True)
            except OSError:
                logger.warning(
                    "Failed to release processing lock at %s",
                    self.global_lock_path,
                )

        return _release
