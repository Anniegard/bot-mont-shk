from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from bot.config import Config
from bot.services.block_ids import load_block_ids
from bot.services.excel import process_file
from bot.services.excel_24h import build_24h_table, load_snapshot, process_24h_file, save_snapshot
from bot.services.file_sources import maybe_extract_zip
from bot.services.no_move_map import load_no_move_map, save_no_move_map
from bot.services.sheets import update_tables, update_warehouse_delay_sheet
from bot.services.warehouse_delay import aggregate_warehouse_delay_files, build_warehouse_delay_sheet_matrix, process_warehouse_delay_consolidated_file
from bot.services.yadisk import yadisk_download_file, yadisk_list_files, yadisk_list_latest
from bot.services.yadisk_ingest import SOURCE_KIND_24H, SOURCE_KIND_NO_MOVE

logger = logging.getLogger(__name__)

EXPECTED_NO_MOVE = "no_move"
EXPECTED_24H = "24h"
EXPECTED_WAREHOUSE_DELAY_SINGLE = "warehouse_delay_single"
EXPECTED_WAREHOUSE_DELAY_MULTIPLE = "warehouse_delay_multiple"

MAX_TG_UPLOAD_BYTES = 20 * 1024 * 1024
GLOBAL_LOCK_STALE_SECONDS = 6 * 60 * 60

FlowKind = Literal[
    "no_move",
    "24h",
    "warehouse_delay_single",
    "warehouse_delay_multiple",
]


class WorkflowError(Exception):
    """Base public error for processing flows."""


class ProcessingBusyError(WorkflowError):
    """Raised when another task already owns the global processing slot."""


@dataclass(frozen=True)
class SourceFileInfo:
    filename: str
    size: int | None = None
    source: str = "upload"
    source_path: str | None = None
    source_url: str | None = None
    modified: str | None = None


@dataclass(frozen=True)
class OutcomeMetric:
    label: str
    value: str


@dataclass(frozen=True)
class ProcessingOutcome:
    kind: FlowKind
    summary: str
    metrics: tuple[OutcomeMetric, ...] = ()
    details: tuple[str, ...] = ()
    sheet_url: str | None = None
    source_name: str | None = None

    def to_plain_text(self) -> str:
        lines = [self.summary]
        lines.extend(f"{metric.label}: {metric.value}." for metric in self.metrics)
        lines.extend(self.details)
        if self.sheet_url:
            lines.append(f"Ссылка на таблицу: {self.sheet_url}")
        return "\n".join(lines)

    def to_html(self) -> str:
        lines = [escape(self.summary)]
        lines.extend(
            f"{escape(metric.label)}: {escape(metric.value)}."
            for metric in self.metrics
        )
        lines.extend(escape(detail) for detail in self.details)
        if self.sheet_url:
            lines.append(f'<a href="{escape(self.sheet_url)}">Ссылка на таблицу</a>')
        return "\n".join(lines)


@dataclass(frozen=True)
class WorkflowPaths:
    root_dir: Path
    data_dir: Path
    block_ids_path: Path
    snapshot_path: Path
    snapshot_meta_path: Path
    no_move_map_dir: Path
    global_lock_path: Path


@dataclass
class ProcessingService:
    config: Config
    gspread_client: object
    paths: WorkflowPaths
    last_ingest_source_kinds: list[str] = field(default_factory=list)

    @classmethod
    def from_root(
        cls,
        config: Config,
        gspread_client: object,
        root_dir: Path | None = None,
    ) -> ProcessingService:
        app_root = (root_dir or Path(__file__).resolve().parent.parent.parent).resolve()
        data_dir = app_root / "data"
        return cls(
            config=config,
            gspread_client=gspread_client,
            paths=WorkflowPaths(
                root_dir=app_root,
                data_dir=data_dir,
                block_ids_path=data_dir / "block_ids.txt",
                snapshot_path=data_dir / "last_24h_snapshot.json",
                snapshot_meta_path=data_dir / "last_24h_meta.json",
                no_move_map_dir=data_dir,
                global_lock_path=data_dir / "processing.lock",
            ),
        )

    @property
    def sheet_url(self) -> str | None:
        if not self.config.spreadsheet_id:
            return None
        return f"https://docs.google.com/spreadsheets/d/{self.config.spreadsheet_id}/edit"

    def build_yadisk_help_text(self) -> str:
        return (
            "Как загрузить файл на Яндекс.Диск и отправить боту:\n"
            f"- Для «без движения» положите файл в папку {self.config.yandex_no_move_dir or '/BOT_UPLOADS/no_move/'}\n"
            f"- Для «24 часа» — в папку {self.config.yandex_24h_dir or '/BOT_UPLOADS/24h/'}\n"
            f"- Для «задержка склада (сводная)» — в папку {self.config.yandex_warehouse_delay_dir or 'disk:/BOT_UPLOADS/warehouse_delay/'}\n"
            "Бот возьмёт последний Excel/zip из выбранной папки.\n"
            "Для сводной задержки склада можно выбрать один файл или обработку всех файлов из папки.\n"
            "Выберите режим, затем нажмите «☁️ Взять с Я.Диска (последний файл)»."
        )

    def make_temp_path(self, prefix: str, suffix: str) -> Path:
        normalized_suffix = suffix if suffix.startswith(".") else f".{suffix.lstrip('.')}"
        return self.paths.root_dir / f"{prefix}_{int(time.time() * 1000)}{normalized_suffix}"

    async def process_local_source(
        self,
        expected: str,
        file_path: str | Path,
        file_info: SourceFileInfo,
        *,
        no_move_export_mode: str | None = None,
    ) -> ProcessingOutcome:
        cleanup_paths: list[Path] = []
        try:
            excel_path = self._prepare_excel_source(file_path, cleanup_paths)
            async with self.processing_slot():
                return await self._process_excel_file(
                    expected=expected,
                    file_path=excel_path,
                    file_info=file_info,
                    no_move_export_mode=no_move_export_mode,
                )
        finally:
            self._cleanup_paths(cleanup_paths)

    async def process_latest_yadisk_file(
        self,
        expected: str,
        *,
        no_move_export_mode: str | None = None,
    ) -> ProcessingOutcome:
        self._ensure_yadisk_configured()
        folder = self._folder_for_expected(expected)
        latest = await yadisk_list_latest(
            self.config.yandex_oauth_token or "",
            folder,
            self.config.yandex_allowed_exts,
        )
        name = latest.get("name") or "yadisk.xlsx"
        temp_path = self.make_temp_path("yadisk", Path(name).suffix or ".xlsx")
        try:
            result = await yadisk_download_file(
                self.config.yandex_oauth_token or "",
                latest.get("path") or "",
                str(temp_path),
                max_bytes=self.config.yandex_max_mb * 1024 * 1024,
            )
            file_info = SourceFileInfo(
                filename=name,
                size=latest.get("size") or result.get("size"),
                source="yadisk",
                modified=latest.get("modified"),
                source_path=(
                    f"{latest.get('path')}|modified:{latest.get('modified')}"
                    if latest.get("modified")
                    else latest.get("path")
                ),
            )
            return await self.process_local_source(
                expected,
                result["path"],
                file_info,
                no_move_export_mode=no_move_export_mode,
            )
        finally:
            self._cleanup_paths([temp_path])

    async def process_warehouse_delay_multiple(self) -> ProcessingOutcome:
        self._ensure_yadisk_configured()
        folder = self.config.yandex_warehouse_delay_dir or "disk:/BOT_UPLOADS/warehouse_delay/"
        async with self.processing_slot():
            files = await yadisk_list_files(
                self.config.yandex_oauth_token or "",
                folder,
                self.config.yandex_allowed_exts,
            )
            aggregation = await self._download_and_process_warehouse_delay_files(files)
            if aggregation.processed_files_count == 0:
                if aggregation.skipped_files:
                    skipped = ", ".join(aggregation.skipped_files)
                    raise WorkflowError(
                        "Не удалось прочитать ни одного файла.\n"
                        f"Пропущенные файлы: {skipped}"
                    )
                raise WorkflowError("Не удалось прочитать ни одного файла из папки.")

            sheet_rows = build_warehouse_delay_sheet_matrix(
                aggregation,
                self._today_date(),
            )
            update_warehouse_delay_sheet(
                self.gspread_client,
                self.config.spreadsheet_id,
                self.config.warehouse_delay_worksheet_name or "Выгрузка задержка склада",
                sheet_rows,
            )
            details = tuple(
                f'Файл "{filename}" непонятен, поэтому я его не прочитал. Остальные файлы обработал.'
                for filename in aggregation.skipped_files
            )
            return ProcessingOutcome(
                kind=EXPECTED_WAREHOUSE_DELAY_MULTIPLE,
                summary="Сводная по задержке склада обновлена.",
                metrics=(
                    OutcomeMetric("Прочитано файлов", str(aggregation.processed_files_count)),
                    OutcomeMetric("Пропущено файлов", str(aggregation.skipped_files_count)),
                ),
                details=details,
                sheet_url=self.sheet_url,
            )

    @asynccontextmanager
    async def processing_slot(self):
        release = await asyncio.to_thread(self._acquire_global_lock)
        try:
            yield
        finally:
            await asyncio.to_thread(release)

    async def _process_excel_file(
        self,
        *,
        expected: str,
        file_path: str,
        file_info: SourceFileInfo,
        no_move_export_mode: str | None,
    ) -> ProcessingOutcome:
        if expected == EXPECTED_NO_MOVE:
            if not no_move_export_mode:
                raise WorkflowError(
                    "Сначала выберите тип выгрузки для режима «Без движения»."
                )
            return await asyncio.to_thread(
                self._process_no_move_file,
                file_path,
                file_info,
                no_move_export_mode,
            )
        if expected == EXPECTED_24H:
            return await asyncio.to_thread(self._process_24h_file, file_path, file_info)
        if expected == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            return await asyncio.to_thread(
                self._process_warehouse_delay_single_file,
                file_path,
                file_info,
            )
        raise WorkflowError("Неизвестный режим. Выберите корректный сценарий.")

    def _process_no_move_file(
        self,
        file_path: str,
        file_info: SourceFileInfo,
        export_mode: str,
    ) -> ProcessingOutcome:
        rows, unknown_summary, stats = process_file(file_path, export_mode)
        self._remember_runtime_ingest(SOURCE_KIND_NO_MOVE)
        product_ids = stats.get("product_ids", set())
        id_to_tary = stats.get("id_to_tary", {})
        save_no_move_map(
            id_to_tary,
            {
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "source_filename": file_info.filename,
                "identifiers_count": len(id_to_tary),
                "groups_count": len(rows),
            },
            self.paths.no_move_map_dir,
        )

        snapshot, meta = load_snapshot(
            self.paths.snapshot_path,
            self.paths.snapshot_meta_path,
        )
        right_rows = build_24h_table(snapshot, id_to_tary) if snapshot else []
        update_tables(
            self.gspread_client,
            self.config.spreadsheet_id,
            self.config.worksheet_name,
            rows,
            right_rows,
            meta,
            skip_left=False,
            skip_right=False,
        )
        details: list[str] = []
        if unknown_summary["count"] > 0:
            values = ", ".join(unknown_summary["values"])
            details.append(
                f"Нестандартные Гофры (порог > 2000): {unknown_summary['count']} шт."
            )
            details.append(f"Значения: {values}")

        return ProcessingOutcome(
            kind=EXPECTED_NO_MOVE,
            summary="Готово. Левая таблица обновлена.",
            metrics=(
                OutcomeMetric("Тип выгрузки", _export_mode_label(export_mode)),
                OutcomeMetric("Строк для выгрузки", str(len(rows))),
                OutcomeMetric("Строк 24ч", str(len(right_rows))),
                OutcomeMetric("Идентификаторов", str(len(product_ids))),
            ),
            details=tuple(details),
            sheet_url=self.sheet_url,
            source_name=file_info.filename,
        )

    def _process_24h_file(
        self,
        file_path: str,
        file_info: SourceFileInfo,
    ) -> ProcessingOutcome:
        block_ids = load_block_ids(self.paths.block_ids_path)
        snapshot, meta = process_24h_file(file_path, block_ids)
        self._remember_runtime_ingest(SOURCE_KIND_24H)
        save_snapshot(
            snapshot,
            meta,
            self.paths.snapshot_path,
            self.paths.snapshot_meta_path,
        )
        id_to_tary, _ = load_no_move_map(self.paths.no_move_map_dir)
        if not id_to_tary:
            return ProcessingOutcome(
                kind=EXPECTED_24H,
                summary=(
                    "Файл 24ч обновлён, но нет сохранённой карты ID тары. "
                    "Сначала выполните выгрузку «Без движения»."
                ),
                metrics=(
                    OutcomeMetric("Строк в исходнике", str(meta.rows_total)),
                    OutcomeMetric("После фильтров", str(meta.rows_after_filter)),
                    OutcomeMetric("Сохранено товаров", str(meta.rows_valid)),
                ),
                source_name=file_info.filename,
            )

        right_rows = build_24h_table(snapshot, id_to_tary)
        update_tables(
            self.gspread_client,
            self.config.spreadsheet_id,
            self.config.worksheet_name,
            left_rows=[],
            right_rows=right_rows,
            right_meta=meta.__dict__,
            skip_left=True,
            skip_right=False,
        )
        return ProcessingOutcome(
            kind=EXPECTED_24H,
            summary="Файл 24ч обновлён и загружен в таблицу.",
            metrics=(
                OutcomeMetric("Строк в исходнике", str(meta.rows_total)),
                OutcomeMetric("После фильтров", str(meta.rows_after_filter)),
                OutcomeMetric("Сохранено уникальных товаров", str(meta.rows_valid)),
                OutcomeMetric("Строк в правом блоке", str(len(right_rows))),
            ),
            sheet_url=self.sheet_url,
            source_name=file_info.filename,
        )

    def _process_warehouse_delay_single_file(
        self,
        file_path: str,
        file_info: SourceFileInfo,
    ) -> ProcessingOutcome:
        aggregation = process_warehouse_delay_consolidated_file(
            file_path,
            filename=file_info.filename,
        )
        sheet_rows = build_warehouse_delay_sheet_matrix(
            aggregation,
            self._today_date(),
        )
        update_warehouse_delay_sheet(
            self.gspread_client,
            self.config.spreadsheet_id,
            self.config.warehouse_delay_worksheet_name or "Выгрузка задержка склада",
            sheet_rows,
        )
        file_stats = aggregation.processed_files[0] if aggregation.processed_files else None
        skipped_blocks = file_stats.skipped_unknown_rows if file_stats else 0
        details: list[str] = []
        if skipped_blocks:
            details.append(f"Пропущено строк с неизвестным блоком: {skipped_blocks}.")
        return ProcessingOutcome(
            kind=EXPECTED_WAREHOUSE_DELAY_SINGLE,
            summary="Сводная по задержке склада обновлена из одного файла.",
            metrics=(
                OutcomeMetric(
                    "Обработано строк",
                    str(file_stats.processed_rows if file_stats else 0),
                ),
                OutcomeMetric(
                    "Без задания",
                    str(file_stats.no_assignment_rows if file_stats else 0),
                ),
                OutcomeMetric(
                    "Топ без задания",
                    str(len(aggregation.top_without_assignment)),
                ),
            ),
            details=tuple(details),
            sheet_url=self.sheet_url,
            source_name=file_info.filename,
        )

    async def _download_and_process_warehouse_delay_files(self, files: list[dict]):
        with TemporaryDirectory(
            prefix="warehouse_delay_",
            dir=self.paths.root_dir,
        ) as temp_dir:
            download_dir = Path(temp_dir)
            processable_files: list[tuple[str, str]] = []
            skipped_files: list[str] = []

            for index, file_info in enumerate(files, start=1):
                filename = file_info.get("name") or f"file_{index}.xlsx"
                source_path = file_info.get("path")
                if not source_path:
                    logger.warning("Warehouse delay skipped file without path: %s", file_info)
                    continue

                try:
                    temp_path = download_dir / f"{index:03d}_{filename}"
                    result = await yadisk_download_file(
                        self.config.yandex_oauth_token or "",
                        source_path,
                        str(temp_path),
                        max_bytes=self.config.yandex_max_mb * 1024 * 1024,
                    )
                    excel_path = maybe_extract_zip(result["path"], download_dir)
                    processable_files.append((filename, excel_path))
                except Exception:
                    logger.exception(
                        "Warehouse delay failed to download or extract file=%s path=%s",
                        filename,
                        source_path,
                    )
                    skipped_files.append(filename)

            aggregation = aggregate_warehouse_delay_files(processable_files)
            aggregation.skipped_files.extend(skipped_files)
            return aggregation

    def _prepare_excel_source(
        self,
        file_path: str | Path,
        cleanup_paths: list[Path],
    ) -> str:
        original_path = Path(file_path)
        excel_path = maybe_extract_zip(str(original_path), self.paths.root_dir)
        extracted_path = Path(excel_path)
        if extracted_path != original_path:
            cleanup_paths.append(extracted_path)
        if extracted_path.suffix.lower() not in {".xlsx", ".xls"}:
            raise WorkflowError("Ожидался Excel (.xlsx/.xls). Проверьте файл.")
        return str(extracted_path)

    def _ensure_yadisk_configured(self) -> None:
        if not self.config.yandex_oauth_token:
            raise WorkflowError("Яндекс OAuth токен не настроен. Обратитесь к администратору.")

    def _folder_for_expected(self, expected: str) -> str:
        if expected == EXPECTED_NO_MOVE:
            return self.config.yandex_no_move_dir or "/"
        if expected == EXPECTED_24H:
            return self.config.yandex_24h_dir or "/"
        if expected == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            return self.config.yandex_warehouse_delay_dir or "/"
        raise WorkflowError("Сначала выберите режим обработки.")

    def _acquire_global_lock(self):
        self.paths.global_lock_path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        lock_path = self.paths.global_lock_path
        if lock_path.exists():
            try:
                age_seconds = now - lock_path.stat().st_mtime
                if age_seconds > GLOBAL_LOCK_STALE_SECONDS:
                    lock_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to inspect or cleanup stale processing lock")

        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ProcessingBusyError(
                "Сейчас выполняется другая обработка. Повторите чуть позже."
            ) from exc

        with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(f"pid={os.getpid()}\ncreated_at={int(now)}\n")

        def _release() -> None:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to release processing lock at %s", lock_path)

        return _release

    def _cleanup_paths(self, paths: list[Path]) -> None:
        for path in paths:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Не удалось удалить временный файл %s", path)

    def _remember_runtime_ingest(self, source_kind: str) -> None:
        self.last_ingest_source_kinds.append(source_kind)
        logger.info("Runtime DB features disabled; skipping raw ingest: kind=%s", source_kind)

    def _today_date(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Europe/Moscow")).date()


def _export_mode_label(export_mode: str) -> str:
    if export_mode == "export_with_transfers":
        return "с передачами"
    if export_mode == "export_without_transfers":
        return "без передач"
    if export_mode == "export_only_transfers":
        return "только передачи"
    return export_mode
