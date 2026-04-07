from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bot.config import Config
from bot.services.ai.types import (
    AI24hSnapshotContent,
    AIExtractionResult,
    AISourceRef,
    AIWarehouseContent,
)
from bot.services.excel_24h import SnapshotMeta, load_snapshot, process_24h_file
from bot.services.file_sources import maybe_extract_zip
from bot.services.processing import ProcessingService
from bot.services.warehouse_delay import (
    WarehouseDelayError,
    process_warehouse_delay_consolidated_file,
)
from bot.services.yadisk import YaDiskError, yadisk_download_file, yadisk_list_latest

logger = logging.getLogger(__name__)

_SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".zip"}
_NO_MOVE_REQUIRED_COLUMNS = {"гофра", "шк", "стоимость"}


class AISourceError(Exception):
    pass


@dataclass(frozen=True)
class LoadedSourceBatch:
    extractions: tuple[AIExtractionResult, ...]
    cleanup_paths: tuple[Path, ...]


class AISourceLoader:
    def __init__(self, config: Config, processing_service: ProcessingService) -> None:
        self.config = config
        self.processing_service = processing_service

    async def load_sources(self, source_refs: list[AISourceRef]) -> LoadedSourceBatch:
        extractions: list[AIExtractionResult] = []
        cleanup_paths: list[Path] = []

        for source_ref in source_refs:
            extraction, paths = await self._load_single_source(source_ref)
            extractions.append(extraction)
            cleanup_paths.extend(paths)

        return LoadedSourceBatch(
            extractions=tuple(extractions),
            cleanup_paths=tuple(cleanup_paths),
        )

    def cleanup(self, paths: tuple[Path, ...]) -> None:
        for path in paths:
            self.processing_service._cleanup_temp_artifacts(path)

    async def _load_single_source(
        self,
        source_ref: AISourceRef,
    ) -> tuple[AIExtractionResult, list[Path]]:
        if source_ref.kind == "uploaded_file":
            if source_ref.file_path is None or source_ref.filename is None:
                raise AISourceError("Внутренняя ошибка: не сохранён путь загруженного файла.")
            extraction = await self._extract_local_file(
                source_ref=source_ref,
                file_path=source_ref.file_path,
                display_name=source_ref.filename,
            )
            return extraction, []

        if source_ref.kind == "yadisk_no_move_latest":
            return await self._load_latest_yadisk_source(
                source_ref=source_ref,
                folder=self.config.yandex_no_move_dir,
            )

        if source_ref.kind == "yadisk_warehouse_latest":
            return await self._load_latest_yadisk_source(
                source_ref=source_ref,
                folder=self.config.yandex_warehouse_delay_dir,
            )

        if source_ref.kind == "project_24h":
            extraction = await self._load_project_24h_source(source_ref)
            return extraction, []

        raise AISourceError("Неизвестный AI-источник.")

    async def _load_latest_yadisk_source(
        self,
        *,
        source_ref: AISourceRef,
        folder: str | None,
    ) -> tuple[AIExtractionResult, list[Path]]:
        if not self.config.yandex_oauth_token:
            raise AISourceError("YANDEX_OAUTH_TOKEN не настроен.")
        if not folder:
            raise AISourceError("Для выбранного источника не настроена папка Яндекс.Диска.")

        latest = await yadisk_list_latest(
            self.config.yandex_oauth_token,
            folder,
            tuple(sorted(_SUPPORTED_EXTENSIONS)),
        )
        filename = str(latest.get("name") or "source.xlsx")
        temp_path = self.processing_service.make_temp_path(
            "ai_source",
            Path(filename).suffix or ".xlsx",
        )
        download_info = await yadisk_download_file(
            self.config.yandex_oauth_token,
            str(latest.get("path") or ""),
            str(temp_path),
            max_bytes=self.config.ai_max_file_mb * 1024 * 1024,
        )
        extraction = await self._extract_local_file(
            source_ref=AISourceRef(
                kind=source_ref.kind,
                label=source_ref.label,
                filename=filename,
                file_path=Path(download_info["path"]),
            ),
            file_path=Path(download_info["path"]),
            display_name=filename,
        )
        return extraction, [temp_path]

    async def _load_project_24h_source(self, source_ref: AISourceRef) -> AIExtractionResult:
        snapshot_path = self.processing_service.snapshot_path
        meta_path = self.processing_service.snapshot_meta_path
        if snapshot_path.exists() and meta_path.exists():
            snapshot, meta_dict = load_snapshot(snapshot_path, meta_path)
            if snapshot and meta_dict:
                meta = SnapshotMeta(**meta_dict)
                return AIExtractionResult(
                    source_ref=source_ref,
                    extracted_kind="h24",
                    display_name=source_ref.label,
                    content=AI24hSnapshotContent(snapshot=snapshot, meta=meta),
                    rows_scanned=len(snapshot),
                    notes=("Использован runtime snapshot 24ч.",),
                )

        if not self.config.yandex_oauth_token:
            raise AISourceError("Нет runtime snapshot 24ч и не настроен YANDEX_OAUTH_TOKEN.")
        latest = await yadisk_list_latest(
            self.config.yandex_oauth_token,
            self.config.yandex_24h_dir or "disk:/BOT_UPLOADS/24h/",
            (".xlsx", ".xls", ".zip"),
        )
        filename = str(latest.get("name") or "24h.xlsx")
        temp_path = self.processing_service.make_temp_path(
            "ai_h24",
            Path(filename).suffix or ".xlsx",
        )
        cleanup_paths = [temp_path]
        try:
            download_info = await yadisk_download_file(
                self.config.yandex_oauth_token,
                str(latest.get("path") or ""),
                str(temp_path),
                max_bytes=self.config.ai_max_file_mb * 1024 * 1024,
            )
            prepared_path = Path(
                maybe_extract_zip(download_info["path"], str(Path(download_info["path"]).parent))
            )
            if prepared_path != temp_path:
                cleanup_paths.append(prepared_path)
            block_ids = load_block_ids_safe(self.processing_service.block_ids_path)
            snapshot, meta = process_24h_file(prepared_path, block_ids)
            return AIExtractionResult(
                source_ref=source_ref,
                extracted_kind="h24",
                display_name=filename,
                content=AI24hSnapshotContent(snapshot=snapshot, meta=meta),
                rows_scanned=meta.rows_total,
                notes=("Runtime snapshot отсутствовал, использован последний файл 24ч с Я.Диска.",),
            )
        except (ValueError, YaDiskError) as exc:
            raise AISourceError(str(exc)) from exc
        finally:
            self.cleanup(tuple(cleanup_paths))

    async def _extract_local_file(
        self,
        *,
        source_ref: AISourceRef,
        file_path: Path,
        display_name: str,
    ) -> AIExtractionResult:
        suffix = file_path.suffix.lower()
        if suffix not in _SUPPORTED_EXTENSIONS:
            raise AISourceError(
                "Поддерживаются только .xlsx, .xls, .csv и .zip-файлы."
            )
        if file_path.stat().st_size > self.config.ai_max_file_mb * 1024 * 1024:
            raise AISourceError(
                f"Файл слишком большой для AI-режима. Лимит: {self.config.ai_max_file_mb} МБ."
            )

        prepared_path = file_path
        cleanup_prepared = False
        if suffix == ".zip":
            try:
                prepared_path = Path(maybe_extract_zip(str(file_path), str(file_path.parent)))
            except ValueError as exc:
                raise AISourceError(str(exc)) from exc
            cleanup_prepared = prepared_path != file_path

        try:
            if prepared_path.suffix.lower() in {".xlsx", ".xls"}:
                h24_extraction = self._try_extract_h24(source_ref, prepared_path, display_name)
                if h24_extraction is not None:
                    return h24_extraction

                warehouse_extraction = self._try_extract_warehouse_delay(
                    source_ref,
                    prepared_path,
                    display_name,
                )
                if warehouse_extraction is not None:
                    return warehouse_extraction

            dataframe = self._read_tabular_dataframe(prepared_path)
            extracted_kind = "no_move" if self._looks_like_no_move(dataframe) else "table"
            rows_scanned = min(len(dataframe), self.config.ai_max_scan_rows_per_source)
            return AIExtractionResult(
                source_ref=source_ref,
                extracted_kind=extracted_kind,
                display_name=display_name,
                content=dataframe,
                rows_scanned=rows_scanned,
                was_truncated=len(dataframe) > rows_scanned,
            )
        finally:
            if cleanup_prepared:
                self.processing_service._cleanup_temp_artifacts(prepared_path)

    def _try_extract_h24(
        self,
        source_ref: AISourceRef,
        file_path: Path,
        display_name: str,
    ) -> AIExtractionResult | None:
        try:
            snapshot, meta = process_24h_file(
                file_path,
                load_block_ids_safe(self.processing_service.block_ids_path),
            )
        except ValueError:
            return None
        return AIExtractionResult(
            source_ref=source_ref,
            extracted_kind="h24",
            display_name=display_name,
            content=AI24hSnapshotContent(snapshot=snapshot, meta=meta),
            rows_scanned=meta.rows_total,
            notes=("Файл распознан как выгрузка 24ч.",),
        )

    def _try_extract_warehouse_delay(
        self,
        source_ref: AISourceRef,
        file_path: Path,
        display_name: str,
    ) -> AIExtractionResult | None:
        try:
            aggregation = process_warehouse_delay_consolidated_file(
                file_path,
                filename=display_name,
            )
        except WarehouseDelayError:
            return None
        rows_scanned = sum(item.processed_rows for item in aggregation.processed_files)
        return AIExtractionResult(
            source_ref=source_ref,
            extracted_kind="warehouse_delay",
            display_name=display_name,
            content=AIWarehouseContent(aggregation=aggregation),
            rows_scanned=rows_scanned,
            notes=("Файл распознан как сводная задержка склада.",),
        )

    def _read_tabular_dataframe(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":
            return self._read_csv_dataframe(path)
        read_kwargs: dict[str, str] = {}
        if path.suffix.lower() == ".xls":
            read_kwargs["engine"] = "xlrd"
        elif path.suffix.lower() == ".xlsx":
            read_kwargs["engine"] = "openpyxl"
        try:
            return pd.read_excel(path, nrows=self.config.ai_max_scan_rows_per_source, **read_kwargs)
        except Exception as exc:
            raise AISourceError(f"Не удалось прочитать табличный файл {path.name}: {exc}") from exc

    def _read_csv_dataframe(self, path: Path) -> pd.DataFrame:
        last_error: Exception | None = None
        for encoding in ("utf-8-sig", "utf-8", "cp1251"):
            try:
                sample = path.read_text(encoding=encoding, errors="strict")
                dialect = csv.Sniffer().sniff(sample[:4096] or "a,b\n1,2")
                return pd.read_csv(
                    path,
                    encoding=encoding,
                    sep=dialect.delimiter,
                    nrows=self.config.ai_max_scan_rows_per_source,
                )
            except Exception as exc:
                last_error = exc
        raise AISourceError(f"Не удалось прочитать CSV-файл {path.name}.") from last_error

    def _looks_like_no_move(self, dataframe: pd.DataFrame) -> bool:
        normalized_columns = {
            self._normalize_column_name(column) for column in dataframe.columns
        }
        return _NO_MOVE_REQUIRED_COLUMNS.issubset(normalized_columns)

    @staticmethod
    def _normalize_column_name(value: object) -> str:
        return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def load_block_ids_safe(path: Path) -> set[str]:
    if not path.exists():
        return set()
    from bot.services.block_ids import load_block_ids

    return load_block_ids(path)
