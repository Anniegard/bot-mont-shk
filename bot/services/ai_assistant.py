from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from bot.config import Config
from bot.services.ai_assistant_sources import (
    PLANNER_JSON_SCHEMA,
    PlannerResult,
    ResolvedYadiskFile,
    SourceType,
    build_folder_catalogs,
    configured_source_types,
    fallback_planner_from_question,
    folder_path_for_source,
    format_24h_snapshot_for_llm,
    format_catalog_for_planner,
    format_warehouse_aggregation_for_llm,
    load_24h_snapshot_sync,
    parse_json_lenient,
    parse_planner_payload,
    planner_system_prompt,
    resolve_planner_to_files,
    try_warehouse_consolidated_or_aggregate,
    validate_and_normalize_plan,
    warehouse_delay_rows_preview,
)
from bot.services.excel import MIN_COST_THRESHOLD, load_no_move_dataframe
from bot.services.file_sources import maybe_extract_zip
from bot.services.ollama_client import OllamaClient, OllamaError, OllamaMessage
from bot.services.processing import ProcessingService
from bot.services.warehouse_delay import WarehouseDelayError
from bot.services.yadisk import YaDiskError, yadisk_download_file

logger = logging.getLogger(__name__)

_NONSTANDARD_GOPHRA_PREFIXES = ("3", "4", "7", "9", "10")
_ITEM_NAME_COLUMN_ALIASES = (
    "наименование",
    "наименование товара",
    "товар",
    "номенклатура",
)
_MIN_COST_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"стоим\w*[^0-9]{0,30}(?:больше|выше|свыше|от|>=|>)\s*(\d+(?:[.,]\d+)?)",
            re.IGNORECASE,
        ),
        ">",
    ),
    (
        re.compile(
            r"стоим\w*[^0-9]{0,30}(?:не меньше|не ниже)\s*(\d+(?:[.,]\d+)?)",
            re.IGNORECASE,
        ),
        ">=",
    ),
)
_GOPHRA_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:гофр\w*|тар\w*)[^0-9]{0,10}(\d+)", re.IGNORECASE),
    re.compile(r"\b(\d+)\s*(?:гофр\w*|тар\w*)", re.IGNORECASE),
)


class AIAssistantError(Exception):
    pass


@dataclass(frozen=True)
class QueryFilters:
    gophra_value: str | None = None
    min_cost: float | None = None
    min_cost_operator: str = ">"
    nonstandard_only: bool = False


@dataclass(frozen=True)
class ContextPayload:
    text: str
    matched_rows: int
    total_rows: int
    applied_filters: tuple[str, ...]


@dataclass(frozen=True)
class AIAssistantResponse:
    text: str
    source_name: str
    source_path: str
    matched_rows: int
    applied_filters: tuple[str, ...]
    sources_used: tuple[str, ...]


@dataclass(frozen=True)
class _ContextBlock:
    label: str
    text: str
    matched_rows: int
    applied_filters: tuple[str, ...]


class AIAssistantService:
    def __init__(self, *, config: Config, processing_service: ProcessingService) -> None:
        self.config = config
        self.processing_service = processing_service
        self.ollama_client = OllamaClient(
            base_url=config.ollama_base_url,
            model=config.ollama_model,
            timeout_seconds=config.ollama_timeout_seconds,
        )

    def ensure_configured(self) -> None:
        self.ollama_client.ensure_configured()
        if not self.config.yandex_oauth_token:
            raise AIAssistantError("YANDEX_OAUTH_TOKEN не настроен.")
        if not configured_source_types(self.config):
            raise AIAssistantError(
                "Не задана ни одна папка Я.Диска для выгрузок "
                "(YANDEX_NO_MOVE_DIR / YANDEX_24H_DIR / YANDEX_WAREHOUSE_DELAY_DIR)."
            )

    async def _notify_status(self, status: Any, text: str) -> None:
        if status is None:
            return
        edit = getattr(status, "edit_text", None)
        if edit is None:
            return
        try:
            await edit(text)
        except Exception:
            logger.debug("AI assistant status edit skipped", exc_info=True)

    async def answer_question(
        self,
        *,
        question: str,
        history: Sequence[dict[str, str]] | None = None,
        status: Any | None = None,
    ) -> AIAssistantResponse:
        normalized_question = question.strip()
        if not normalized_question:
            raise AIAssistantError("Введите вопрос для AI-ассистента.")
        self.ensure_configured()

        available = configured_source_types(self.config)
        token = self.config.yandex_oauth_token or ""

        async with self.processing_service.processing_slot():
            await self._notify_status(status, "Собираю каталог файлов на Я.Диске…")
            catalogs = await build_folder_catalogs(
                config=self.config,
                token=token,
                per_folder_limit=self.config.ai_assistant_catalog_files_per_folder,
            )
            if not any(cat.files for cat in catalogs.values()):
                raise AIAssistantError(
                    "В настроенных папках Я.Диска нет подходящих файлов (xlsx/xls/zip)."
                )

            catalog_text = format_catalog_for_planner(
                catalogs,
                max_chars=self.config.ai_assistant_planner_max_catalog_chars,
            )
            await self._notify_status(status, "Планирую, какие файлы нужны для ответа…")
            plan = await self._run_planner(
                question=normalized_question,
                catalog_text=catalog_text,
                available=available,
            )
            plan = validate_and_normalize_plan(plan, available=available)
            if plan.need_data and not plan.sources:
                plan = fallback_planner_from_question(
                    normalized_question, available=available
                )
                logger.info("AI planner fallback applied sources=%s", plan.sources)

            if not plan.need_data:
                await self._notify_status(status, "Готовлю ответ без выгрузки файлов…")
                answer = (plan.answer_without_data or "").strip()
                if not answer:
                    answer = await self._general_reply_without_files(
                        normalized_question, history or ()
                    )
                finalized = self._finalize_reply(
                    answer,
                    source_name="—",
                    context_payload=ContextPayload(
                        text="",
                        matched_rows=0,
                        total_rows=0,
                        applied_filters=(),
                    ),
                    sources_used=(),
                )
                return AIAssistantResponse(
                    text=finalized,
                    source_name="—",
                    source_path="—",
                    matched_rows=0,
                    applied_filters=(),
                    sources_used=(),
                )

            resolved = resolve_planner_to_files(
                plan,
                catalogs,
                max_files=self.config.ai_assistant_max_yadisk_files_per_question,
            )
            resolved = _dedupe_resolved(resolved)
            if not resolved:
                logger.info("AI planner resolved zero files; applying keyword fallback")
                plan_fb = fallback_planner_from_question(
                    normalized_question, available=available
                )
                resolved = resolve_planner_to_files(
                    plan_fb,
                    catalogs,
                    max_files=self.config.ai_assistant_max_yadisk_files_per_question,
                )
                resolved = _dedupe_resolved(resolved)
            if not resolved:
                raise AIAssistantError(
                    "По плану нет файлов для загрузки: проверьте каталог на Я.Диске."
                )

            await self._notify_status(
                status,
                f"Загружаю с Я.Диска ({len(resolved)} файл(ов)) и готовлю контекст…",
            )
            temp_paths: list[Path] = []
            try:
                locals_by_type: dict[SourceType, list[tuple[str, Path]]] = {
                    "no_move": [],
                    "h24": [],
                    "warehouse_delay": [],
                }
                for item in resolved:
                    local = await self._download_yadisk_file(item.disk_path, item.name)
                    temp_paths.append(local)
                    locals_by_type[item.source_type].append((item.name, local))

                blocks = await self._build_all_context_blocks(
                    locals_by_type,
                    normalized_question,
                )
            finally:
                for p in temp_paths:
                    self.processing_service._cleanup_temp_artifacts(p)

        merged = _merge_context_blocks(
            blocks,
            max_chars=max(2000, self.config.ai_assistant_max_context_chars),
        )
        await self._notify_status(status, "Отправляю контекст в модель и формирую ответ…")
        messages = self._build_messages_multi(
            question=normalized_question,
            history=history or (),
            context_text=merged.text,
        )
        answer_text = await self.ollama_client.chat(messages)
        sources_used = tuple(b.label for b in blocks)
        source_summary = "; ".join(sources_used) if sources_used else "—"
        path_summary = "; ".join(f"{r.name}" for r in resolved[:5])
        if len(resolved) > 5:
            path_summary += "…"
        return AIAssistantResponse(
            text=self._finalize_reply(
                answer_text,
                source_name=source_summary,
                context_payload=ContextPayload(
                    text=merged.text,
                    matched_rows=merged.matched_rows,
                    total_rows=merged.total_rows,
                    applied_filters=merged.applied_filters,
                ),
                sources_used=sources_used,
            ),
            source_name=source_summary,
            source_path=path_summary,
            matched_rows=merged.matched_rows,
            applied_filters=merged.applied_filters,
            sources_used=sources_used,
        )

    async def _run_planner(
        self,
        *,
        question: str,
        catalog_text: str,
        available: Sequence[SourceType],
    ) -> PlannerResult:
        user_content = (
            f"Вопрос пользователя:\n{question}\n\n"
            f"Каталог файлов на Я.Диске:\n{catalog_text}"
        )
        messages = [
            OllamaMessage(role="system", content=planner_system_prompt(available)),
            OllamaMessage(role="user", content=user_content),
        ]
        try:
            raw = await self.ollama_client.chat_json(
                messages, json_schema=PLANNER_JSON_SCHEMA
            )
            return parse_planner_payload(raw)
        except OllamaError as first_exc:
            logger.info("AI planner chat_json failed: %s", first_exc)
        try:
            text = await self.ollama_client.chat(messages)
            data = parse_json_lenient(text)
            if data:
                return parse_planner_payload(data)
        except OllamaError as exc:
            logger.warning("AI planner retry failed: %s", exc)
        return PlannerResult(need_data=True, answer_without_data=None, sources=())

    async def _general_reply_without_files(
        self,
        question: str,
        history: Sequence[dict[str, str]],
    ) -> str:
        messages = [
            OllamaMessage(
                role="system",
                content=(
                    "Ты помощник Telegram-бота выгрузок. Пользователь задал вопрос, "
                    "не требующий данных из Excel. Ответь по-русски кратко и по делу."
                ),
            )
        ]
        for item in self.trim_history(history):
            role = item.get("role")
            content = (item.get("content") or "").strip()
            if role in {"user", "assistant"} and content:
                messages.append(OllamaMessage(role=role, content=content))
        messages.append(OllamaMessage(role="user", content=question))
        return await self.ollama_client.chat(messages)

    async def _download_yadisk_file(self, disk_path: str, name: str) -> Path:
        suffix = Path(name).suffix or ".xlsx"
        temp_path = self.processing_service.make_temp_path("ai_yadisk", suffix)
        extracted_path: Path | None = None
        try:
            download_info = await yadisk_download_file(
                self.config.yandex_oauth_token or "",
                disk_path,
                str(temp_path),
                max_bytes=self.config.yandex_max_mb * 1024 * 1024,
            )
            prepared = await asyncio.to_thread(
                maybe_extract_zip,
                download_info["path"],
                str(Path(download_info["path"]).parent),
            )
            extracted_path = Path(prepared)
            return extracted_path
        except (ValueError, YaDiskError) as exc:
            self.processing_service._cleanup_temp_artifacts(temp_path)
            if extracted_path is not None and extracted_path != temp_path:
                self.processing_service._cleanup_temp_artifacts(extracted_path)
            raise AIAssistantError(str(exc)) from exc

    async def _build_all_context_blocks(
        self,
        locals_by_type: dict[SourceType, list[tuple[str, Path]]],
        question: str,
    ) -> list[_ContextBlock]:
        n = _count_blocks_plan(locals_by_type)
        budget = max(800, self.config.ai_assistant_max_context_chars // max(1, n))
        rows_budget = max(
            10, self.config.ai_assistant_max_context_rows // max(1, n)
        )
        blocks: list[_ContextBlock] = []

        for name, path in locals_by_type["no_move"]:
            try:
                df = await asyncio.to_thread(load_no_move_dataframe, path)
            except ValueError as exc:
                raise AIAssistantError(str(exc)) from exc
            payload = await asyncio.to_thread(
                self._build_context_payload,
                df,
                question,
                name,
                folder_path_for_source(self.config, "no_move"),
                max_chars=budget,
                max_rows=rows_budget,
            )
            blocks.append(
                _ContextBlock(
                    label=f"no_move:{name}",
                    text=f"### Без движения — {name}\n{payload.text}",
                    matched_rows=payload.matched_rows,
                    applied_filters=payload.applied_filters,
                )
            )

        for name, path in locals_by_type["h24"]:
            snap, meta = await asyncio.to_thread(
                load_24h_snapshot_sync,
                str(path),
                str(self.processing_service.block_ids_path),
            )
            text, shown = format_24h_snapshot_for_llm(
                snap,
                meta,
                max_rows=rows_budget,
                max_chars=budget,
            )
            blocks.append(
                _ContextBlock(
                    label=f"h24:{name}",
                    text=f"### 24 часа — {name}\n{text}",
                    matched_rows=shown,
                    applied_filters=(),
                )
            )

        wh = locals_by_type["warehouse_delay"]
        if wh:
            try:
                agg = await asyncio.to_thread(
                    try_warehouse_consolidated_or_aggregate,
                    [(n, str(p)) for n, p in wh],
                )
                names = ", ".join(n for n, _ in wh)
                text = format_warehouse_aggregation_for_llm(
                    agg,
                    title=f"### Задержка склада — {names}",
                    max_chars=budget * max(1, len(wh)),
                )
                blocks.append(
                    _ContextBlock(
                        label=f"warehouse_delay:{names}",
                        text=text,
                        matched_rows=agg.processed_files_count,
                        applied_filters=(),
                    )
                )
            except WarehouseDelayError:
                for name, path in wh:
                    try:
                        preview = await asyncio.to_thread(
                            warehouse_delay_rows_preview,
                            str(path),
                            max_rows=rows_budget,
                            max_chars=budget,
                        )
                    except WarehouseDelayError as exc:
                        preview = f"(не удалось разобрать файл: {exc})"
                    blocks.append(
                        _ContextBlock(
                            label=f"warehouse_delay:{name}",
                            text=f"### Задержка склада — {name}\n{preview}",
                            matched_rows=0,
                            applied_filters=(),
                        )
                    )

        return blocks

    def _build_messages_multi(
        self,
        *,
        question: str,
        history: Sequence[dict[str, str]],
        context_text: str,
    ) -> list[OllamaMessage]:
        messages = [
            OllamaMessage(
                role="system",
                content=(
                    "Ты AI-ассистент Telegram-бота по выгрузкам с Яндекс.Диска "
                    "(«Без движения», «24 часа», «Задержка склада»). "
                    "В сообщении пользователя может быть несколько разделов контекста. "
                    "Отвечай только по приведённым данным. Не выдумывай строки, ШК, суммы. "
                    "Если данных мало или они обрезаны — скажи об этом. "
                    "По-русски, кратко. Списки — маркированные."
                ),
            )
        ]
        for item in self.trim_history(history):
            role = item.get("role")
            content = (item.get("content") or "").strip()
            if role not in {"user", "assistant"} or not content:
                continue
            messages.append(OllamaMessage(role=role, content=content))
        messages.append(
            OllamaMessage(
                role="user",
                content=(
                    f"Вопрос пользователя:\n{question}\n\n"
                    f"Контекст из файлов:\n{context_text}"
                ),
            )
        )
        return messages

    def trim_history(self, history: Sequence[dict[str, str]]) -> list[dict[str, str]]:
        limit = max(0, self.config.ai_assistant_max_history_messages)
        if not limit:
            return []
        cleaned = [
            {"role": str(item.get("role", "")), "content": str(item.get("content", ""))}
            for item in history
            if item.get("role") in {"user", "assistant"} and (item.get("content") or "").strip()
        ]
        return cleaned[-limit:]

    def _build_context_payload(
        self,
        dataframe: pd.DataFrame,
        question: str,
        source_name: str,
        source_path: str,
        *,
        max_chars: int | None = None,
        max_rows: int | None = None,
    ) -> ContextPayload:
        if dataframe.empty:
            raise AIAssistantError("Файл пустой: в нём нет строк для анализа.")

        max_chars = max_chars or max(2000, self.config.ai_assistant_max_context_chars)
        max_rows = max_rows or max(10, self.config.ai_assistant_max_context_rows)

        item_name_column = self._detect_item_name_column(dataframe)
        working_df = pd.DataFrame(
            {
                "Гофра": dataframe["Гофра"].astype(str).str.strip(),
                "ШК": dataframe["ШК"].astype(str).str.strip(),
                "Стоимость": pd.to_numeric(dataframe["Стоимость"], errors="coerce").fillna(0),
            }
        )
        working_df["Наименование"] = (
            dataframe[item_name_column].fillna("").astype(str).str.strip()
            if item_name_column
            else ""
        )
        working_df = working_df[
            working_df["ШК"].ne("") | working_df["Наименование"].ne("") | working_df["Гофра"].ne("")
        ]
        working_df = working_df.sort_values("Стоимость", ascending=False).reset_index(drop=True)

        filters = self._extract_filters(question)
        filtered_df, applied_filters = self._apply_filters(working_df, filters)
        selected_df = filtered_df if applied_filters else working_df
        selected_df = selected_df.sort_values("Стоимость", ascending=False).reset_index(drop=True)

        summary_lines = self._build_summary_lines(
            working_df=working_df,
            filtered_df=filtered_df,
            applied_filters=applied_filters,
            source_name=source_name,
            source_path=source_path,
        )
        grouped_lines = self._build_group_summary_lines(working_df)

        reserved_text = "\n".join(summary_lines + [""] + grouped_lines + ["", "Релевантные строки:"])
        remaining_chars = max(800, max_chars - len(reserved_text))
        row_lines, row_truncated = self._build_row_lines(
            selected_df,
            max_rows=max_rows,
            max_chars=remaining_chars,
        )

        if row_truncated:
            row_lines.append("... список строк обрезан по лимиту контекста.")
        if not row_lines:
            row_lines.append("Совпадающих строк не найдено.")

        context_text = "\n".join(summary_lines + [""] + grouped_lines + ["", "Релевантные строки:"] + row_lines)
        return ContextPayload(
            text=context_text[:max_chars],
            matched_rows=int(len(filtered_df)),
            total_rows=int(len(working_df)),
            applied_filters=tuple(applied_filters),
        )

    def _build_summary_lines(
        self,
        *,
        working_df: pd.DataFrame,
        filtered_df: pd.DataFrame,
        applied_filters: list[str],
        source_name: str,
        source_path: str,
    ) -> list[str]:
        unique_gophra = int(working_df["Гофра"].nunique())
        rows_above_threshold = int((working_df["Стоимость"] > MIN_COST_THRESHOLD).sum())
        nonstandard_values = sorted(
            {value for value in working_df["Гофра"].tolist() if self._is_nonstandard_gophra(value)}
        )
        nonstandard_preview = ", ".join(nonstandard_values[:10]) if nonstandard_values else "нет"
        filter_line = ", ".join(applied_filters) if applied_filters else "нет явных фильтров"
        matched_rows = int(len(filtered_df))
        return [
            f"Источник: {source_name}",
            f"Путь на Я.Диске: {source_path}",
            f"Всего строк в файле: {len(working_df)}",
            f"Строк со стоимостью > {MIN_COST_THRESHOLD}: {rows_above_threshold}",
            f"Уникальных значений гофры: {unique_gophra}",
            f"Нестандартные гофры (до 10): {nonstandard_preview}",
            f"Применённые фильтры: {filter_line}",
            f"Совпавших строк по фильтрам: {matched_rows}",
        ]

    def _build_group_summary_lines(self, dataframe: pd.DataFrame) -> list[str]:
        grouped = (
            dataframe.groupby("Гофра", dropna=False)
            .agg(
                количество=("ШК", "count"),
                сумма_стоимости=("Стоимость", "sum"),
            )
            .sort_values("сумма_стоимости", ascending=False)
            .head(12)
            .reset_index()
        )
        lines = ["Сводка по гофрам (топ 12 по сумме стоимости):"]
        for row in grouped.to_dict("records"):
            lines.append(
                f"- Гофра {row['Гофра']}: строк={int(row['количество'])}, "
                f"сумма={self._format_amount(row['сумма_стоимости'])}"
            )
        return lines

    def _build_row_lines(
        self,
        dataframe: pd.DataFrame,
        *,
        max_rows: int,
        max_chars: int,
    ) -> tuple[list[str], bool]:
        lines: list[str] = []
        used_chars = 0
        for index, row in enumerate(dataframe.to_dict("records"), start=1):
            if index > max_rows:
                return lines, True
            item_name = str(row.get("Наименование") or "").strip()
            item_label = item_name if item_name else str(row.get("ШК") or "—").strip()
            line = (
                f"- Гофра {str(row.get('Гофра') or '—').strip()} | "
                f"ШК {str(row.get('ШК') or '—').strip()} | "
                f"Товар {item_label} | "
                f"Стоимость {self._format_amount(row.get('Стоимость'))}"
            )
            projected = used_chars + len(line) + 1
            if projected > max_chars:
                return lines, True
            lines.append(line)
            used_chars = projected
        return lines, False

    def _extract_filters(self, question: str) -> QueryFilters:
        normalized = question.lower().replace("ё", "е")
        min_cost: float | None = None
        min_cost_operator = ">"
        for pattern, operator in _MIN_COST_PATTERNS:
            match = pattern.search(normalized)
            if match:
                min_cost = float(match.group(1).replace(",", "."))
                min_cost_operator = operator
                break

        gophra_value = None
        for pattern in _GOPHRA_PATTERNS:
            match = pattern.search(normalized)
            if match:
                gophra_value = match.group(1).lstrip("0") or "0"
                break

        nonstandard_only = "нестандарт" in normalized or "не стандарт" in normalized
        return QueryFilters(
            gophra_value=gophra_value,
            min_cost=min_cost,
            min_cost_operator=min_cost_operator,
            nonstandard_only=nonstandard_only,
        )

    def _apply_filters(
        self, dataframe: pd.DataFrame, filters: QueryFilters
    ) -> tuple[pd.DataFrame, list[str]]:
        filtered = dataframe
        applied_filters: list[str] = []

        if filters.nonstandard_only:
            filtered = filtered[filtered["Гофра"].map(self._is_nonstandard_gophra)]
            applied_filters.append("только нестандартные гофры")

        if filters.gophra_value is not None:
            filtered = filtered[filtered["Гофра"] == filters.gophra_value]
            applied_filters.append(f"гофра = {filters.gophra_value}")

        if filters.min_cost is not None:
            if filters.min_cost_operator == ">=":
                filtered = filtered[filtered["Стоимость"] >= filters.min_cost]
            else:
                filtered = filtered[filtered["Стоимость"] > filters.min_cost]
            applied_filters.append(
                f"стоимость {filters.min_cost_operator} {self._format_amount(filters.min_cost)}"
            )

        return filtered, applied_filters

    def _detect_item_name_column(self, dataframe: pd.DataFrame) -> str | None:
        normalized_to_original = {
            self._normalize_column_name(column): str(column) for column in dataframe.columns
        }
        for alias in _ITEM_NAME_COLUMN_ALIASES:
            column_name = normalized_to_original.get(alias)
            if column_name:
                return column_name
        return None

    def _finalize_reply(
        self,
        answer_text: str,
        *,
        source_name: str,
        context_payload: ContextPayload,
        sources_used: tuple[str, ...] | None = None,
    ) -> str:
        parts = [f"Источники: {source_name}."]
        if context_payload.applied_filters:
            parts.append(f"Фильтры: {', '.join(context_payload.applied_filters)}.")
        if context_payload.matched_rows:
            parts.append(f"Совпавших строк (оценка): {context_payload.matched_rows}.")
        parts.append(answer_text.strip())
        result = "\n".join(parts)
        limit = max(500, self.config.ai_assistant_max_reply_chars)
        if len(result) <= limit:
            return result
        return result[: limit - 28].rstrip() + "\n\nОтвет обрезан по лимиту Telegram."

    @staticmethod
    def _is_nonstandard_gophra(value: object) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        return not any(text.startswith(prefix) for prefix in _NONSTANDARD_GOPHRA_PREFIXES)

    @staticmethod
    def _normalize_column_name(value: object) -> str:
        return " ".join(str(value or "").replace("\xa0", " ").strip().lower().replace("ё", "е").split())

    @staticmethod
    def _format_amount(value: object) -> str:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            return "0"
        integer_value = int(numeric)
        if float(numeric) == float(integer_value):
            return str(integer_value)
        return f"{float(numeric):.2f}"


def _dedupe_resolved(files: list[ResolvedYadiskFile]) -> list[ResolvedYadiskFile]:
    seen: set[str] = set()
    out: list[ResolvedYadiskFile] = []
    for item in files:
        if item.disk_path in seen:
            continue
        seen.add(item.disk_path)
        out.append(item)
    return out


def _count_blocks_plan(locals_by_type: dict[SourceType, list[tuple[str, Path]]]) -> int:
    n = len(locals_by_type["no_move"]) + len(locals_by_type["h24"])
    wh = locals_by_type["warehouse_delay"]
    if wh:
        n += 1
    return max(1, n)


@dataclass(frozen=True)
class _MergedContext:
    text: str
    matched_rows: int
    total_rows: int
    applied_filters: tuple[str, ...]


def _merge_context_blocks(blocks: list[_ContextBlock], *, max_chars: int) -> _MergedContext:
    if not blocks:
        return _MergedContext(
            text="(нет данных)",
            matched_rows=0,
            total_rows=0,
            applied_filters=(),
        )
    parts: list[str] = []
    total_matched = 0
    total_all = 0
    all_filters: list[str] = []
    used = 0
    for block in blocks:
        chunk = block.text.strip()
        if used + len(chunk) + 2 > max_chars:
            chunk = chunk[: max(0, max_chars - used - 30)].rstrip() + "\n…(раздел обрезан)"
        parts.append(chunk)
        used += len(chunk) + 2
        total_matched += block.matched_rows
        total_all += block.matched_rows
        all_filters.extend(block.applied_filters)
        if used >= max_chars:
            break
    return _MergedContext(
        text="\n\n".join(parts)[:max_chars],
        matched_rows=total_matched,
        total_rows=total_all,
        applied_filters=tuple(dict.fromkeys(all_filters)),
    )
