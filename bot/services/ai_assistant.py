from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from bot.config import Config
from bot.services.excel import MIN_COST_THRESHOLD, load_no_move_dataframe
from bot.services.file_sources import maybe_extract_zip
from bot.services.ollama_client import OllamaClient, OllamaError, OllamaMessage
from bot.services.processing import ProcessingService
from bot.services.yadisk import YaDiskError, yadisk_download_file, yadisk_list_latest

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
        if not (self.config.yandex_no_move_dir or "").strip():
            raise AIAssistantError("YANDEX_NO_MOVE_DIR не настроен.")

    async def answer_question(
        self,
        *,
        question: str,
        history: Sequence[dict[str, str]] | None = None,
    ) -> AIAssistantResponse:
        normalized_question = question.strip()
        if not normalized_question:
            raise AIAssistantError("Введите вопрос для AI-ассистента.")
        self.ensure_configured()

        async with self.processing_service.processing_slot():
            latest = await yadisk_list_latest(
                self.config.yandex_oauth_token or "",
                self.config.yandex_no_move_dir or "disk:/BOT_UPLOADS/no_move/",
                self.config.yandex_allowed_exts,
            )
            dataframe = await self._download_latest_no_move_dataframe(latest)

        context_payload = await asyncio.to_thread(
            self._build_context_payload,
            dataframe,
            normalized_question,
            latest.get("name") or "no_move.xlsx",
            latest.get("path") or "",
        )

        messages = self._build_messages(
            question=normalized_question,
            history=history or (),
            context_payload=context_payload,
        )
        answer_text = await self.ollama_client.chat(messages)
        return AIAssistantResponse(
            text=self._finalize_reply(
                answer_text,
                source_name=latest.get("name") or "no_move.xlsx",
                context_payload=context_payload,
            ),
            source_name=latest.get("name") or "no_move.xlsx",
            source_path=latest.get("path") or "",
            matched_rows=context_payload.matched_rows,
            applied_filters=context_payload.applied_filters,
        )

    async def _download_latest_no_move_dataframe(self, latest: dict[str, Any]) -> pd.DataFrame:
        suffix = Path(latest.get("name") or "source.xlsx").suffix or ".xlsx"
        temp_path = self.processing_service.make_temp_path("ai_no_move", suffix)
        extracted_path: Path | None = None
        try:
            download_info = await yadisk_download_file(
                self.config.yandex_oauth_token or "",
                latest["path"],
                str(temp_path),
                max_bytes=self.config.yandex_max_mb * 1024 * 1024,
            )
            prepared_path = await asyncio.to_thread(
                maybe_extract_zip,
                download_info["path"],
                str(Path(download_info["path"]).parent),
            )
            extracted_path = Path(prepared_path)
            return await asyncio.to_thread(load_no_move_dataframe, extracted_path)
        except ValueError as exc:
            raise AIAssistantError(str(exc)) from exc
        finally:
            self.processing_service._cleanup_temp_artifacts(temp_path)
            if extracted_path is not None and extracted_path != temp_path:
                self.processing_service._cleanup_temp_artifacts(extracted_path)

    def _build_messages(
        self,
        *,
        question: str,
        history: Sequence[dict[str, str]],
        context_payload: ContextPayload,
    ) -> list[OllamaMessage]:
        messages = [
            OllamaMessage(
                role="system",
                content=(
                    "Ты AI-ассистент Telegram-бота по Excel-файлу 'Без движения'. "
                    "Отвечай только по предоставленному контексту файла. "
                    "Не выдумывай строки, ШК, гофры, стоимости или наименования. "
                    "Если данных недостаточно или они обрезаны, прямо скажи об этом. "
                    "Отвечай по-русски, кратко и по делу. "
                    "Если пользователь просит список, используй маркированный список."
                ),
            )
        ]
        trimmed_history = self.trim_history(history)
        for item in trimmed_history:
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
                    f"Контекст файла:\n{context_payload.text}"
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
    ) -> ContextPayload:
        if dataframe.empty:
            raise AIAssistantError("Файл пустой: в нём нет строк для анализа.")

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

        max_chars = max(2000, self.config.ai_assistant_max_context_chars)
        max_rows = max(10, self.config.ai_assistant_max_context_rows)
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
    ) -> str:
        parts = [f"Файл: {source_name}."]
        if context_payload.applied_filters:
            parts.append(f"Фильтры: {', '.join(context_payload.applied_filters)}.")
        if context_payload.matched_rows:
            parts.append(f"Совпавших строк: {context_payload.matched_rows}.")
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
