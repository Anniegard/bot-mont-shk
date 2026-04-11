from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bot.config import Config
from bot.services.ai.types import (
    AI24hSnapshotContent,
    AIContextBlock,
    AIContextPackage,
    AIExtractionResult,
    AIWarehouseContent,
)
from bot.services.warehouse_delay import BUCKET_LABELS, TOTAL_COLUMN

_ITEM_NAME_ALIASES = {
    "наименование",
    "наименование товара",
    "товар",
    "номенклатура",
}


@dataclass(frozen=True)
class _QuestionHints:
    numeric_tokens: tuple[str, ...]
    lowered: str


class AIContextBuilder:
    def __init__(self, config: Config) -> None:
        self.config = config

    def build_context(self, extractions: list[AIExtractionResult], question: str) -> AIContextPackage:
        hints = self._question_hints(question)
        blocks = [self._build_block(item, hints) for item in extractions]

        parts: list[str] = []
        truncation_count = 0
        rows_scanned = 0
        rows_selected = 0
        source_types: list[str] = []
        used_chars = 0

        for block in blocks:
            text = block.text.strip()
            remaining = self.config.ai_max_context_chars - used_chars
            if remaining <= 0:
                truncation_count += 1
                break
            if len(text) > remaining:
                text = text[: max(0, remaining - 16)].rstrip() + "\n...(обрезано)"
                truncation_count += 1
            parts.append(text)
            used_chars += len(text) + 2
            rows_scanned += block.rows_scanned
            rows_selected += block.rows_selected
            source_types.append(block.title)
            if block.was_truncated:
                truncation_count += 1

        return AIContextPackage(
            text="\n\n".join(parts),
            blocks=tuple(blocks),
            total_rows_scanned=rows_scanned,
            total_rows_selected=rows_selected,
            truncation_count=truncation_count,
            source_types=tuple(source_types),
        )

    def _build_block(self, extraction: AIExtractionResult, hints: _QuestionHints) -> AIContextBlock:
        if extraction.extracted_kind in {"table", "no_move"}:
            return self._build_table_block(extraction, hints)
        if extraction.extracted_kind == "h24":
            return self._build_h24_block(extraction)
        return self._build_warehouse_block(extraction)

    def _build_table_block(
        self,
        extraction: AIExtractionResult,
        hints: _QuestionHints,
    ) -> AIContextBlock:
        dataframe = extraction.content
        assert isinstance(dataframe, pd.DataFrame)
        working = dataframe.copy()
        working.columns = [str(column).strip() for column in working.columns]
        total_rows = len(working)
        lines = [
            f"### {extraction.display_name}",
            f"Тип источника: {extraction.extracted_kind}.",
            f"Колонок: {len(working.columns)}. Строк просмотрено: {total_rows}.",
        ]
        if extraction.notes:
            lines.extend(extraction.notes)

        selected = self._select_rows(working, hints)
        item_name_column = self._detect_item_name_column(working)
        was_truncated = len(selected) > self.config.ai_max_rows_per_source
        selected = selected.head(self.config.ai_max_rows_per_source)

        if extraction.extracted_kind == "no_move":
            lines.extend(self._build_no_move_summary(working))
        else:
            preview_columns = ", ".join(str(column) for column in working.columns[:8])
            lines.append(f"Колонки: {preview_columns}.")

        if selected.empty:
            lines.append("Совпадающих или релевантных строк не найдено.")
        else:
            lines.append("Релевантные строки:")
            for _, row in selected.iterrows():
                gophra = self._value(row.get("Гофра"))
                shk = self._value(row.get("ШК"))
                cost = self._value(row.get("Стоимость"))
                item_name = self._value(row.get(item_name_column)) if item_name_column else "—"
                if extraction.extracted_kind == "no_move":
                    lines.append(
                        f"- Гофра {gophra} | ШК {shk} | Товар {item_name} | Стоимость {cost}"
                    )
                else:
                    row_preview = " | ".join(
                        f"{column}={self._value(row.get(column))}"
                        for column in working.columns[:5]
                    )
                    lines.append(f"- {row_preview}")

        if was_truncated or extraction.was_truncated:
            lines.append("Часть строк была обрезана по лимиту AI-контекста.")

        return AIContextBlock(
            title=extraction.display_name,
            text="\n".join(lines),
            rows_scanned=extraction.rows_scanned,
            rows_selected=len(selected),
            notes=extraction.notes,
            was_truncated=was_truncated or extraction.was_truncated,
        )

    def _build_no_move_summary(self, dataframe: pd.DataFrame) -> list[str]:
        columns = {str(column).strip(): column for column in dataframe.columns}
        if not {"Гофра", "ШК", "Стоимость"}.issubset(columns):
            return []
        working = pd.DataFrame(
            {
                "Гофра": dataframe[columns["Гофра"]].astype(str).str.strip(),
                "ШК": dataframe[columns["ШК"]].astype(str).str.strip(),
                "Стоимость": pd.to_numeric(dataframe[columns["Стоимость"]], errors="coerce").fillna(0),
            }
        )
        top_groups = (
            working.groupby("Гофра", dropna=False)
            .agg(rows=("ШК", "count"), total_cost=("Стоимость", "sum"))
            .sort_values("total_cost", ascending=False)
            .head(5)
            .reset_index()
        )
        lines = [f"Строк со стоимостью > 2000: {int((working['Стоимость'] > 2000).sum())}."]
        for row in top_groups.to_dict("records"):
            lines.append(
                f"- Гофра {self._value(row['Гофра'])}: строк={int(row['rows'])}, сумма={self._value(row['total_cost'])}"
            )
        return lines

    def _build_h24_block(self, extraction: AIExtractionResult) -> AIContextBlock:
        content = extraction.content
        assert isinstance(content, AI24hSnapshotContent)
        snapshot = content.snapshot
        meta = content.meta
        rows = sorted(
            (
                (
                    product_id,
                    row.get("forecast") or "",
                    row.get("cost") or 0,
                    row.get("tare_id") or "",
                )
                for product_id, row in snapshot.items()
            ),
            key=lambda item: str(item[1]),
        )
        selected_rows = rows[: self.config.ai_max_rows_per_source]
        lines = [
            f"### {extraction.display_name}",
            "Тип источника: h24.",
            f"Источник файла: {meta.source_filename}.",
            f"Строк в исходнике: {meta.rows_total}. После фильтров: {meta.rows_after_filter}. Уникальных товаров: {meta.rows_valid}.",
        ]
        if extraction.notes:
            lines.extend(extraction.notes)
        lines.append("Ближайшие позиции по прогнозу:")
        for product_id, forecast, cost, tare_id in selected_rows:
            lines.append(
                f"- Товар {product_id} | forecast={forecast} | cost={self._value(cost)} | tare={self._value(tare_id)}"
            )
        if len(rows) > len(selected_rows):
            lines.append("Часть позиций 24ч была обрезана по лимиту AI-контекста.")
        return AIContextBlock(
            title=extraction.display_name,
            text="\n".join(lines),
            rows_scanned=extraction.rows_scanned,
            rows_selected=len(selected_rows),
            notes=extraction.notes,
            was_truncated=len(rows) > len(selected_rows),
        )

    def _build_warehouse_block(self, extraction: AIExtractionResult) -> AIContextBlock:
        content = extraction.content
        assert isinstance(content, AIWarehouseContent)
        aggregation = content.aggregation
        lines = [
            f"### {extraction.display_name}",
            "Тип источника: warehouse_delay.",
            f"Обработано файлов: {aggregation.processed_files_count}. Пропущено: {aggregation.skipped_files_count}.",
        ]
        if extraction.notes:
            lines.extend(extraction.notes)
        lines.append("Сводка по складам:")
        for row_name, stats in list(aggregation.all_rows.items())[:8]:
            total = stats[TOTAL_COLUMN]
            if not total:
                continue
            bucket_preview = ", ".join(
                f"{bucket}={stats[bucket]}"
                for bucket in BUCKET_LABELS[:4]
                if stats[bucket]
            )
            lines.append(f"- {row_name}: total={total}; {bucket_preview or 'без активных корзин'}")
        if aggregation.top_without_assignment:
            lines.append("Топ без задания:")
            for item in aggregation.top_without_assignment[:10]:
                lines.append(
                    f"- Тара {item.tare} | простой={item.delay_display} | МХ={item.mx_processing} | qty={item.unplaced_quantity}"
                )
        if aggregation.skipped_files:
            lines.append(
                "Пропущенные файлы: " + ", ".join(aggregation.skipped_files[:5])
            )
        return AIContextBlock(
            title=extraction.display_name,
            text="\n".join(lines),
            rows_scanned=extraction.rows_scanned,
            rows_selected=min(len(aggregation.processed_files), self.config.ai_max_rows_per_source),
            notes=extraction.notes,
            was_truncated=False,
        )

    def _question_hints(self, question: str) -> _QuestionHints:
        tokens = tuple(
            token
            for token in question.replace(",", " ").replace(";", " ").split()
            if any(char.isdigit() for char in token)
        )
        return _QuestionHints(numeric_tokens=tokens, lowered=question.lower().replace("ё", "е"))

    def _select_rows(self, dataframe: pd.DataFrame, hints: _QuestionHints) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe
        selected = dataframe.copy()
        if hints.numeric_tokens:
            text_frame = dataframe.astype(str)
            mask = pd.Series(False, index=dataframe.index)
            for token in hints.numeric_tokens:
                mask = mask | text_frame.apply(
                    lambda column: column.str.contains(token, case=False, na=False, regex=False)
                ).any(axis=1)
            filtered = dataframe[mask]
            if not filtered.empty:
                selected = filtered
        if "стоим" in hints.lowered and "Стоимость" in dataframe.columns:
            selected = selected.sort_values("Стоимость", ascending=False, na_position="last")
        return selected

    def _detect_item_name_column(self, dataframe: pd.DataFrame) -> str | None:
        normalized = {
            " ".join(str(column).strip().lower().replace("ё", "е").split()): str(column)
            for column in dataframe.columns
        }
        for alias in _ITEM_NAME_ALIASES:
            if alias in normalized:
                return normalized[alias]
        return None

    @staticmethod
    def _value(value: object) -> str:
        text = str(value).strip() if value is not None else ""
        return text or "—"
