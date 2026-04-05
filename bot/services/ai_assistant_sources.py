from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence

from bot.config import Config
from bot.services.block_ids import load_block_ids
from bot.services.excel_24h import SnapshotMeta, process_24h_file
from bot.services.warehouse_delay import (
    BUCKET_LABELS,
    CANONICAL_ROW_ORDER,
    TOTAL_COLUMN,
    WarehouseDelayAggregationResult,
    WarehouseDelayError,
    aggregate_warehouse_delay_files,
    process_warehouse_delay_consolidated_file,
    read_warehouse_delay_file,
    resolve_columns,
)
from bot.services.yadisk import YaDiskError, yadisk_list_files

logger = logging.getLogger(__name__)

SourceType = Literal["no_move", "h24", "warehouse_delay"]
PickMode = Literal["latest", "by_name", "all_in_folder"]

SOURCE_LABELS: dict[SourceType, str] = {
    "no_move": "Без движения",
    "h24": "24 часа (прогноз списаний)",
    "warehouse_delay": "Задержка склада",
}

PLANNER_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "need_data": {"type": "boolean"},
        "answer_without_data": {"type": "string"},
        "sources": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "enum": ["no_move", "h24", "warehouse_delay"],
                    },
                    "pick": {"type": "string", "enum": ["latest", "by_name", "all_in_folder"]},
                    "filenames": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["source_type", "pick"],
            },
        },
    },
    "required": ["need_data"],
}


@dataclass(frozen=True)
class FolderCatalog:
    source_type: SourceType
    folder_key: str
    folder_path: str
    files: tuple[dict[str, Any], ...]

    def by_name(self, name: str) -> dict[str, Any] | None:
        for item in self.files:
            if item.get("name") == name:
                return item
        return None


@dataclass(frozen=True)
class PlannerSourceItem:
    source_type: SourceType
    pick: PickMode
    filenames: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlannerResult:
    need_data: bool
    answer_without_data: str | None
    sources: tuple[PlannerSourceItem, ...]


@dataclass(frozen=True)
class ResolvedYadiskFile:
    source_type: SourceType
    name: str
    disk_path: str


def configured_source_types(config: Config) -> list[SourceType]:
    out: list[SourceType] = []
    if (config.yandex_no_move_dir or "").strip():
        out.append("no_move")
    if (config.yandex_24h_dir or "").strip():
        out.append("h24")
    if (config.yandex_warehouse_delay_dir or "").strip():
        out.append("warehouse_delay")
    return out


def folder_path_for_source(config: Config, source_type: SourceType) -> str:
    if source_type == "no_move":
        return (config.yandex_no_move_dir or "").strip() or "disk:/BOT_UPLOADS/no_move/"
    if source_type == "h24":
        return (config.yandex_24h_dir or "").strip() or "disk:/BOT_UPLOADS/24h/"
    return (config.yandex_warehouse_delay_dir or "").strip() or "disk:/BOT_UPLOADS/warehouse_delay/"


def _sort_files_by_modified(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(item: dict[str, Any]) -> str:
        return str(item.get("modified") or "")

    return sorted(files, key=key, reverse=True)


async def build_folder_catalogs(
    *,
    config: Config,
    token: str,
    per_folder_limit: int,
) -> dict[SourceType, FolderCatalog]:
    exts = config.yandex_allowed_exts
    catalogs: dict[SourceType, FolderCatalog] = {}
    for source_type in configured_source_types(config):
        path = folder_path_for_source(config, source_type)
        try:
            raw = await yadisk_list_files(token, path, exts)
        except YaDiskError as exc:
            logger.info(
                "AI assistant catalog empty or error source=%s path=%s: %s",
                source_type,
                path,
                exc,
            )
            raw = []
        trimmed = _sort_files_by_modified(list(raw))[: max(1, per_folder_limit)]
        catalogs[source_type] = FolderCatalog(
            source_type=source_type,
            folder_key=source_type,
            folder_path=path,
            files=tuple(trimmed),
        )
    return catalogs


def format_catalog_for_planner(
    catalogs: Mapping[SourceType, FolderCatalog],
    *,
    max_chars: int,
) -> str:
    lines: list[str] = []
    for st in ("no_move", "h24", "warehouse_delay"):
        cat = catalogs.get(st)
        if not cat:
            continue
        label = SOURCE_LABELS[st]
        lines.append(f"## {label} (source_type={st})")
        lines.append(f"Папка: {cat.folder_path}")
        if not cat.files:
            lines.append("(нет файлов в каталоге)")
        else:
            for item in cat.files:
                name = item.get("name") or "?"
                modified = item.get("modified") or ""
                size = item.get("size")
                lines.append(f"- {name} | modified={modified} | size={size}")
        lines.append("")
    text = "\n".join(lines).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 20].rstrip() + "\n…(каталог обрезан)"


def planner_system_prompt(available: Sequence[SourceType]) -> str:
    types_line = ", ".join(SOURCE_LABELS[st] for st in available)
    return (
        "Ты планировщик запросов к данным на Яндекс.Диске. Доступные типы источников: "
        f"{types_line}.\n"
        "Ответь ТОЛЬКО одним JSON-объектом по схеме (без markdown):\n"
        '- need_data: true если для ответа пользователю нужны данные из Excel на диске, '
        "false если достаточно общего ответа (приветствие, уточнение без цифр, вопрос "
        "не про выгрузки).\n"
        "- answer_without_data: если need_data=false — готовый короткий ответ пользователю "
        "на русском; иначе null или пустая строка.\n"
        "- sources: массив заданий (только если need_data=true). Каждый элемент:\n"
        '  - source_type: "no_move" | "h24" | "warehouse_delay"\n'
        '  - pick: "latest" — самый свежий файл в папке; '
        '"by_name" — только перечисленные имена из каталога; '
        '"all_in_folder" — все файлы из папки задержки (только для warehouse_delay), '
        "до лимита сервера.\n"
        "  - filenames: массив имён файлов (только для pick=by_name).\n"
        "Можно несколько элементов, если нужно сравнить разные источники или несколько файлов.\n"
        "Если вопрос явно про один отчёт — выбери соответствующий source_type. "
        "«24 часа», прогноз списаний → h24. Задержка склада, МХ, простой → warehouse_delay. "
        "Гофра, ШК, без движения → no_move."
    )


def parse_planner_payload(data: Mapping[str, Any]) -> PlannerResult:
    need_data = bool(data.get("need_data"))
    answer = data.get("answer_without_data")
    answer_str = str(answer).strip() if answer is not None else ""
    raw_sources = data.get("sources") or []
    items: list[PlannerSourceItem] = []
    if isinstance(raw_sources, list):
        for raw in raw_sources:
            if not isinstance(raw, dict):
                continue
            st = raw.get("source_type")
            pick = raw.get("pick")
            if st not in ("no_move", "h24", "warehouse_delay"):
                continue
            if pick not in ("latest", "by_name", "all_in_folder"):
                continue
            names_raw = raw.get("filenames") or []
            names: list[str] = []
            if isinstance(names_raw, list):
                names = [str(n).strip() for n in names_raw if str(n).strip()]
            items.append(
                PlannerSourceItem(
                    source_type=st,
                    pick=pick,
                    filenames=tuple(names),
                )
            )
    return PlannerResult(
        need_data=need_data,
        answer_without_data=answer_str or None,
        sources=tuple(items),
    )


def validate_and_normalize_plan(
    plan: PlannerResult,
    *,
    available: Sequence[SourceType],
) -> PlannerResult:
    avail = set(available)
    cleaned: list[PlannerSourceItem] = []
    for item in plan.sources:
        if item.source_type not in avail:
            continue
        if item.pick == "all_in_folder" and item.source_type != "warehouse_delay":
            cleaned.append(
                PlannerSourceItem(
                    source_type=item.source_type,
                    pick="latest",
                    filenames=(),
                )
            )
            continue
        if item.pick == "by_name" and not item.filenames:
            cleaned.append(
                PlannerSourceItem(source_type=item.source_type, pick="latest", filenames=())
            )
            continue
        cleaned.append(item)
    return PlannerResult(
        need_data=plan.need_data,
        answer_without_data=plan.answer_without_data,
        sources=tuple(cleaned),
    )


def fallback_planner_from_question(
    question: str,
    *,
    available: Sequence[SourceType],
) -> PlannerResult:
    if not available:
        return PlannerResult(
            need_data=False,
            answer_without_data="Нет настроенных папок Я.Диска для выгрузок.",
            sources=(),
        )
    q = question.lower().replace("ё", "е")
    scores: dict[SourceType, int] = {st: 0 for st in available}
    if "без движен" in q or "гофр" in q or " тара " in q or "шк" in q or "стоимост" in q:
        scores["no_move"] = scores.get("no_move", 0) + 3
    if (
        "24" in q
        or "двадцат" in q
        or "прогноз" in q
        or "списан" in q
        or "24ч" in q
        or "24 ч" in q
    ):
        scores["h24"] = scores.get("h24", 0) + 3
    if "задержк" in q or "склад" in q or "простой" in q or "мх" in q or "не разложен" in q:
        scores["warehouse_delay"] = scores.get("warehouse_delay", 0) + 3
    best: SourceType | None = None
    best_score = 0
    for st, sc in scores.items():
        if sc > best_score:
            best_score = sc
            best = st
    if best is None or best_score == 0:
        best = available[0]
    return PlannerResult(
        need_data=True,
        answer_without_data=None,
        sources=(PlannerSourceItem(source_type=best, pick="latest", filenames=()),),
    )


def resolve_planner_to_files(
    plan: PlannerResult,
    catalogs: Mapping[SourceType, FolderCatalog],
    *,
    max_files: int,
) -> list[ResolvedYadiskFile]:
    if not plan.need_data:
        return []
    out: list[ResolvedYadiskFile] = []
    remaining = max_files

    for item in plan.sources:
        if remaining <= 0:
            break
        cat = catalogs.get(item.source_type)
        if not cat or not cat.files:
            continue

        if item.pick == "latest":
            first = cat.files[0]
            name = str(first.get("name") or "")
            path = str(first.get("path") or "")
            if name and path:
                out.append(ResolvedYadiskFile(item.source_type, name, path))
                remaining -= 1
            continue

        if item.pick == "by_name":
            for fn in item.filenames:
                if remaining <= 0:
                    break
                hit = cat.by_name(fn)
                if hit:
                    name = str(hit.get("name") or "")
                    path = str(hit.get("path") or "")
                    if name and path:
                        out.append(ResolvedYadiskFile(item.source_type, name, path))
                        remaining -= 1
            continue

        if item.pick == "all_in_folder" and item.source_type == "warehouse_delay":
            for f in cat.files:
                if remaining <= 0:
                    break
                name = str(f.get("name") or "")
                path = str(f.get("path") or "")
                if name and path:
                    out.append(ResolvedYadiskFile(item.source_type, name, path))
                    remaining -= 1

    return out


def format_24h_snapshot_for_llm(
    snapshot: dict[str, dict[str, Any]],
    meta: SnapshotMeta,
    *,
    max_rows: int,
    max_chars: int,
) -> tuple[str, int]:
    lines = [
        f"Файл 24ч: {meta.source_filename}",
        f"Строк в исходнике: {meta.rows_total}, валидных позиций: {meta.rows_valid}, "
        f"отброшено: missing={meta.dropped_missing}, block={meta.dropped_block}, "
        f"forecast={meta.dropped_forecast}.",
        "",
        "Позиции (product_id → стоимость, прогноз ISO, тара):",
    ]
    rows: list[tuple[str, float, str, str]] = []
    for pid, data in snapshot.items():
        cost = float(data.get("cost") or 0)
        fc = str(data.get("forecast") or "")
        tare = str(data.get("tare_id") or "")
        rows.append((pid, cost, fc, tare))
    rows.sort(key=lambda x: x[1], reverse=True)
    used = len("\n".join(lines))
    count = 0
    for pid, cost, fc, tare in rows:
        if count >= max_rows:
            lines.append("... (остальные строки опущены)")
            break
        line = f"- {pid} | cost={cost:.2f} | forecast={fc} | tare={tare}"
        if used + len(line) + 1 > max_chars:
            lines.append("... (контекст обрезан по лимиту символов)")
            break
        lines.append(line)
        used += len(line) + 1
        count += 1
    text = "\n".join(lines)
    return text[:max_chars], count


def format_warehouse_aggregation_for_llm(
    aggregation: WarehouseDelayAggregationResult,
    *,
    title: str,
    max_chars: int,
) -> str:
    lines = [title, "", "Матрица по складам (строка = блок/склад, столбцы = корзины часов):"]
    header = "Склад/" + " | ".join(BUCKET_LABELS) + f" | {TOTAL_COLUMN}"
    lines.append(header)
    for row_name in CANONICAL_ROW_ORDER:
        stats = aggregation.all_rows[row_name]
        cells = [str(stats[c]) for c in BUCKET_LABELS] + [str(stats[TOTAL_COLUMN])]
        lines.append(f"{row_name} | " + " | ".join(cells))
    lines.append("")
    lines.append("Топ без задания (тара, время, МХ, кол-во):")
    for item in aggregation.top_without_assignment[:15]:
        lines.append(
            f"- {item.tare} | {item.delay_display} | {item.mx_processing} | {item.unplaced_quantity}"
        )
    lines.append("")
    lines.append(
        f"Обработано файлов: {aggregation.processed_files_count}, пропущено: {aggregation.skipped_files_count}."
    )
    if aggregation.skipped_files:
        lines.append("Пропущенные: " + ", ".join(aggregation.skipped_files[:20]))
    text = "\n".join(lines)
    return text[:max_chars]


def warehouse_delay_rows_preview(
    file_path: str,
    *,
    max_rows: int,
    max_chars: int,
) -> str:
    df = read_warehouse_delay_file(file_path)
    mapping = resolve_columns(df)
    cols = [mapping.hours, mapping.mx_processing]
    if mapping.warehouse:
        cols.append(mapping.warehouse)
    if mapping.tare:
        cols.append(mapping.tare)
    if mapping.unplaced_quantity:
        cols.append(mapping.unplaced_quantity)
    lines = ["Построчный фрагмент (задержка склада):", ""]
    used = len("\n".join(lines))
    shown = 0
    for _, row in df.iterrows():
        if shown >= max_rows:
            lines.append("... (строки обрезаны)")
            break
        parts = [str(row[c]) for c in cols if c in df.columns]
        line = " | ".join(parts)
        if used + len(line) + 1 > max_chars:
            lines.append("... (контекст обрезан)")
            break
        lines.append(line)
        used += len(line) + 1
        shown += 1
    return "\n".join(lines)[:max_chars]


def load_24h_snapshot_sync(path: str, block_ids_path: str) -> tuple[dict[str, Any], SnapshotMeta]:
    block_ids = load_block_ids(block_ids_path)
    return process_24h_file(path, block_ids)


def try_warehouse_consolidated_or_aggregate(
    paths: list[tuple[str, str]],
) -> WarehouseDelayAggregationResult:
    if len(paths) == 1:
        filename, local_path = paths[0]
        try:
            return process_warehouse_delay_consolidated_file(local_path, filename)
        except WarehouseDelayError:
            return aggregate_warehouse_delay_files([(filename, local_path)])
    return aggregate_warehouse_delay_files(paths)


_JSON_OBJECT_PATTERN = re.compile(r"\{[\s\S]*\}")


def parse_json_lenient(text: str) -> dict[str, Any] | None:
    text = text.strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass
    match = _JSON_OBJECT_PATTERN.search(text)
    if match:
        try:
            data = json.loads(match.group(0))
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            return None
    return None
