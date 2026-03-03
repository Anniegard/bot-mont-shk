from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class SnapshotMeta:
    uploaded_at: str
    source_filename: str
    rows_total: int
    rows_after_filter: int
    rows_valid: int
    dropped_missing: int
    dropped_forecast: int
    dropped_block: int


def _normalize_column_name(name: str) -> str:
    return name.strip().lower()


def _find_column(columns: Iterable[str], keywords: Iterable[str]) -> str | None:
    for col in columns:
        normalized = _normalize_column_name(col)
        for kw in keywords:
            if kw in normalized:
                return col
    return None


def _parse_cost(value: str | float | int) -> float:
    text = "" if pd.isna(value) else str(value)
    text = text.replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


def _parse_forecast(value: str) -> datetime | None:
    if pd.isna(value):
        return None
    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def process_24h_file(file_path: str | Path, block_ids: set[str]) -> Tuple[Dict[str, Dict], SnapshotMeta]:
    df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
    df.columns = [col.strip() for col in df.columns]

    columns = df.columns.tolist()
    block_col = _find_column(columns, ["id блока", "блока"])
    product_col = _find_column(columns, ["идентификатор товара", "шк", "sku"])
    forecast_col = _find_column(columns, ["прогноз", "начнет", "спис"])
    cost_col = _find_column(columns, ["стоим"])
    tare_col = _find_column(columns, ["гофра", "тара", "id тары"])

    missing = []
    if not block_col:
        missing.append("ID Блока")
    if not product_col:
        missing.append("Идентификатор товара")
    if not forecast_col:
        missing.append("Прогноз")
    if not cost_col:
        missing.append("Стоимость")
    if missing:
        raise ValueError(f"Не найдены обязательные колонки 24ч: {', '.join(missing)}")

    subset_cols = [block_col, product_col, forecast_col, cost_col] + ([tare_col] if tare_col else [])
    df = df[subset_cols]
    rename_map = {
        block_col: "block_id",
        product_col: "product_id",
        forecast_col: "forecast",
        cost_col: "cost",
    }
    if tare_col:
        rename_map[tare_col] = "tare_id"
    df = df.rename(columns=rename_map)

    rows_total = len(df)

    df["block_id"] = df["block_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["forecast"] = df["forecast"].astype(str).str.strip()
    df["block_id"] = df["block_id"].replace({"nan": "", "NaN": ""})
    df["product_id"] = df["product_id"].replace({"nan": "", "NaN": ""})
    df["forecast"] = df["forecast"].replace({"nan": "", "NaN": ""})

    missing_mask = df[["block_id", "product_id", "forecast"]].isna().any(axis=1) | (
        (df["block_id"] == "") | (df["product_id"] == "") | (df["forecast"] == "")
    )
    dropped_missing = int(missing_mask.sum())
    df = df[~missing_mask]

    dropped_block = 0
    dropped_forecast = 0

    snapshot: Dict[str, Dict] = {}

    for _, row in df.iterrows():
        block_id = row["block_id"]
        if block_ids and block_id not in block_ids:
            dropped_block += 1
            continue

        forecast_dt = _parse_forecast(row["forecast"])
        if not forecast_dt:
            dropped_forecast += 1
            continue

        product_id = row["product_id"]
        tare_id = str(row["tare_id"]).strip() if "tare_id" in row and not pd.isna(row["tare_id"]) else ""
        cost_value = _parse_cost(row["cost"])

        existing = snapshot.get(product_id)
        if existing:
            existing["cost"] += cost_value
            if forecast_dt.isoformat() < existing["forecast"]:
                existing["forecast"] = forecast_dt.isoformat()
            if not existing.get("tare_id") and tare_id:
                existing["tare_id"] = tare_id
        else:
            snapshot[product_id] = {
                "cost": cost_value,
                "forecast": forecast_dt.isoformat(),
                "tare_id": tare_id,
            }

    rows_after_filter = max(len(df) - dropped_block - dropped_forecast, 0)
    rows_valid = len(snapshot)

    meta = SnapshotMeta(
        uploaded_at=datetime.now().isoformat(timespec="seconds"),
        source_filename=Path(file_path).name,
        rows_total=rows_total,
        rows_after_filter=rows_after_filter,
        rows_valid=rows_valid,
        dropped_missing=dropped_missing,
        dropped_forecast=dropped_forecast,
        dropped_block=dropped_block,
    )

    return snapshot, meta


def save_snapshot(snapshot: Dict[str, Dict], meta: SnapshotMeta, snapshot_path: Path, meta_path: Path) -> None:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(meta), f, ensure_ascii=False, indent=2)


def load_snapshot(snapshot_path: Path, meta_path: Path) -> Tuple[Dict[str, Dict] | None, Dict | None]:
    if not snapshot_path.exists() or not meta_path.exists():
        return None, None
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return snapshot, meta


def build_24h_table(snapshot: Dict[str, Dict], id_to_tary: Dict[str, str]) -> List[List]:
    # Filter to identifiers present in mapping
    filtered = {pid: data for pid, data in snapshot.items() if pid in id_to_tary}

    groups: Dict[str, List[Tuple[str, Dict]]] = {}
    warnings = 0
    for pid, data in filtered.items():
        tare_id = id_to_tary.get(pid)
        if tare_id is None:
            continue
        groups.setdefault(tare_id, []).append((pid, data))

    rows: List[List] = []
    for tare_id, items in groups.items():
        ids = []
        cost_sum = 0
        forecasts = []
        for pid, data in items:
            ids.append(pid)
            cost_sum += data.get("cost", 0)
            try:
                forecasts.append(datetime.fromisoformat(data.get("forecast")))
            except Exception:
                warnings += 1
                continue
        if not forecasts:
            continue
        min_forecast = min(forecasts)
        rows.append(
            [
                tare_id,
                "\n".join(ids),
                len(ids),
                cost_sum,
                min_forecast.strftime("%d.%m.%Y %H:%M"),
            ]
        )

    rows.sort(key=lambda r: datetime.strptime(r[4], "%d.%m.%Y %H:%M") if r[4] else datetime.max)
    return rows
