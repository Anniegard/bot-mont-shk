from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

EXPORT_WITHOUT_TRANSFERS = "export_without_transfers"
EXPORT_WITH_TRANSFERS = "export_with_transfers"
EXPORT_ONLY_TRANSFERS = "export_only_transfers"

REQUIRED_COLUMNS = {"Гофра", "ШК", "Стоимость"}
MIN_COST_THRESHOLD = 2000


def _validate_columns(df: pd.DataFrame) -> None:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"В файле нет обязательных колонок: {missing}")


def process_file(file_path: str | Path, export_mode: str) -> Tuple[List[List], Dict, Dict]:
    df = pd.read_excel(file_path, engine="openpyxl")
    _validate_columns(df)

    df["Стоимость"] = pd.to_numeric(df["Стоимость"], errors="coerce").fillna(0)
    df["ШК"] = df["ШК"].astype(str)
    df["Гофра"] = df["Гофра"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    grouped = (
        df.groupby("Гофра", as_index=False)
        .agg(
            **{
                "ШК": ("ШК", lambda s: "\n".join(s.tolist())),
                "Количество ШК": ("ШК", "count"),
                "Стоимость": ("Стоимость", "sum"),
            }
        )
        .sort_values("Стоимость", ascending=False)
    )

    grouped = grouped[grouped["Стоимость"] > MIN_COST_THRESHOLD]

    transfers_mask = grouped["Гофра"].str.startswith(("3", "4"))
    if export_mode == EXPORT_WITHOUT_TRANSFERS:
        grouped = grouped[~transfers_mask]
    elif export_mode == EXPORT_ONLY_TRANSFERS:
        grouped = grouped[transfers_mask]

    formatted_rows = [
        [row["Гофра"], row["ШК"], row["Количество ШК"], row["Стоимость"]]
        for _, row in grouped.iterrows()
    ]

    unknown_values = (
        grouped.loc[~grouped["Гофра"].str.startswith(("3", "4", "7", "9", "10")), "Гофра"]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    unknown_summary = {"count": len(unknown_values), "values": unknown_values}

    stats = {
        "source_rows": len(df),
        "groups_total": len(grouped),
        "rows_after_filter": len(formatted_rows),
    }

    return formatted_rows, unknown_summary, stats
