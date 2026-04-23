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


def process_file(
    file_path: str | Path, export_mode: str
) -> Tuple[List[List], Dict, Dict]:
    df = pd.read_excel(file_path, engine="openpyxl")
    _validate_columns(df)

    df["Стоимость"] = pd.to_numeric(df["Стоимость"], errors="coerce").fillna(0)
    df["ШК"] = df["ШК"].astype(str)
    df["Гофра"] = (
        df["Гофра"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    )

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

    transfers_mask = grouped["Гофра"].str.startswith(("3", "4", "5"))
    if export_mode == EXPORT_WITHOUT_TRANSFERS:
        grouped = grouped[~transfers_mask]
    elif export_mode == EXPORT_ONLY_TRANSFERS:
        grouped = grouped[transfers_mask]

    product_ids_set = set()
    formatted_rows = []
    id_to_tary = {}
    for _, row in grouped.iterrows():
        g = row["Гофра"]
        ids = str(row["ШК"]).split("\n")
        formatted_rows.append([g, row["ШК"], row["Количество ШК"], row["Стоимость"]])
        for pid in ids:
            pid_clean = str(pid).strip()
            if not pid_clean:
                continue
            if pid_clean in id_to_tary and id_to_tary[pid_clean] != g:
                # keep first; conflict logged later by caller
                continue
            id_to_tary[pid_clean] = g
            product_ids_set.add(pid_clean)

    unknown_values = (
        grouped.loc[
            ~grouped["Гофра"].str.startswith(("3", "4", "5", "7", "9", "10")), "Гофра"
        ]
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
        "product_ids": product_ids_set,
        "id_to_tary": id_to_tary,
    }

    return formatted_rows, unknown_summary, stats
