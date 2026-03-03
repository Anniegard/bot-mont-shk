from __future__ import annotations

from pathlib import Path
from typing import Set


def load_block_ids(path: str | Path) -> Set[str]:
    """Load whitelist of block IDs; ignores empty lines and comments starting with '#'."""
    path_obj = Path(path)
    if not path_obj.exists():
        return set()

    items = set()
    for line in path_obj.read_text(encoding="utf-8").splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        items.add(cleaned)
    return items
