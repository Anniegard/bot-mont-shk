from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple


def save_no_move_map(mapping: Dict[str, str], meta: Dict, base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    with (base_dir / "last_no_move_map.json").open("w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    with (base_dir / "last_no_move_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def load_no_move_map(base_dir: Path) -> Tuple[Optional[Dict[str, str]], Optional[Dict]]:
    map_path = base_dir / "last_no_move_map.json"
    meta_path = base_dir / "last_no_move_meta.json"
    if not map_path.exists() or not meta_path.exists():
        return None, None
    mapping = json.loads(map_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return mapping, meta
