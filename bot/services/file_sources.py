from __future__ import annotations

import shutil
import zipfile
from pathlib import Path


def maybe_extract_zip(path: str, workdir: str) -> str:
    p = Path(path)
    if p.suffix.lower() != ".zip":
        return str(p)

    extract_dir = Path(workdir) / (p.stem + "_unzipped")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(p, "r") as zf:
        excel_members = [
            member
            for member in zf.infolist()
            if not member.is_dir()
            and member.filename.lower().endswith((".xlsx", ".xls"))
        ]
        if not excel_members:
            raise ValueError("В архиве нет Excel файлов.")
        target_member = excel_members[0]
        extracted_path = extract_dir / Path(target_member.filename).name
        with zf.open(target_member) as source, extracted_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        return str(extracted_path)
