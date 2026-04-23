from __future__ import annotations

import json
import subprocess
import sys


def test_process_file_includes_prefix_5_in_only_transfers_mode(tmp_path) -> None:
    script = """
import json
from pathlib import Path
import pandas as pd
from bot.services.excel import process_file, EXPORT_ONLY_TRANSFERS

p = Path(r'{path}')
pd.DataFrame({{
    'Гофра': ['51234', '39876', '42345', '71234'],
    'ШК': ['A', 'B', 'C', 'D'],
    'Стоимость': [3000, 3500, 4000, 4500],
}}).to_excel(p, index=False, engine='openpyxl')
rows, unknown, _ = process_file(p, EXPORT_ONLY_TRANSFERS)
print(json.dumps({{'rows': [r[0] for r in rows], 'unknown': unknown['values']}}, ensure_ascii=False))
""".format(path=str(tmp_path / "source.xlsx").replace("\\", "\\\\"))
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout.strip())
    assert "51234" in payload["rows"]
    assert "51234" not in payload["unknown"]


def test_process_file_excludes_prefix_5_in_without_transfers_mode(tmp_path) -> None:
    script = """
import json
from pathlib import Path
import pandas as pd
from bot.services.excel import process_file, EXPORT_WITHOUT_TRANSFERS

p = Path(r'{path}')
pd.DataFrame({{
    'Гофра': ['51234', '81234', '39876'],
    'ШК': ['A', 'B', 'C'],
    'Стоимость': [3000, 3200, 3500],
}}).to_excel(p, index=False, engine='openpyxl')
rows, unknown, _ = process_file(p, EXPORT_WITHOUT_TRANSFERS)
print(json.dumps({{'rows': [r[0] for r in rows], 'unknown': unknown['values']}}, ensure_ascii=False))
""".format(path=str(tmp_path / "source_without.xlsx").replace("\\", "\\\\"))
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout.strip())
    assert "51234" not in payload["rows"]
    assert "39876" not in payload["rows"]
    assert "81234" in payload["rows"]
    assert "51234" not in payload["unknown"]
