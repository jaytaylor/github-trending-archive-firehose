from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY_DIR = ROOT / "py"
if PY_DIR.is_dir():
    path = str(PY_DIR)
    if path not in sys.path:
        sys.path.insert(0, path)
