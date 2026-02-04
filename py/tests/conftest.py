from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PY_DIR = ROOT / "py"
if PY_DIR.is_dir():
    path = str(PY_DIR)
    if path not in sys.path:
        sys.path.insert(0, path)

TEST_DIR = ROOT / "py" / "tests"
if TEST_DIR.is_dir():
    test_path = str(TEST_DIR)
    if test_path not in sys.path:
        sys.path.insert(0, test_path)
