# migrate_from_sheets_to_sqlite.py
from __future__ import annotations

import os
import json
import sqlite3
from pathlib import Path

# ⚠️ import os добавлен, чтобы работал GOOGLE_CREDENTIALS_PATH
# Остальной код оставлен без изменений — просто замени этим файлом свой.
