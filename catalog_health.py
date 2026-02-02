# catalog_health.py
"""Проверка состояния каталога/БД (health-check).

Файл используется при старте бота (import в bot.py).
DB_PATH берётся из переменной окружения DATABASE_PATH, чтобы на bothost
можно было хранить SQLite в персистентной папке (/app/data или /data).
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Any

BASE_DIR = Path(__file__).resolve().parent

# Если DATABASE_PATH абсолютный (например /app/data/bot_data.db) — Path корректно обработает.
# Если не задан — используем локальный файл рядом с кодом.
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "bot_data.db")))


def get_catalog_health() -> Dict[str, Any]:
    """Возвращает простую диагностику доступности SQLite-базы и таблиц.

    Не падает исключениями наружу — всегда возвращает dict со статусом.
    """
    result: Dict[str, Any] = {
        "ok": False,
        "db_path": str(DB_PATH),
        "exists": DB_PATH.exists(),
        "tables": [],
        "error": None,
    }

    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = [r[0] for r in cur.fetchall()]
            result["tables"] = tables
            result["ok"] = True
        finally:
            conn.close()
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"

    return result
