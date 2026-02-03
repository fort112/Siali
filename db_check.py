# db_check.py
"""Small diagnostics for Bothost: checks SQLite DB path, tables, row counts."""

import os
import sqlite3
from pathlib import Path

db_path = Path(os.getenv("DATABASE_PATH", "/app/data/bot_data.db"))
print("DATABASE_PATH =", db_path)

try:
    print("exists:", db_path.exists(), "size:", db_path.stat().st_size if db_path.exists() else None)
except Exception as e:
    print("stat error:", e)

try:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in cur.fetchall()]
    print("tables:", tables)
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            n = cur.fetchone()[0]
            print(f"count({t}) =", n)
        except Exception as e:
            print(f"count({t}) error:", e)
    conn.close()
except Exception as e:
    print("connect/query error:", e)
