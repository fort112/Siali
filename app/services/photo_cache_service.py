from __future__ import annotations

import os
import sqlite3
import time
import asyncio
from pathlib import Path
from typing import Optional, Dict

# Используем ту же БД, что и каталог (bot_data.db рядом с корнем проекта)
DB_PATH = Path(os.getenv("DATABASE_PATH", str(Path(__file__).resolve().parents[2] / "bot_data.db")))


def _ensure_db_parent_dir() -> None:
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

PHOTO_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS photo_cache (
    url TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
"""

class PhotoCacheService:
    """Кэш соответствия url -> telegram file_id.

    - Быстрый слой: in-memory dict
    - Долговременный слой: SQLite (bot_data.db)
    """

    def __init__(self):
        self._mem: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._inited = False

    def _conn(self) -> sqlite3.Connection:
        _ensure_db_parent_dir()
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn

    def init_sync(self) -> None:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(PHOTO_TABLE_SQL)
            conn.commit()
        finally:
            conn.close()

        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT url, file_id FROM photo_cache")
            for row in cur.fetchall():
                self._mem[row["url"]] = row["file_id"]
        finally:
            conn.close()

        self._inited = True

    async def init(self) -> None:
        # не блокируем event loop на старте
        await asyncio.to_thread(self.init_sync)

    async def get(self, url: str) -> Optional[str]:
        if not url:
            return None
        return self._mem.get(url)

    async def set(self, url: str, file_id: str) -> None:
        if not url or not file_id:
            return
        self._mem[url] = file_id
        ts = int(time.time())

        def _write():
            conn = self._conn()
            try:
                cur = conn.cursor()
                cur.execute(PHOTO_TABLE_SQL)
                cur.execute(
                    "INSERT INTO photo_cache(url, file_id, updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(url) DO UPDATE SET file_id=excluded.file_id, updated_at=excluded.updated_at",
                    (url, file_id, ts),
                )
                conn.commit()
            finally:
                conn.close()

        await asyncio.to_thread(_write)

    async def get_or_lock(self):
        return self._lock

photo_cache_service = PhotoCacheService()
