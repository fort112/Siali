# app/services/photo_cache_service.py
"""Кэш Telegram file_id для фото товаров.

На сервере (Bothost) важно хранить кэш в персистентной папке.
Путь задаётся через переменную окружения PHOTO_CACHE_PATH.
Рекомендуемое значение: /app/data/photo_cache.json
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional


def _default_cache_path() -> Path:
    # дефолт рядом с кодом (локальная разработка)
    return Path(__file__).resolve().parent.parent.parent / "photo_cache.json"


def get_cache_path() -> Path:
    env_path = os.getenv("PHOTO_CACHE_PATH")
    if env_path:
        p = Path(env_path)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
    return _default_cache_path()


class PhotoCacheService:
    def __init__(self) -> None:
        self.path: Path = get_cache_path()
        self._cache: Dict[str, str] = {}
        self._loaded: bool = False

    def load(self) -> None:
        if self._loaded:
            return
        self._loaded = True

        # гарантируем папку
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        if not self.path.exists():
            # создаём пустой кэш
            try:
                self.path.write_text("{}", encoding="utf-8")
            except Exception:
                return

        try:
            raw = self.path.read_text(encoding="utf-8").strip() or "{}"
            self._cache = json.loads(raw)
            if not isinstance(self._cache, dict):
                self._cache = {}
        except Exception:
            self._cache = {}

    def get(self, key: str) -> Optional[str]:
        self.load()
        return self._cache.get(key)

    def set(self, key: str, file_id: str) -> None:
        self.load()
        self._cache[key] = file_id
        try:
            self.path.write_text(json.dumps(self._cache, ensure_ascii=False), encoding="utf-8")
        except Exception:
            # не роняем бота из-за записи кэша
            pass
