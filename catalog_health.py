# catalog_health.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


# Путь к базе SQLite (лежит рядом с bot.py)
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "os.getenv("DATABASE_PATH", "bot_data.db")"

# Пороги напоминаний
WARNING_DAYS = 5      # мягкое напоминание
CRITICAL_DAYS = 10    # жёсткое напоминание


@dataclass
class CatalogHealth:
    status: str                  # 'ok' | 'warning' | 'critical' | 'no_db'
    age_days: Optional[int]      # возраст базы в днях (целое), либо None
    message_to_admin: Optional[str]  # готовый текст для отправки админу (или None)


def _format_days_ru(days: int) -> str:
    """
    Красивое склонение "день/дня/дней" для русского языка.
    """
    if days % 10 == 1 and days % 100 != 11:
        return f"{days} день"
    if days % 10 in (2, 3, 4) and not (12 <= days % 100 <= 14):
        return f"{days} дня"
    return f"{days} дней"


def get_catalog_health(
    warning_days: int = WARNING_DAYS,
    critical_days: int = CRITICAL_DAYS,
) -> CatalogHealth:
    """
    Проверяет «свежесть» локальной базы каталога os.getenv("DATABASE_PATH", "bot_data.db").

    Возвращает:
      - статус
      - возраст базы в днях
      - готовый текст уведомления для администратора (или None, если всё ок)
    """
    # 1) Базы нет вообще
    if not DB_PATH.exists():
        msg = (
            "⚠️ Каталог SQLite ещё не создан.\n\n"
            "Сейчас бот читает данные напрямую из Google Sheets.\n"
            "Рекомендуется выполнить миграцию каталога:\n"
            "в админ-меню нажмите «🔄 Обновить каталог»."
        )
        return CatalogHealth(
            status="no_db",
            age_days=None,
            message_to_admin=msg,
        )

    # 2) Считаем возраст файла
    mtime = DB_PATH.stat().st_mtime
    dt_mtime = datetime.fromtimestamp(mtime)
    age_days_float = (datetime.now() - dt_mtime).total_seconds() / (60 * 60 * 24)
    age_days_int = int(age_days_float)

    # 3) Меньше warning_days — всё ок, без сообщений
    if age_days_int < warning_days:
        return CatalogHealth(
            status="ok",
            age_days=age_days_int,
            message_to_admin=None,
        )

    # 4) Между warning и critical — мягкое предупреждение
    if warning_days <= age_days_int < critical_days:
        age_str = _format_days_ru(age_days_int)
        msg = (
            f"⚠️ Каталог SQLite не обновлялся уже {age_str}.\n\n"
            "Рекомендуется обновить каталог из Google Sheets, "
            "чтобы цены и наличие были актуальными.\n\n"
            "В админ-меню воспользуйтесь кнопкой «🔄 Обновить каталог»."
        )
        return CatalogHealth(
            status="warning",
            age_days=age_days_int,
            message_to_admin=msg,
        )

    # 5) Больше либо равно critical_days — жёсткое предупреждение
    age_str = _format_days_ru(age_days_int)
    msg = (
        f"⛔ Каталог SQLite устарел: {age_str} без обновления.\n\n"
        "Это может привести к некорректным ценам и моделям в боте.\n\n"
        "Настоятельно рекомендуется как можно скорее обновить каталог "
        "через кнопку «🔄 Обновить каталог» в админ-меню."
    )
    return CatalogHealth(
        status="critical",
        age_days=age_days_int,
        message_to_admin=msg,
    )
