# db_master.py
"""
Работа с локальной SQLite-базой вместо Google Sheets для каталога товаров.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from app.profiling.timer import timer  # type: ignore
except Exception:  # pragma: no cover
    from contextlib import contextmanager
    import time as _time
    @contextmanager
    def timer(label: str, trace_id: str = "-"):
        t0 = _time.perf_counter()
        try:
            yield
        finally:
            _ = (_time.perf_counter() - t0)


# Файл БД лежит рядом с этим модулем (в корне проекта)
DB_PATH = Path(os.getenv("DATABASE_PATH", str(Path(__file__).parent / "bot_data.db")))

# ---------------- БАЗОВЫЕ ВЕЩИ ---------------- #

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Создаёт таблицу products, если её ещё нет.
    Вызывается один раз при старте бота.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id   INTEGER PRIMARY KEY,
            type         TEXT,
            category     TEXT,
            model        TEXT,
            material     TEXT,
            name         TEXT,
            price        REAL,
            image        TEXT,
            model_image  TEXT,
            is_active   INTEGER DEFAULT 1,
            raw_json     TEXT NOT NULL
        )
        """
    )


    # --- auto-migration: add is_active column if upgrading from old DB ---
    try:
        cur.execute("PRAGMA table_info(products)")
        cols = {row[1] for row in cur.fetchall()}
        if "is_active" not in cols:
            cur.execute("ALTER TABLE products ADD COLUMN is_active INTEGER DEFAULT 1")
    except Exception:
        # ignore migration errors (e.g., read-only or already migrated)
        pass

    # Индексы для быстрых выборок
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_type     ON products(type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_model    ON products(model)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_material ON products(material)")

    conn.commit()
    conn.close()


def clear_products() -> None:
    """
    Полностью очищает таблицу products – удобно при миграции.
    """
    conn = get_connection()
    conn.execute("DELETE FROM products")
    conn.commit()
    conn.close()


# ---------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------- #

def _to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        s = str(value).strip().replace(" ", "").replace(",", ".")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def normalize_image_link(image: Any) -> Optional[str]:
    """
    Приводит значение изображения к удобной ссылке.
    - Если пусто/None -> None
    - Если уже http/https -> оставляем как есть
    - Если это похоже на ID файла Google Drive -> делаем ссылку вида
      https://drive.google.com/uc?export=view&id=...
    Иначе -> None.
    """
    if image is None:
        return None
    s = str(image).strip()
    if not s:
        return None

    # Уже ссылка
    if s.startswith("http://") or s.startswith("https://"):
        return s

    # Похоже на ID файла в Google Drive
    import re
    if re.fullmatch(r"[a-zA-Z0-9_-]{20,200}", s):
        return f"https://drive.google.com/uc?export=view&id={s}"

    # Иначе считаем мусором
    return None


# ---------------- ЗАПИСЬ СТРОК (МИГРАЦИЯ) ---------------- #

def upsert_product(row: Dict[str, Any]) -> None:
    """
    Принимает одну строку из Google Sheets (dict) и кладёт/обновляет её в SQLite.
    В raw_json сохраняем ВСЮ строку, чтобы код бота мог работать по-старому.
    """
    # Пробуем вытащить ID товара
    product_id = (
        row.get("ID")
        or row.get("Id")
        or row.get("id")
    )
    product_id = _to_int_or_none(product_id)

    # Базовые поля
    type_val = (row.get("Тип") or row.get("type") or "").strip()
    category_val = (row.get("Категория") or row.get("category") or "").strip()
    model_val = (row.get("Модель") or row.get("model") or "").strip()
    material_val = (row.get("Материал") or row.get("material") or "").strip()

    # Активность товара (Google Sheets: Active TRUE/FALSE)
    is_active = 1
    if "Active" in row or "active" in row:
        val = row.get("Active") if "Active" in row else row.get("active")
        s = str(val).strip().lower()
        is_active = 1 if s in ("true", "1", "yes", "y", "да", "истина") else 0


    # Название – на всякий случай пробуем несколько полей
    name_val = (
        row.get("Название")
        or row.get("Название товара")
        or row.get("Товар")
        or row.get("Наименование")
        or row.get("name")
        or model_val
        or ""
    )
    name_val = str(name_val).strip()

    price_val = _to_float_or_none(
        row.get("Цена")
        or row.get("Price")
        or row.get("price")
    )

    image_val = normalize_image_link(row.get("Изображение"))
    model_image_val = normalize_image_link(row.get("Изображение модели"))

    # Обновляем row, чтобы при выдаче через raw_json там уже были нормальные ссылки/ID
    if product_id is not None:
        row["ID"] = product_id
    row["Изображение"] = image_val
    row["Изображение модели"] = model_image_val

    raw_json = json.dumps(row, ensure_ascii=False)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO products (
            product_id, type, category, model, material,
            name, price, image, model_image, is_active, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            type        = excluded.type,
            category    = excluded.category,
            model       = excluded.model,
            material    = excluded.material,
            name        = excluded.name,
            price       = excluded.price,
            image       = excluded.image,
            model_image = excluded.model_image,
            is_active   = excluded.is_active,
            raw_json    = excluded.raw_json
        """,
        (
            product_id,
            type_val or None,
            category_val or None,
            model_val or None,
            material_val or None,
            name_val or None,
            price_val,
            image_val,
            model_image_val,
            is_active,
            raw_json,
        ),
    )
    conn.commit()
    conn.close()


# ---------------- ВЫБОРКИ ДЛЯ БОТА ---------------- #

def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    """
    Преобразует строки из SQLite в список dict'ов (как раньше было из Google Sheets).
    """
    result: List[Dict[str, Any]] = []
    for r in rows:
        try:
            obj = json.loads(r["raw_json"])
            if isinstance(obj, dict):
                result.append(obj)
        except Exception:
            # В крайнем случае можно игнорировать битый JSON
            continue
    return result


def query_products(product_type: Optional[str] = None,
                   model: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Основной метод, который будет дергать бот:
    - product_type -> 'Корсет', 'Бюст', 'Набор', 'Трусики по акции', 'Сертификат' и т.п.
    - model        -> конкретная модель (если нужно)
    """
    conn = get_connection()
    cur = conn.cursor()

    sql = "SELECT * FROM products WHERE 1=1 AND is_active=1"
    params: List[Any] = []

    if product_type:
        sql += " AND (LOWER(type) = LOWER(?) OR LOWER(category) = LOWER(?))"
        params.extend([product_type, product_type])

    if model:
        sql += " AND LOWER(model) = LOWER(?)"
        params.append(model)

    with timer("db.query_products"):
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def query_all_products() -> List[Dict[str, Any]]:
    """
    Для случаев, где нужно ВСЁ (корзина/подтверждение и т.п.).
    """
    conn = get_connection()
    cur = conn.cursor()
    with timer("db.query_all_products"):
        cur.execute("SELECT * FROM products")
        rows = cur.fetchall()
    conn.close()
    return _rows_to_dicts(rows)
