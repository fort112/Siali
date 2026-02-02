# migrate_from_sheets_to_sqlite.py
"""
Синхронизация каталога из Google Sheets в SQLite (bot_data.db).

ВАЖНО (замеры):
- В листе Catalog у строки "Замеры" обычно НЕТ OldID.
- Раньше твой бот мог брать эту строку напрямую из Sheets, но теперь мы живём от SQLite.
- Поэтому эту строку нужно ВСЕ РАВНО заливать в SQLite с фиксированным ID.

Решение:
- Если OldID пустой, но строка является "Замеры" (Category/Type/Title == "Замеры"),
  то используем фиксированный ID: MEASUREMENTS_GUIDE_ID = 999999

Цвет:
- Цвет берём из Materials.Color (по MaterialSKU). Catalog.Color может отсутствовать.

Листы:
- "Catalog"   -> модели/служебные строки (в т.ч. "Замеры")
- "Materials" -> справочник материалов (MaterialSKU/MaterialName/MaterialID2/MaterialPhotoId/Color/Active)
"""

from __future__ import annotations

import os

import traceback
from typing import Any, Dict, List, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from db_master import init_db, upsert_product

CREDS_FILE = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1zBxm9vPdTBJH0CalLr690UG8hYcA7lBC3Tb4JMwHdY0")
MEASUREMENTS_GUIDE_ID = 999999


def _gs_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)


def _to_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _read_sheet_records(sh, title: str) -> List[Dict[str, Any]]:
    ws = sh.worksheet(title)
    return ws.get_all_records()


def _clean_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _to_bool(v: Any, default: bool = True) -> bool:
    """Parse Google Sheets booleans (TRUE/FALSE) and common variants."""
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "да", "истина"):
        return True
    if s in ("false", "0", "no", "n", "нет", "ложь"):
        return False
    return default


def _is_measurements_row(row: Dict[str, Any]) -> bool:
    # допускаем любое из полей
    cat = _clean_str(row.get("Category")).lower()
    typ = _clean_str(row.get("Type")).lower()
    title = _clean_str(row.get("Title")).lower()
    model = _clean_str(row.get("Model")).lower()
    return "замеры" in (cat, typ, title, model) or cat == "замеры" or typ == "замеры"


def _pick_image_id(row: Dict[str, Any]) -> str:
    # В твоём Catalog это обычно колонка ModelPhotoId
    for k in ("ModelPhotoId", "Image", "Фото", "PhotoId", "Изображение", "Изображение модели"):
        v = _clean_str(row.get(k))
        if v:
            return v
    return ""


def catalog_row_to_legacy(row: Dict[str, Any], material_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    legacy_id = _to_int_or_none(row.get("OldID"))

    # ✅ особая обработка строки "Замеры"
    if legacy_id is None:
        if _is_measurements_row(row):
            legacy_id = MEASUREMENTS_GUIDE_ID
        else:
            # обычные строки без OldID пропускаем (модели без ID ломают корзину)
            return None

    category = _clean_str(row.get("Category"))
    ptype = _clean_str(row.get("Type"))
    model = _clean_str(row.get("Model"))
    title = _clean_str(row.get("Title"))

    # Цена
    price_val = row.get("Price")
    try:
        price = float(price_val) if _clean_str(price_val) != "" else 0.0
    except Exception:
        price = 0.0

    # Фото (у замеров тоже здесь)
    image_id = _pick_image_id(row)

    # Варианты посадки (для трусиков)
    fit_variants = _clean_str(row.get("FitVariants"))

    # Материал по справочнику
    mat_sku = _clean_str(row.get("MaterialSKU"))
    mat_name = ""
    material_id2: Optional[int] = None
    material_photo_id = ""
    material_color = ""
    material_active: bool = True

    if mat_sku and mat_sku in material_map:
        m = material_map[mat_sku]
        mat_name = _clean_str(m.get("MaterialName"))
        material_id2 = _to_int_or_none(m.get("MaterialID2"))
        # fallback: если MaterialID2 отсутствует, берём из MaterialSKU (MAT0100 -> 100)
        if material_id2 is None and mat_sku:
            m_sku = __import__('re').search(r'(\d+)', mat_sku)
            if m_sku:
                material_id2 = int(m_sku.group(1))
        material_photo_id = _clean_str(m.get("MaterialPhotoId"))
        material_color = _clean_str(m.get("Color"))
        material_active = _to_bool(m.get("Active"), default=True)
    elif mat_sku:
        # Если MaterialSKU указан в Catalog, но отсутствует в Materials — считаем недоступным.
        material_active = False

    # Цвет только из Materials (Catalog.Color уже можно удалить)
    color = material_color

    legacy: Dict[str, Any] = {
        "ID": legacy_id,
        "Категория": category,
        "Тип": ptype,
        "Модель": model,
        "Название": title,
        "Цвет": color,
        "Цена": price,

        # совместимость: у тебя в коде местами смотрят и "Изображение", и "Изображение модели"
        "Изображение": image_id,
        "Изображение модели": image_id,

        "Материал": mat_name,
        "Active": row.get("Active", True),
        # Активность именно варианта материала из листа Materials (НЕ путать с Active товара в Catalog)
        "MaterialActive": material_active,

        # поля для материала/посадки
        "ID 2": material_id2,
        "Изображение материала": material_photo_id,
        "Вариант посадки": fit_variants,

        # для отладки
        "SKU": row.get("SKU"),
        "MaterialSKU": mat_sku,
        "_catalog_source": row,
    }
    return legacy


def main() -> int:
    try:
        init_db()
        client = _gs_client()
        sh = client.open_by_key(SPREADSHEET_ID)

        catalog_rows = _read_sheet_records(sh, "Catalog")
        materials_rows = _read_sheet_records(sh, "Materials")

        material_map: Dict[str, Dict[str, Any]] = {}
        for m in materials_rows:
            sku = _clean_str(m.get("MaterialSKU"))
            if not sku:
                continue
            material_map[sku] = m

        written = 0
        skipped = 0
        wrote_measurements = False

        for r in catalog_rows:
            legacy = catalog_row_to_legacy(r, material_map)
            if legacy is None:
                skipped += 1
                continue
            if legacy.get("ID") == MEASUREMENTS_GUIDE_ID:
                wrote_measurements = True
            upsert_product(legacy)
            written += 1

        print(f"✅ Sync done. Written: {written}, skipped: {skipped}, measurements_row: {'OK' if wrote_measurements else 'NOT FOUND'}")
        return 0

    except Exception as e:
        print("❌ Sync failed:", e)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
