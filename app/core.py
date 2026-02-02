# Normalized via ast.unparse - comments removed
import asyncio

# --- Slider concurrency guards (anti double-render) ---
_slider_locks: dict[int, asyncio.Lock] = {}

import logging
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from aiogram.types import InputMediaPhoto
from aiogram import Bot, Dispatcher, F
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, Contact, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, URLInputFile
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from app.profiling.middleware import PerfMiddleware
from app.profiling.timer import timer
from app.profiling.counters import hit
from app.services.photo_cache_service import photo_cache_service
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError, TelegramBadRequest, TelegramForbiddenError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import BOT_TOKEN, ADMIN_CHAT_ID
from db_master import init_db, query_products, query_all_products
import sys
import subprocess
from config import BOT_TOKEN, ADMIN_CHAT_ID
import json
import os
import hashlib


PICKUP_ADDRESS = 'г. Санкт-Петербург, территориально метро Рыбацкое.'
CREDS_FILE = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
SPREADSHEET_ID = '1zBxm9vPdTBJH0CalLr690UG8hYcA7lBC3Tb4JMwHdY0'
STATS_NOTIFICATION_INTERVAL_DAYS = 1
BROADCAST_BATCH_SIZE = 10
BROADCAST_DELAY = 0.5
MAX_CONCURRENT_SEND = 5
# --- Channel photo cache (to speed up sliders) ---
PHOTO_STORAGE_CHANNEL_ID = -1003692314035  # @Sklad_photo
PHOTO_STORAGE_CHANNEL_TITLE = 'Sklad_photo'
PHOTO_CACHE_PATH = os.getenv('PHOTO_CACHE_PATH', os.path.join(os.path.dirname(__file__), 'photo_cache.json'))
_photo_cache_lock = asyncio.Lock()


# --- Reply keyboard helper (prevents chat spam and fixes NameError) ---
_LAST_REPLY_KB_MSG = {}

def _invalidate_reply_keyboard_cache(chat_id: int):
    """Drop cached signature/id of the last 'service' reply-keyboard message.

    Нужно вызывать, когда мы меняем reply-клавиатуру обычным message.answer(..., reply_markup=...)
    (в обход _apply_reply_keyboard). Иначе _apply_reply_keyboard может ошибочно решить,
    что клавиатура уже такая же, и НЕ переотправить её — в результате пользователь видит
    старые кнопки и может "застрять" в меню.
    """
    try:
        _LAST_REPLY_KB_MSG.pop(chat_id, None)
    except Exception:
        pass

async def _apply_reply_keyboard(message, kb):
    """Attach a reply keyboard using a single hidden 'service' message.

    Important: Telegram cannot update reply keyboards via message edits.
    So we only (re)send the service message when the keyboard actually changes.
    This prevents 'empty' messages from appearing on every color switch.
    """
    try:
        chat_id = message.chat.id

        # Compute a stable signature of the keyboard to avoid re-sending the same one
        try:
            sig = repr(kb.model_dump() if hasattr(kb, "model_dump") else kb.to_python())
        except Exception:
            sig = repr(kb)

        prev = _LAST_REPLY_KB_MSG.get(chat_id)
        if isinstance(prev, dict):
            prev_id = prev.get("msg_id")
            prev_sig = prev.get("sig")
        else:
            prev_id = prev
            prev_sig = None

        # If keyboard didn't change — do nothing (avoid new 'empty' message)
        if prev_id and prev_sig == sig:
            return

        # Remove previous service message if any
        if prev_id:
            try:
                await message.bot.delete_message(chat_id, prev_id)
            except Exception:
                pass

        sent = await message.bot.send_message(chat_id, "\u2063", reply_markup=kb)
        _LAST_REPLY_KB_MSG[chat_id] = {"msg_id": sent.message_id, "sig": sig}
    except Exception:
        # Fallback: at least try to set the keyboard once
        try:
            await message.answer("\u2063", reply_markup=kb)
        except Exception:
            await message.answer(" ", reply_markup=kb)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())

# --- PERF: измеряем время обработки апдейтов ---
dp.update.middleware(PerfMiddleware(slow_ms=250))


_ANTI_DC_LAST: dict[tuple[int, str], float] = {}


def _is_input_state_name(state_name: str) -> bool:
    """Состояния, где пользователь вводит данные руками (НЕ кнопками).

    Здесь анти-дребезг для текстовых сообщений НЕ применяем,
    чтобы не мешать вводу мерок/адреса/пожеланий и т.п.
    """
    if not state_name:
        return False
    # Любые текстовые инпуты / сбор данных
    input_parts = (
        "bust",
        "waist",
        "hips",
        "underbust",
        "horizontalarc",
        "ordernotes",
        "photo",
        "phone",
        "address",
        "sdek",
        "post",
        "certificate",
        "applycertificate",
        "certificateemail",
        "broadcasttext",
    )
    s = state_name.lower()
    return any(p in s for p in input_parts)


class AntiDoubleClickMiddleware(BaseMiddleware):
    """Глобальная защита от двойных нажатий для inline/reply кнопок."""

    def __init__(self, ttl: float = 0.9):
        self.ttl = float(ttl)

    async def __call__(self, handler, event, data):
        # CallbackQuery — это всегда кнопка
        if isinstance(event, CallbackQuery):
            user_id = int(event.from_user.id)
            key = f"cb:{event.data or ''}"
            now = time.time()

            last = _ANTI_DC_LAST.get((user_id, key))
            if last and now - last < self.ttl:
                # Убираем "часики" максимально тихо
                try:
                    await event.answer("⏳", show_alert=False, cache_time=1)
                except Exception:
                    pass
                return

            _ANTI_DC_LAST[(user_id, key)] = now

            lock = get_action_lock(user_id, f"anti_dc:{key}")
            if lock.locked():
                try:
                    await event.answer("⏳", show_alert=False, cache_time=1)
                except Exception:
                    pass
                return

            await lock.acquire()
            try:
                return await handler(event, data)
            finally:
                try:
                    lock.release()
                except Exception:
                    pass

        # Message — защиту применяем ТОЛЬКО для кнопочных текстов (reply keyboard)
        if isinstance(event, Message) and (event.text is not None):
            user_id = int(event.from_user.id)
            text = (event.text or "").strip()

            # Тексты, которые НЕ должны зависеть от FSM-состояния.
            # Иначе первый клик может поменять state, второй клик придёт с другим state_name
            # и дедуп-ключ изменится → двойная обработка.
            _STATE_INDEPENDENT_TEXTS = {"Корзина", "Перейти в корзину"}

            # Команды не трогаем
            if text.startswith("/"):
                return await handler(event, data)

            # Определяем текущее состояние
            state: FSMContext | None = data.get("state")
            state_name = ""
            if state is not None:
                try:
                    state_name = await state.get_state() or ""
                except Exception:
                    state_name = ""

            # В состояниях ввода данных — не блокируем
            if _is_input_state_name(state_name):
                return await handler(event, data)

            # Слишком длинные сообщения считаем не кнопками
            if len(text) > 80:
                return await handler(event, data)

            # Для некоторых кнопок ключ не должен включать FSM state
            # (иначе быстрый двойной клик после смены state пройдёт два раза).
            if text in _STATE_INDEPENDENT_TEXTS:
                key = f"msg:__indep__:{text}"
            else:
                key = f"msg:{state_name}:{text}"
            now = time.time()

            last = _ANTI_DC_LAST.get((user_id, key))
            if last and now - last < self.ttl:
                return

            _ANTI_DC_LAST[(user_id, key)] = now

            lock = get_action_lock(user_id, f"anti_dc:{key}")
            if lock.locked():
                return

            await lock.acquire()
            try:
                return await handler(event, data)
            finally:
                try:
                    lock.release()
                except Exception:
                    pass

        return await handler(event, data)


# Подключаем мидлварь глобально
_anti_dc_mw = AntiDoubleClickMiddleware(ttl=0.9)
dp.callback_query.middleware(_anti_dc_mw)
dp.message.middleware(_anti_dc_mw)


# --- Photo cache helpers ---
def _normalize_image_source(src: Optional[str]) -> Optional[str]:
    if not src or not isinstance(src, str):
        return None
    s = src.strip()
    if not s:
        return None

    # URLs: keep as-is
    if s.startswith(("http://", "https://")):
        return s

    # Telegram file_id обычно начинается с одного из этих префиксов (фото/видео/документы)
    # и НЕ должен превращаться в Google Drive ссылку.
    telegram_prefixes = ("AgAC", "AQAD", "BAAC", "CAAC", "CQAC", "DAAC", "EAAC", "FAAC", "GQAC", "HAA", "IgAC")
    if s.startswith(telegram_prefixes):
        return s

    # Похоже на Google Drive ID -> превращаем в direct-view ссылку
    if re.match(r"^[a-zA-Z0-9_-]{20,200}$", s):
        return f"https://drive.google.com/uc?export=view&id={s}"

    return s


def _load_photo_cache() -> dict:
    try:
        if os.path.exists(PHOTO_CACHE_PATH):
            with open(PHOTO_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception as e:
        print(f"⚠️ Не удалось прочитать {PHOTO_CACHE_PATH}: {e}")
    return {}


def _save_photo_cache(cache: dict) -> None:
    try:
        tmp_path = PHOTO_CACHE_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, PHOTO_CACHE_PATH)
    except Exception as e:
        print(f"⚠️ Не удалось сохранить {PHOTO_CACHE_PATH}: {e}")


async def ensure_photo_in_channel(image_src: Optional[str], *, caption: Optional[str] = None, trace_id: str = "-") -> Optional[str]:
    norm = _normalize_image_source(image_src)
    if not norm:
        return None

    # Если уже file_id/локальный идентификатор — отдаём как есть
    if not norm.startswith(("http://", "https://")):
        return norm

    # PERF
    hit("photo.ensure", trace_id)
    with timer("ensure_photo_in_channel", trace_id):
        # Быстрый кэш (in-memory + SQLite)
        cached_fast = await photo_cache_service.get(norm)
        if cached_fast:
            hit("photo.cache_hit", trace_id)
            return cached_fast

        cache_key = norm
        cache_key_h = hashlib.sha256(norm.encode("utf-8")).hexdigest()

        # Фоллбек: старый JSON-кэш (на случай миграции/совместимости)
        async with _photo_cache_lock:
            cache = _load_photo_cache()
            cached = cache.get(cache_key) or cache.get(cache_key_h)
            if cached:
                # поднимаем в быстрый кэш
                await photo_cache_service.set(norm, cached)
                hit("photo.json_hit", trace_id)
                return cached

            try:
                msg = await bot.send_photo(
                    PHOTO_STORAGE_CHANNEL_ID,
                    URLInputFile(norm),
                    caption=caption
                )
                file_id = msg.photo[-1].file_id if getattr(msg, "photo", None) else None
                if file_id:
                    cache[cache_key] = file_id
                    cache[cache_key_h] = file_id
                    _save_photo_cache(cache)
                    await photo_cache_service.set(norm, file_id)
                return file_id
            except TelegramForbiddenError as e:
                print(f"❌ Нет прав отправлять в канал {PHOTO_STORAGE_CHANNEL_TITLE} ({PHOTO_STORAGE_CHANNEL_ID}): {e}")
                return None
            except TelegramBadRequest as e:
                print(f"❌ Ошибка загрузки фото в канал (BadRequest): {e} | src={norm}")
                return None
            except Exception as e:
                print(f"❌ Ошибка загрузки фото в канал: {e} | src={norm}")
                return None


def escape_markdown(text: str) -> str:
    r"""
    Простейший аналог aiogram.utils.markdown.escape_markdown
    Подходит для ParseMode.MARKDOWN (не MarkdownV2).

    Экранирует служебные символы: \ * _ ` [
    """
    if text is None:
        return ''
    if not isinstance(text, str):
        text = str(text)

    # Важно сначала экранировать обратный слэш
    text = text.replace('\\', '\\\\')
    text = text.replace('*', '\\*')
    text = text.replace('_', '\\_')
    text = text.replace('`', '\\`')
    text = text.replace('[', '\\[')
    return text


def _short_material_name(material: str) -> str:
    """Убираем тех-префиксы вида 'Материал бюста:' и т.п., чтобы карточки были чище."""
    if material is None:
        return ""
    m = str(material).strip()
    if not m:
        return ""

    # Частые префиксы из таблиц
    prefixes = [
        "Материал бюста:",
        "Материал трусиков:",
        "Материал пояса:",
        "Материал корсета:",
    ]
    low = m.lower()
    for pref in prefixes:
        if low.startswith(pref.lower()):
            m = m[len(pref):].strip()
            break

    # Нормализация слов (по желанию — можно расширять)
    low = m.lower()
    if "кружев" in low:
        return "Кружево"
    if "хлоп" in low:
        return "Хлопок"
    if "сетк" in low:
        return "Эластичная сетка"
    if "вышив" in low:
        return "Вышивка"
    return m


def _short_model_name(model: str) -> str:
    """
    Чистим технические названия моделей/типов (особенно для трусиков),
    чтобы не было дубля 'Трусики из ...' в строке 'Модель'.
    Цель: показывать покупателю только форму: Стринги/Слипы/Бразилиана/Шортики и т.п.
    """
    if model is None:
        return ""
    m = str(model).strip()
    if not m:
        return ""

    low = m.lower()

    # Убираем типовые "шумные" слова
    noise_phrases = [
        "трусики из",
        "трусики",
        "бюст",
        "лиф",
        "из",
        "материал",
        "бюстгальтер",
    ]
    for p in noise_phrases:
        low = low.replace(p, "")

    low = " ".join(low.split())

    # Нормализуем формы
    forms = [
        ("стринги", "Стринги"),
        ("слипы", "Слипы"),
        ("бразили", "Бразилиана"),
        ("шорты", "Шортики"),
        ("танга", "Танга"),
        ("классик", "Классика"),
        ("high", "Высокая посадка"),
        ("low", "Низкая посадка"),
    ]
    for key, label in forms:
        if key in low:
            return label

    # fallback: вернуть аккуратно, но без лишних пробелов
    return low.strip().capitalize()


def format_item_caption(item: dict, state_data: dict, mode: str = "mini") -> str:
    """
    Единый формат карточек товара:
      - mini: название + цена (для листания)
      - context: + выбранные материал/цвет/посадка (после выбора)
      - final: ещё чуть подробнее (для корзины/подтверждения)
    """
    name = escape_markdown(str(item.get("Название", "") or "").strip()) or "Товар"

    price = safe_convert_price(item.get("Цена", 0))
    display_price = int(price) if hasattr(price, "is_integer") and price.is_integer() else price

    selected_color = (state_data.get("lingerie_set_color") or state_data.get("bust_selected_color") or state_data.get("selected_color") or state_data.get("panties_selected_color") or state_data.get("stock_belts_selected_color") or "").strip()
    if not selected_color:
        selected_color = str(item.get("Цвет") or "").strip()
    selected_material = (state_data.get("selected_material") or state_data.get("stockbelts_selected_material") or "").strip()

    material_from_item = _short_material_name(item.get("Материал", ""))
    material = _short_material_name(selected_material) if selected_material else material_from_item

    fit = str(item.get("Вариант посадки", "") or "").strip()
    model = _short_model_name(item.get("Модель", ""))
    type_ = str(item.get("Тип", "") or "").strip()

    lines = [f"*{name}*", ""]

    # Цена в карточках материалов (и любых позиций с нулевой ценой) не показываем
    if display_price and float(display_price) > 0:
        lines.append(f"*{display_price} ₽*")

    if mode in ("context", "final"):
        if material:
            lines.append(f"Материал: {escape_markdown(material)}")
        if selected_color:
            lines.append(f"Цвет: {escape_markdown(selected_color)}")
        if fit:
            lines.append(f"Посадка: {escape_markdown(fit)}")

    if mode == "final":
        # Показываем модель только если она не дублирует материал
        if model and (not material or model.lower() not in material.lower()):
            lines.append(f"Модель: {escape_markdown(model)}")
        elif type_:
            lines.append(f"Тип: {escape_markdown(type_)}")

    return "\n".join(lines).strip()


def retry_on_network_error(max_attempts=3):
    return retry(stop=stop_after_attempt(max_attempts), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type((TelegramNetworkError, TelegramServerError, ConnectionError, TimeoutError)), before_sleep=lambda retry_state: print(f'Повторная попытка {retry_state.attempt_number} из-за ошибки: {retry_state.outcome.exception()}'))

class RetryManager:

    @staticmethod
    @retry_on_network_error()
    async def send_message(chat_id: int, text: str, **kwargs):
        return await bot.send_message(chat_id, text, **kwargs)

    @staticmethod
    @retry_on_network_error()
    async def send_photo(chat_id: int, photo: str, caption: str=None, **kwargs):
        return await bot.send_photo(chat_id, photo, caption=caption, **kwargs)

    @staticmethod
    @retry_on_network_error()
    async def send_video(chat_id: int, video: str, caption: str=None, **kwargs):
        return await bot.send_video(chat_id, video, caption=caption, **kwargs)

    @staticmethod
    @retry_on_network_error()
    async def send_video_note(chat_id: int, video_note: str, **kwargs):
        return await bot.send_video_note(chat_id, video_note, **kwargs)

class DataCache:

    def __init__(self):
        self._cache = {}
        self._timestamps = {}

    def get(self, key, max_age=300):
        if key in self._cache and time.time() - self._timestamps.get(key, 0) < max_age:
            return self._cache[key]
        return None

    def set(self, key, value):
        self._cache[key] = value
        self._timestamps[key] = time.time()

    def clear(self, key=None):
        if key:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
        else:
            self._cache.clear()
            self._timestamps.clear()
data_cache = DataCache()

class UserStats:

    def __init__(self):
        self._users = {}
        self._last_notification_sent = None

    def add_user(self, user_id: int, username: str=None, first_name: str=None):
        current_time = time.time()
        if user_id not in self._users:
            self._users[user_id] = {'first_seen': current_time, 'last_seen': current_time, 'visit_count': 1, 'username': username, 'first_name': first_name, 'is_new': True}
            return True
        else:
            self._users[user_id]['last_seen'] = current_time
            self._users[user_id]['visit_count'] += 1
            self._users[user_id]['is_new'] = False
            return False

    def get_stats(self):
        current_time = time.time()
        thirty_days_ago = current_time - 30 * 24 * 60 * 60
        total_users = len(self._users)
        new_users_today = 0
        new_users_week = 0
        new_users_month = 0
        active_users_today = 0
        active_users_week = 0
        today_start = current_time - 24 * 60 * 60
        week_start = current_time - 7 * 24 * 60 * 60
        for user_data in self._users.values():
            first_seen = user_data['first_seen']
            last_seen = user_data['last_seen']
            if first_seen >= today_start:
                new_users_today += 1
            if first_seen >= week_start:
                new_users_week += 1
            if first_seen >= thirty_days_ago:
                new_users_month += 1
            if last_seen >= today_start:
                active_users_today += 1
            if last_seen >= week_start:
                active_users_week += 1
        return {'total_users': total_users, 'new_users_today': new_users_today, 'new_users_week': new_users_week, 'new_users_month': new_users_month, 'active_users_today': active_users_today, 'active_users_week': active_users_week}

    def should_send_notification(self):
        if self._last_notification_sent is None:
            return True
        current_time = time.time()
        interval_days = STATS_NOTIFICATION_INTERVAL_DAYS
        if interval_days is None:
            interval_days = 1
        interval_seconds = interval_days * 24 * 60 * 60
        return current_time - self._last_notification_sent >= interval_seconds

    def mark_notification_sent(self):
        self._last_notification_sent = time.time()

    def save_stats_to_sheet(self, order_manager):
        try:
            worksheet = order_manager._get_client().open_by_key(SPREADSHEET_ID).worksheet('Статистика')
        except gspread.WorksheetNotFound:
            spreadsheet = order_manager._get_client().open_by_key(SPREADSHEET_ID)
            worksheet = spreadsheet.add_worksheet(title='Статистика', rows=1000, cols=10)
            headers = ['Дата', 'Всего пользователей', 'Новых за сегодня', 'Новых за неделю', 'Активных за сегодня', 'Активных за неделю']
            worksheet.append_row(headers)
        stats = self.get_stats()
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_data = [current_date, stats['total_users'], stats['new_users_today'], stats['new_users_week'], stats['active_users_today'], stats['active_users_week']]
        worksheet.append_row(row_data)
        return True
user_stats = UserStats()

class AdminPanel(StatesGroup):
    MainMenu = State()
    StatsSettings = State()
    ChangeNotificationInterval = State()
    Broadcast = State()
    BroadcastText = State()
    BroadcastMedia = State()
    BroadcastConfirmation = State()

class Order(StatesGroup):
    MainMenu = State()
    OrderMenu = State()
    CorsetMenu = State()
    CorsetView = State()
    CartView = State()
    Checkout = State()
    PrivacyPolicy = State()
    Measurements = State()
    Bust = State()
    Waist = State()
    Hips = State()
    StockBeltsModel = State()
    Underbust = State()
    HorizontalArc = State()
    OrderNotes = State()
    Photo = State()
    Phone = State()
    Delivery = State()
    Address = State()
    Confirmation = State()
    SdekAddress = State()
    PostAddress = State()
    ConfirmPickup = State()
    ConfirmDelivery = State()
    PantiesMenu = State()
    PantiesMaterial = State()
    PantiesColor = State()
    PantiesType = State()
    PantiesView = State()
    PantiesFit = State()
    BustMenu = State()
    BustColor = State()
    BustMaterial = State()
    BustView = State()
    BustModel = State()
    AccessoriesMenu = State()
    StockBeltsMenu = State()
    StockBeltsColor = State()
    StockBeltsMaterial = State()
    StockBeltsView = State()
    OtherAccessoriesView = State()
    CertificateFormat = State()
    ElectronicCertificate = State()
    PaperCertificate = State()
    CertificateAmount = State()
    CertificateEmail = State()
    ApplyCertificate = State()
    BustMeasurementsComplete = State()
class LingerieSet(StatesGroup):
    """FSM для раздела 'Комплект белья'.
    Отдельный сценарий, не затрагивает существующие 'Бюст' и 'Трусики'.
    """
    MaterialMenu = State()       # выбор базового материала комплекта (кнопки)
    ColorMenu = State()          # выбор цвета комплекта (кнопки)

    BustMaterial = State()       # слайдер материалов бюста (фото материалов)
    BustModel = State()          # слайдер моделей бюста

    PantiesType = State()        # выбор типа трусиков (кнопки)
    PantiesModel = State()       # слайдер моделей трусиков выбранного типа (все модели типа)
    PantiesFit = State()         # выбор посадки (inline)
    PantiesView = State()        # меню после добавления трусиков в комплект (Выбрать еще / Корзина / Каталог)


class CertificateManager:

    def __init__(self, creds_file: str, spreadsheet_id: str):
        self.creds_file = creds_file
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = 'Сертификаты'
        self._client = None
        self._worksheet = None

    @retry_on_network_error()
    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, scope)
            self._client = gspread.authorize(creds)
            return self._client
        except Exception as e:
            logging.error(f'Ошибка авторизации Google Sheets: {e}')
            return None

    @retry_on_network_error()
    def _get_worksheet(self):
        if self._worksheet is not None:
            return self._worksheet
        try:
            client = self._get_client()
            if not client:
                return None
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            try:
                self._worksheet = spreadsheet.worksheet(self.worksheet_name)
            except gspread.WorksheetNotFound:
                self._worksheet = spreadsheet.add_worksheet(title=self.worksheet_name, rows=1000, cols=10)
                headers = ['Номер сертификата', 'Номинал', 'Статус', 'Дата активации', 'Дата использования', 'ID пользователя', 'ID заказа']
                self._worksheet.append_row(headers)
            return self._worksheet
        except Exception as e:
            logging.error(f'Ошибка получения листа сертификатов: {e}')
            return None


    def _sanitize_headers(self, headers: list[str]) -> list[str]:
        """Make worksheet headers unique & non-empty for gspread.get_all_records()."""
        seen: dict[str, int] = {}
        cleaned: list[str] = []
        for i, h in enumerate(headers, start=1):
            name = (h or '').strip()
            if not name:
                name = f'__col_{i}'
            if name in seen:
                seen[name] += 1
                name = f'{name}_{seen[name]}'
            else:
                seen[name] = 1
            cleaned.append(name)
        return cleaned

    def _get_all_records_safe(self, worksheet) -> list[dict]:
        """Read records even if header row has duplicates/empty cells."""
        try:
            return worksheet.get_all_records()
        except Exception as e:
            msg = str(e)
            if 'header row' in msg and 'duplicates' in msg:
                try:
                    headers = worksheet.row_values(1) or []
                    expected = self._sanitize_headers(headers)
                    return worksheet.get_all_records(expected_headers=expected)
                except Exception as e2:
                    logging.error(f'Ошибка чтения записей сертификатов (fallback): {e2}')
                    return []
            logging.error(f'Ошибка чтения записей сертификатов: {e}')
            return []

    def _record_get(self, record: dict, keys: list[str], default=None):
        for k in keys:
            if k in record and record[k] not in (None, ''):
                return record[k]
        return default

    @retry_on_network_error()
    def validate_certificate(self, certificate_number: str) -> dict:
        try:
            worksheet = self._get_worksheet()
            if not worksheet:
                return {'valid': False, 'amount': 0, 'message': 'Ошибка доступа к базе сертификатов'}
            records = self._get_all_records_safe(worksheet)
            for record in records:
                num = str(self._record_get(record, ['Номер сертификата','Номер','Сертификат'], '')).strip()
                if num == str(certificate_number).strip():
                    status = str(self._record_get(record, ['Статус','status'], '')).strip()
                    if status == 'Активен':
                        nom = self._record_get(record, ['Номинал','Сумма','Amount'], 0)
                        try:
                            amount = int(float(str(nom).replace(' ', '').replace(',', '.')))
                        except Exception:
                            amount = 0
                        return {'valid': True, 'amount': amount, 'message': f"✅ Сертификат на {amount} руб. активен"}
                    else:
                        return {'valid': False, 'amount': 0, 'message': '❌ Сертификат уже использован'}
            return {'valid': False, 'amount': 0, 'message': '❌ Сертификат не найден'}
        except Exception as e:
            logging.error(f'Ошибка проверки сертификата: {e}')
            return {'valid': False, 'amount': 0, 'message': '❌ Ошибка проверки сертификата'}

    @retry_on_network_error()
    def apply_certificate(self, certificate_number: str, user_id: int, order_number: str) -> bool:
        try:
            worksheet = self._get_worksheet()
            if not worksheet:
                return False
            records = self._get_all_records_safe(worksheet)
            for i, record in enumerate(records, start=2):
                num = str(self._record_get(record, ['Номер сертификата','Номер','Сертификат'], '')).strip()
                if num == str(certificate_number).strip():
                    worksheet.update_cell(i, 3, 'Использован')
                    worksheet.update_cell(i, 5, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    worksheet.update_cell(i, 6, user_id)
                    worksheet.update_cell(i, 7, order_number)
                    return True
            return False
        except Exception as e:
            logging.error(f'Ошибка применения сертификата: {e}')
            return False
certificate_manager = CertificateManager(CREDS_FILE, SPREADSHEET_ID)

class GoogleSheetsOrderManager:

    def __init__(self, creds_file: str, spreadsheet_id: str):
        self.creds_file = creds_file
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = 'Заказы'
        self._client = None
        self._worksheet = None

    @retry_on_network_error()
    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, scope)
            self._client = gspread.authorize(creds)
            return self._client
        except Exception as e:
            logging.error(f'Ошибка авторизации Google Sheets: {e}')
            return None

    @retry_on_network_error()
    def _get_worksheet(self):
        if self._worksheet is not None:
            return self._worksheet
        try:
            client = self._get_client()
            if not client:
                return None
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            try:
                self._worksheet = spreadsheet.worksheet(self.worksheet_name)
            except gspread.WorksheetNotFound:
                self._worksheet = spreadsheet.add_worksheet(title=self.worksheet_name, rows=1000, cols=20)
                headers = ['ID заказа', 'Дата и время', 'Имя клиента', 'Телефон', 'Способ доставки', 'Адрес/ПВЗ', 'Email', 'Обхват груди', 'Горизонтальная дуга', 'Обхват под грудью', 'Обхват талии', 'Обхват бедер', 'Пожелания к заказу', 'Фото (ID)', 'Состав заказа', 'Итоговая сумма', 'Статус заказа']
                self._worksheet.append_row(headers)
            return self._worksheet
        except Exception as e:
            logging.error(f'Ошибка получения листa: {e}')
            return None

    
    def format_cart_items(self, cart: list) -> str:
        """Подробный состав заказа для записи в Google Sheets (в один столбец, с переносами строк)."""
        # Используем тот же формат, что и для админа (но это plain-text; Markdown-экранирование нам не мешает)
        try:
            return build_sheet_order_items_text(cart)
        except Exception:
            # fallback: максимально простой формат
            parts = []
            for it in cart or []:
                q = int(it.get('quantity', 1) or 1)
                name = it.get('Название') or it.get('Модель') or f"ID {it.get('ID')}"
                color = it.get('Цвет') or ''
                parts.append(f"{name} | Цвет: {color} | x{q} | ID: {it.get('ID')}")
            return "\n".join(parts)


    def _get_certificate_email(self, cart: list) -> str:
        for item in cart:
            if item.get('is_certificate') and item.get('certificate_type') == 'electronic':
                return item.get('Email', '')
        return ''

    @retry_on_network_error()
    def save_order_to_sheet(self, order_data: dict) -> tuple:
        try:
            worksheet = self._get_worksheet()
            if not worksheet:
                logging.error('Не удалось получить доступ к таблице')
                return (False, '')
            cart = order_data.get('cart', [])
            order_number = self._generate_order_number()
            row_data = [order_number, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), order_data.get('user_name', ''), order_data.get('phone', ''), order_data.get('delivery', ''), order_data.get('address', ''), self._get_certificate_email(cart), order_data.get('bust', ''), order_data.get('horizontal_arc', ''), order_data.get('underbust', ''), order_data.get('waist', ''), order_data.get('hips', ''), order_data.get('order_notes', ''), order_data.get('photo_id', ''), self.format_cart_items(cart), order_data.get('total_amount', 0), 'Новый']
            worksheet.append_row(row_data)
            logging.info(f'Заказ {order_number} успешно сохранен в Google Таблицу')
            return (True, order_number)
        except Exception as e:
            logging.error(f'Ошибка сохранения заказа в таблицу: {e}')
            return (False, '')

    def _generate_order_number(self):
        from datetime import datetime
        now = datetime.now()
        date_part = now.strftime('%y%m%d')
        try:
            worksheet = self._get_worksheet()
            if worksheet:
                all_orders = worksheet.get_all_records()
                max_number = 0
                for order in all_orders:
                    order_id = order.get('ID заказа', '')
                    if '-' in order_id and len(order_id.split('-')) == 2:
                        try:
                            num_part = order_id.split('-')[1]
                            num = int(num_part)
                            max_number = max(max_number, num)
                        except (ValueError, IndexError):
                            continue
                next_number = max_number + 1
            else:
                next_number = 1
        except:
            next_number = 1
        return f'{date_part}-{next_number:04d}'
order_manager = GoogleSheetsOrderManager(CREDS_FILE, SPREADSHEET_ID)

class UserCarts:

    def __init__(self):
        self._carts = {}
        self._timestamps = {}
        self._applied_certificates = {}

    def get(self, user_id: int):
        current_time = time.time()
        expired_users = [uid for uid, ts in self._timestamps.items() if current_time - ts > 86400]
        for uid in expired_users:
            self._carts.pop(uid, None)
            self._timestamps.pop(uid, None)
            self._applied_certificates.pop(uid, None)
        return self._carts.get(user_id, [])

    def set(self, user_id: int, cart: list):
        self._carts[user_id] = cart
        self._timestamps[user_id] = time.time()

    def clear(self, user_id: int):
        self._carts.pop(user_id, None)
        self._timestamps.pop(user_id, None)
        self._applied_certificates.pop(user_id, None)

    def get_applied_certificate(self, user_id: int):
        return self._applied_certificates.get(user_id)

    def set_applied_certificate(self, user_id: int, certificate_data: dict):
        self._applied_certificates[user_id] = certificate_data

    def clear_applied_certificate(self, user_id: int):
        self._applied_certificates.pop(user_id, None)
user_carts = UserCarts()

# Последний выбранный цвет на пользователя.
# Нужен, чтобы цвет не терялся в тех ветках, где add_item_to_cart вызывается напрямую
# без прокидывания FSMContext.
USER_LAST_COLOR: dict[int, str] = {}



# --- ANTI DOUBLE-CLICK / FSM-LOCK (in-memory) ---
# Защищает критичные места (add_to_cart, подтверждение заказа) от двойных нажатий/дубликатов.
# Ключ: (user_id, action_key) -> asyncio.Lock
_ACTION_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}

def get_action_lock(user_id: int, action_key: str) -> asyncio.Lock:
    key = (int(user_id), str(action_key))
    lock = _ACTION_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _ACTION_LOCKS[key] = lock
    return lock


def remember_user_color(user_id: int, color: str) -> None:
    color = (color or "").strip()
    if color:
        USER_LAST_COLOR[user_id] = color


def debug_cart_contents(cart, function_name):
    """Выводит отладочную информацию о содержимом корзины"""
    print(f"🔍 [{function_name}] АНАЛИЗ КОРЗИНЫ:")
    if not cart:
        print("   Корзина пуста")
        return

    for i, item in enumerate(cart, 1):
        print(f"   {i}. {item.get('Название', 'Без названия')}")
        print(f"      Тип: {item.get('Тип', 'Не указан')}")
        print(f"      Категория: {item.get('Категория', 'Не указана')}")
        print(f"      Модель: {item.get('Модель', 'Не указана')}")
        print(f"      Материал: {item.get('Материал', 'Не указан')}")
        print(f"      is_panties: {item.get('is_panties', False)}")
        print(f"      ID: {item.get('ID', 'Без ID')}")
        print(f"      is_certificate: {item.get('is_certificate', False)}")
        print(f"      ---")



def remove_previous_bust_items(user_id: int):
    """Удаляет предыдущие материалы бюста при выборе нового материала (оставляет модели)"""
    cart = user_carts.get(user_id)
    if not cart:
        return

    print("🔍 Удаление предыдущих материалов бюста (модели остаются)")

    # Удаляем только материалы бюста, но оставляем модели
    new_cart = [item for item in cart if not (
            item.get('Материал') and
            (not item.get('Модель')) and
            any((mat in str(item.get('Материал', '')).lower() for mat in [
                'хлопковый', 'кружевной', 'эластичной сетки', 'кружевной с вышивкой'
            ]))
    )]

    print(f"🔍 Удалено материалов бюста: {len(cart) - len(new_cart)}")
    user_carts.set(user_id, new_cart)

def remove_previous_stock_belts_items(user_id: int):
    """Удаляет 'висящие' материалы поясов для чулок при выборе нового материала.
    Оставляет уже собранные/добавленные модели поясов (is_stock_belt=True) и любые другие товары.
    """
    cart = user_carts.get(user_id)
    if not cart:
        return

    def _is_stock_belt_material(ci: dict) -> bool:
        mat = str(ci.get('Материал', '') or '').lower()
        return (
            ci.get('Материал') and
            (not ci.get('Модель')) and
            (('материал пояса' in mat) or bool(ci.get('is_stock_belt_material'))) and
            (ci.get('Тип') in ['Аксессуары', 'Пояс для чулок'])
        )

    before = len(cart)
    new_cart = [ci for ci in cart if not _is_stock_belt_material(ci)]
    removed = before - len(new_cart)

    if removed:
        print(f"🔍 Удалено материалов поясов для чулок: {removed}")
        user_carts.set(user_id, new_cart)


def _detect_bust_category(text: str) -> str | None:
    """
    Определяет "тип материала" бюста по строке:
    - хлопковый бюст / материал бюста: хлопковый -> 'cotton'
    - кружевной бюст / материал бюста: кружевной -> 'lace'
    - бюст из эластичной сетки / материал бюста: эластичная сетка -> 'mesh'
    - бюст из вышивки / материал бюста: вышивка -> 'embroidery'
    """
    if not text:
        return None

    t = str(text).lower()

    if 'вышивк' in t:
        return 'embroidery'
    if 'эластичн' in t and 'сетк' in t:
        return 'mesh'
    if 'хлопков' in t:
        return 'cotton'
    if 'кружевн' in t:
        return 'lace'

    return None





def validate_bust_order(cart):
    """
    Упрощённая и надёжная валидация заказа бюстов.

    В текущей архитектуре у каждой модели бюста в корзине уже записан свой материал в поле "Материал"
    (например: "Материал бюста: Вышивка"). Поэтому отдельные строки "Материал: ..." в корзине
    не считаем обязательными и НЕ пытаемся "сопоставлять" их с моделями.

    Правило:
    - если в корзине есть модели бюста, то у каждой такой модели должен быть заполнен материал бюста
      (поле "Материал" содержит "Материал бюста").
    """

    try:
        bust_models = []

        for item in (cart or []):
            name = str(item.get('Название', '') or '').lower()
            model = str(item.get('Модель', '') or '').strip()
            mat = str(item.get('Материал', '') or '').strip()

            # Материальные строки вида "Материал: ..." пропускаем
            is_bust_material_line = ('материал бюста' in name) and name.startswith('материал')
            if is_bust_material_line:
                continue

            # Трусики / пояс / прочее — пропускаем
            if bool(item.get('is_panties')) or bool(item.get('is_stock_belt')):
                continue

            # Модель бюста (а НЕ корсет/прочее).
            # Раньше считали "любое наличие поля 'Модель'" как бюст — из-за этого корсеты попадали в проверку.
            name_low = name.lower()
            model_low = model.lower()
            type_low = str(item.get('Тип', '') or '').lower()

            # Корсеты исключаем явно
            if ('корсет' in name_low) or ('corset' in name_low) or ('корсет' in model_low) or ('corset' in model_low) or ('корсет' in type_low) or ('corset' in type_low):
                continue

            # Бюст считаем только если есть явные маркеры "бюст/bust" в названии/модели/типе
            is_bust_model = (
                ('бюст' in name_low) or ('bust' in name_low) or
                ('бюст' in model_low) or ('bust' in model_low) or
                ('бюст' in type_low) or ('bust' in type_low)
            )
            if is_bust_model:
                bust_models.append(item)

        # Если бюстов нет — всё ок
        if not bust_models:
            return True, 'OK'

        # Проверяем, что у каждой модели бюста есть материал бюста
        for bm in bust_models:
            mat = str(bm.get('Материал', '') or '').strip()
            if (not mat) or ('материал бюста' not in mat.lower()):
                model_name = bm.get('Название') or bm.get('Модель') or 'Неизвестная модель'
                return False, f'Для модели {model_name} не указан материал бюста.'

        return True, 'OK'

    except Exception:
        # Никогда не валим заказ из‑за ошибки валидации
        return True, 'OK'


def load_data_from_master_cached(product_type=None, model=None, cache_key=None):
    if cache_key:
        cached_data = data_cache.get(cache_key)
        if cached_data is not None:
            return cached_data
    data = _load_data_from_master_impl(product_type, model)
    if cache_key:
        data_cache.set(cache_key, data)
    return data

@retry_on_network_error()
def _load_data_from_master_impl(product_type=None, model=None):
    """Загрузка данных каталога из локальной SQLite через db_master.

    product_type — то, что раньше передавалось как тип/категория ("Корсет", "Трусики", "Набор" и т.п.)
    model        — конкретная модель (если нужно).
    """
    try:
        # Если нужно отладить, можно здесь временно печатать входные параметры:
        # print(f"[DEBUG] _load_data_from_master_impl: product_type={product_type!r}, model={model!r}")
        if product_type is None and model is None:
            rows = query_all_products()
        else:
            rows = query_products(product_type=product_type, model=model)
        return rows
    except Exception as e:
        print(f"Ошибка чтения каталога из SQLite: {e}")
        return []

def escape_markdown(text):
    if text is None:
        return ''
    if not isinstance(text, str):
        text = str(text)
    escape_chars = '_*[]()~`>#+-=|{}\\!'
    return re.sub(f'([{re.escape(escape_chars)}])', '\\\\\\1', text)

def safe_convert_price(price):
    if price is None:
        return 0
    if isinstance(price, (int, float)):
        return price
    if isinstance(price, str):
        try:
            cleaned_price = price.replace(' ', '').replace(',', '.')
            return float(cleaned_price)
        except (ValueError, AttributeError):
            return 0
    return 0

def _is_number(txt: str) -> bool:
    return bool(re.fullmatch('\\d+(\\.\\d+)?', txt.strip()))


# ↓↓↓ ДОБАВЬТЕ ЗДЕСЬ ↓↓↓
def get_panties_type_keyboard(selected_material: str) -> ReplyKeyboardMarkup:
    """Возвращает клавиатуру с типами трусиков в зависимости от материала"""

    # ОПРЕДЕЛЯЕМ ДОСТУПНЫЕ ТИПЫ ДЛЯ КАЖДОГО МАТЕРИАЛА
    available_types = {
        'Хлопковые трусики': ['Стринги', 'Бразильянки', 'Шорты'],  # Без Классики
        'Трусики из эластичной сетки': ['Стринги', 'Бразильянки', 'Классика'],  # Без Шорт
        'Кружевные трусики': ['Стринги', 'Бразильянки', 'Классика'],  # Без Шорт
        'Материал трусиков: Вышивка': ['Стринги', 'Бразильянки', 'Классика']
    }

    types = available_types.get(selected_material, ['Стринги', 'Бразильянки', 'Классика', 'Шорты'])

    # Создаем кнопки для доступных типов
    keyboard = []
    row = []
    for panties_type in types:
        row.append(KeyboardButton(text=panties_type))
        if len(row) == 2:  # По 2 кнопки в ряду
            keyboard.append(row)
            row = []
    if row:  # Добавляем оставшиеся кнопки
        keyboard.append(row)

    # Добавляем служебные кнопки
    keyboard.append([KeyboardButton(text='Назад к материалам'), KeyboardButton(text='Корзина')])

    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=keyboard)
# ↑↑↑ ДОБАВЬТЕ ЗДЕСЬ ↑↑↑




@retry_on_network_error()
def _load_all_bust_rows():
    """Возвращает все строки категории 'Бюст' (оригинальная структура таблицы)."""
    return load_data_from_master_cached(product_type='Бюст', cache_key='bust_all_rows')

@retry_on_network_error()
def build_material_items_for_slider(material_name: str, color: str | None = None) -> list:
    """Создает элементы материалов бюста для слайдера.
    Если указан color — фильтрует по колонке 'Цвет' в той же строке.
    'Изображение материала' считается Telegram file_id или URL (не Drive).
    """
    all_rows = _load_all_bust_rows()
    material_name_norm = (material_name or '').strip().lower()
    color_norm = (color or '').strip() if color else None
    items: list[dict] = []
    seen: set[str] = set()

    def _to_bool(v, default=True):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y", "да"):
            return True
        if s in ("false", "0", "no", "n", "нет"):
            return False
        return default

    for row in all_rows:
        # ВАЖНО: отключенные варианты материалов (из листа Materials.Active)
        # должны исчезать из выбора.
        mat_active = row.get('MaterialActive')
        if mat_active is not None and not _to_bool(mat_active, default=True):
            continue
        row_material = str(row.get('Материал', '') or '').strip()
        if not row_material:
            continue
        if row_material.strip().lower() != material_name_norm:
            continue

        if color_norm:
            row_color = str(row.get('Цвет', '') or '').strip()
            if row_color != color_norm:
                continue

        rec: dict = {}
        rec['is_panties'] = True
        id2 = row.get('ID 2')
        main_id = row.get('ID')
        try:
            if id2 and str(id2).strip() and (int(float(id2)) != 0):
                rec['ID'] = int(float(id2))
            elif main_id:
                # Если в таблице материалов нет 'ID 2', пытаемся стабильно получить его из MaterialSKU (например MAT0100 -> 100)
                sku_raw = row.get('MaterialSKU') or row.get('material_sku') or row.get('Артикул материала') or row.get('Артикул') or row.get('SKU')
                sku_s = str(sku_raw).strip() if sku_raw is not None else ''
                m_sku = re.search(r'(\d+)', sku_s) if sku_s else None
                if m_sku:
                    rec['ID'] = int(m_sku.group(1))
                else:
                    rec['ID'] = int(float(main_id)) * 1000
            else:
                rec['ID'] = abs(hash(row_material)) % 10**9
        except Exception:
            rec['ID'] = abs(hash(row_material)) % 10**9

        rec['Материал'] = row_material

        rec['Название'] = f'Материал: {row_material}'
        rec['Описание'] = f'Выбран материал: {row_material}' + (f' (цвет: {color_norm})' if color_norm else '')
        rec['Цена'] = 0
        rec['Тип'] = 'Бюст'

        img_raw = row.get('Изображение материала') or row.get('Изображение') or ''
        img = img_raw.strip() if isinstance(img_raw, str) else ''
        rec['Изображение'] = _normalize_image_source(img) if img else None

        dedupe_key = rec['Изображение'] or f"{row_material}|{color_norm or ''}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        items.append(rec)

    items.sort(key=lambda x: x.get('ID') or 0)
    return items



@retry_on_network_error()
def build_model_items_for_slider(selected_material: str) -> list:
    """
    Возвращает список моделей бюста для выбранного материала.

    Логика:
    - Материал бюста: Хлопковый        -> модель "Хлопковый бюст"
    - Материал бюста: Кружевной       -> модель "Кружевной бюст"
    - Материал бюста: Эластичная сетка -> модель "Бюст из эластичной сетки"
    - Материал бюста: Вышивка         -> ЛЮБАЯ модель бюста, в названии которой есть "вышивк"
                                          (и "с вышивкой", и "из вышивки")
    """
    sel = (selected_material or "").strip().lower()

    # Для материалов без вышивки — точное соответствие модели
    base_mapping = {
        "материал бюста: хлопковый": "хлопковый бюст",
        "материал бюста: кружевной": "кружевной бюст",
        "материал бюста: эластичная сетка": "бюст из эластичной сетки",
    }

    all_rows = _load_all_bust_rows()
    models: list[dict] = []

    # Проверяем, выбран ли "материал бюста: вышивка" (или любой вариант с «вышивк»)
    is_embroidery_material = "вышивк" in sel
    target_model_lower = base_mapping.get(sel, None)

    if target_model_lower:
        target_model_lower = target_model_lower.lower()

    for row in all_rows:
        row_model = str(row.get("Модель", "") or "").strip()
        row_material = str(row.get("Материал", "") or "").strip()

        row_model_lower = row_model.lower()
        row_material_lower = row_material.lower()

        add = False

        # 1) Материал бюста: Вышивка → ищем по названию модели, где есть "вышивк"
        if is_embroidery_material:
            if "вышивк" in row_model_lower:
                add = True

        # 2) Базовые материалы (хлопковый, кружевной, сетка) → точное имя модели
        elif target_model_lower:
            if row_model_lower == target_model_lower:
                add = True

        # 3) На всякий случай fallback: если ничего не сматчилось по mapping —
        #    пробуем совпадение по полю "Материал"
        else:
            if row_material_lower == sel:
                add = True

        if not add:
            continue

        # Формируем карточку
        rec: dict = {}
        try:
            rec["ID"] = int(float(row.get("ID")))
        except Exception:
            rec["ID"] = abs(hash(row_model_lower or row.get("Название", ""))) % 10**9

        rec["Модель"] = row_model
        rec["Название"] = row.get("Название") or row_model
        rec["Описание"] = f"Модель бюста: {rec['Название']}"
        rec["Цена"] = row.get("Цена") or 2500
        rec["Материал"] = row_material

        img = row.get("Изображение модели") or row.get("Изображение") or ""
        if isinstance(img, str) and img.strip():
            if img.startswith(("http://", "https://")):
                rec["Изображение"] = img
            elif re.match(r"^[a-zA-Z0-9_-]{20,200}$", img):
                rec["Изображение"] = f"https://drive.google.com/uc?export=view&id={img}"
            else:
                rec["Изображение"] = None
        else:
            rec["Изображение"] = None

        models.append(rec)

    models.sort(key=lambda x: (x.get("Название") or "", x.get("ID") or 0))
    return models



PANTIES_DEBUG: bool = False

@retry_on_network_error()
def _load_all_panties_rows(debug: bool = False):
    """Возвращает все строки категории 'Трусики'.
    Важно: без спама в консоль (полный дамп отключён), чтобы не тормозить обработчики.
    """
    try:
        all_data = load_data_from_master_cached(product_type='Трусики', cache_key='panties_all_rows')
        if not all_data:
            print("⚠️ Не найдено данных для категории 'Трусики'")
            return []
        if debug:
            print(f"✅ Загружено строк с категорией 'Трусики': {len(all_data)}")
            for i, row in enumerate(all_data[:5], 1):
                print(f"   {i}. ID={row.get('ID')} Материал={row.get('Материал')} Тип={row.get('Тип')} Модель={row.get('Модель')}")
        return all_data
    except Exception as e:
        print(f'❌ Ошибка загрузки данных трусиков: {e}')
        return []

async def _get_panties_rows_cached(state: FSMContext) -> list:
    """Кэш rows трусиков на время сценария (FSM), чтобы не грузить и не фильтровать заново."""
    data = await state.get_data()
    rows = data.get('panties_rows')
    if isinstance(rows, list) and rows:
        return rows
    rows = _load_all_panties_rows(debug=False)
    await state.update_data(panties_rows=rows)
    return rows


@retry_on_network_error()
def build_panties_material_items_for_slider(material_name: str, color: Optional[str] = None, all_rows: Optional[list] = None) -> list:
    """Создает элементы материалов трусиков для слайдера (с фильтрацией по цвету).

    Важно: поле 'Изображение материала' ожидается как Telegram file_id (или URL).
    Цвет берётся напрямую из колонки 'Цвет' в той же строке.
    """
    if all_rows is None:
        all_rows = _load_all_panties_rows(debug=False)
    material_name_norm = (material_name or '').strip()
    color_norm = (color or '').strip() if color else None

    items: list[dict] = []
    seen: set[str] = set()
    print(f"🔍 ПОИСК МАТЕРИАЛА: '{material_name_norm}'" + (f" | цвет='{color_norm}'" if color_norm else ""))

    def _to_bool(v, default=True):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y", "да"):
            return True
        if s in ("false", "0", "no", "n", "нет"):
            return False
        return default

    for row in all_rows:
        mat_active = row.get('MaterialActive')
        if mat_active is not None and not _to_bool(mat_active, default=True):
            continue
        row_material = str(row.get('Материал', '') or '').strip()
        if not row_material:
            continue

        # Точное совпадение материала
        if row_material != material_name_norm:
            continue

        # Фильтрация по цвету из этой же строки
        if color_norm:
            row_color = str(row.get('Цвет', '') or '').strip()
            if row_color != color_norm:
                continue

        rec: dict = {}
        id2 = row.get('ID 2')
        main_id = row.get('ID')
        try:
            if id2 and str(id2).strip() and (int(float(id2)) != 0):
                rec['ID'] = int(float(id2))
            elif main_id:
                # Если в таблице материалов нет 'ID 2', пытаемся стабильно получить его из MaterialSKU (например MAT0100 -> 100)
                sku_raw = row.get('MaterialSKU') or row.get('material_sku') or row.get('Артикул материала') or row.get('Артикул') or row.get('SKU')
                sku_s = str(sku_raw).strip() if sku_raw is not None else ''
                m_sku = re.search(r'(\d+)', sku_s) if sku_s else None
                if m_sku:
                    rec['ID'] = int(m_sku.group(1))
                else:
                    rec['ID'] = int(float(main_id)) * 1000
            else:
                rec['ID'] = abs(hash(f"{row_material}|{color_norm or ''}")) % 10 ** 9
        except Exception:
            rec['ID'] = abs(hash(f"{row_material}|{color_norm or ''}")) % 10 ** 9

        rec['Материал'] = row_material
        rec['Название'] = f'Материал: {row_material}'
        rec['Описание'] = f'Выбран материал: {row_material}' + (f" (цвет: {color_norm})" if color_norm else '')
        rec['Цена'] = 0

        img = row.get('Изображение материала') or row.get('Изображение') or ''
        img = img.strip() if isinstance(img, str) else ''
        rec['Изображение'] = img or None

        # Дедупликация по file_id/URL, чтобы одинаковые картинки не повторялись
        dedupe_key = rec['Изображение'] or f"{row_material}|{color_norm or ''}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        items.append(rec)

    items.sort(key=lambda x: x.get('ID') or 0)
    print(f"🔍 Найдено элементов материала: {len(items)}")
    return items



@retry_on_network_error()
def build_panties_type_items_for_slider(selected_material: str, panties_type: str, all_rows: Optional[list] = None) -> list:
    """
    Возвращает список моделей трусиков для выбранного материала и типа.

    Логика:
    - «Кружевные трусики»       -> модели, начинающиеся с "Кружевные трусики <тип>", БЕЗ вышивки
    - Любой материал с вышивкой -> только модели с вышивкой и нужным типом
    - Остальные материалы       -> точное совпадение Материал + Тип
    """
    print(f"🎯 ФУНКЦИЯ ВЫЗВАНА: material='{selected_material}', type='{panties_type}'")

    if all_rows is None:
        all_rows = _load_all_panties_rows()
    panties_data: list[dict] = []

    material = (selected_material or "").strip()
    material_lower = material.lower()
    type_norm = (panties_type or "").strip()
    type_lower = type_norm.lower()

    for row in all_rows:
        row_material = str(row.get("Материал", "") or "").strip()
        row_model = str(row.get("Модель", "") or "").strip()
        row_type = str(row.get("Тип", "") or "").strip()

        row_material_lower = row_material.lower()
        row_model_lower = row_model.lower()
        row_type_lower = row_type.lower()

        # Любая форма слова "вышивк..." (с вышивкой / из вышивки / вышивка и т.п.)
        is_embroidery = "вышивк" in row_model_lower
        if PANTIES_DEBUG:
            print(
                f"🔍 Анализ строки ID={row.get('ID')}: "
                f"Материал='{row_material}', Тип='{row_type}', Модель='{row_model}'"
            )

        add_row = False

        # ----------------------------
        # 1) КРУЖЕВНЫЕ ТРУСИКИ (БЕЗ ВЫШИВКИ)
        # ----------------------------
        if material_lower == "кружевные трусики":
            # отсеиваем всё, где есть вышивка
            if is_embroidery:
                print("   ⛔ Пропускаем: это модель с вышивкой, а выбран базовый кружевной материал")
                continue

            # берём модели, начинающиеся с "кружевные трусики <тип>"
            if row_model_lower.startswith(f"кружевные трусики {type_lower}"):
                add_row = True

        # ----------------------------
        # 2) ЛЮБЫЕ ТРУСИКИ С ВЫШИВКОЙ
        #    (трусики с вышивкой / из вышивки / материал трусиков: вышивка и т.п.)
        # ----------------------------
        elif "вышивк" in material_lower:
            # только модели с вышивкой
            if not is_embroidery:
                print("   ⛔ Пропускаем: это модель без вышивки, а выбран материал с вышивкой")
                continue

            # тип должен совпадать
            if row_type_lower == type_lower:
                add_row = True

        # ----------------------------
        # 3) ОСТАЛЬНЫЕ МАТЕРИАЛЫ (ХЛОПКОВЫЕ, СЕТКА И Т.П.)
        # ----------------------------
        else:
            if row_material_lower == material_lower and row_type_lower == type_lower:
                add_row = True

        if not add_row:
            continue

        print("✅ ДОБАВЛЯЕМ В РЕЗУЛЬТАТ")

        # Формируем запись
        rec: dict = {}
        try:
            rec["ID"] = int(float(row.get("ID")))
        except Exception:
            rec["ID"] = abs(hash(row_model_lower or row.get("Название", ""))) % 10**9

        rec["Материал"] = row_material or material
        rec["Тип"] = row_type
        rec["Модель"] = row_model
        rec["Название"] = row.get("Название") or row_model or f"{material} {type_norm}"
        rec["Описание"] = f"Модель: {row_model}"
        rec["Цена"] = row.get("Цена") or 2400
        rec["original_price"] = rec["Цена"]
        rec["Вариант посадки"] = row.get("Вариант посадки", "")
        rec["is_panties"] = True

        # Картинка
        img = row.get("Изображение модели") or row.get("Изображение") or ""
        if isinstance(img, str) and img.strip():
            if img.startswith(("http://", "https://")):
                rec["Изображение"] = img
            elif re.match(r"^[a-zA-Z0-9_-]{20,200}$", img):
                rec["Изображение"] = f"https://drive.google.com/uc?export=view&id={img}"
            else:
                rec["Изображение"] = None
        else:
            rec["Изображение"] = None

        panties_data.append(rec)

    print(f"🔍 ИТОГО найдено записей: {len(panties_data)}")
    panties_data.sort(key=lambda x: (x.get("Модель") or "", x.get("ID") or 0))
    return panties_data






@dp.message(LingerieSet.PantiesView, F.text == "Выбрать еще трусики")
@retry_on_network_error()
async def lingerie_set_choose_more_panties_from_view(message: Message, state: FSMContext):
    # Возвращаемся к выбору типа трусиков внутри сценария "Комплект белья"
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите тип трусиков:", reply_markup=_lingerie_set_panties_type_kb())
    await state.set_state(LingerieSet.PantiesType)


@dp.message(LingerieSet.PantiesView, F.text == "Перейти в корзину")
@retry_on_network_error()
async def lingerie_set_go_cart_from_view(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.PantiesView, F.text == "Каталог товаров")
@retry_on_network_error()
async def lingerie_set_catalog_from_view(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.OrderMenu, F.text == 'Трусики')
@retry_on_network_error()
async def show_panties_menu(message: Message, state: FSMContext):
    """Показывает меню выбора материала трусиков"""
    promo_settings = get_promo_settings()
    if promo_settings.get('PANTIES_PROMO_ACTIVE', True):
        promo_text = f"*{promo_settings.get('PANTIES_PROMO_TEXT', '🖤 АКЦИЯ НА ТРУСИКИ!')}*\n\nПри покупке {promo_settings.get('PANTIES_PROMO_COUNT', 3)}х любых трусиков - фиксированная цена {promo_settings.get('PANTIES_PROMO_PRICE', 6500)} рублей!\n\n*Исключение:* хлопковые шорты не участвуют в акции.\n\nАкция распространяется на любые модели трусиков кроме хлопковых шорт. При добавлении в корзину нужного количества трусиков, они автоматически будут учтены по акционной цене."
        await message.answer(promo_text)
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Хлопковые трусики'), KeyboardButton(text='Трусики из эластичной сетки')], [KeyboardButton(text='Кружевные трусики'), KeyboardButton(text='Трусики с вышивкой')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer('Пожалуйста, выберите материал трусиков:', reply_markup=kb)
    await state.set_state(Order.PantiesMaterial)
    await state.update_data(current_category='panties_material')


@dp.message(Order.PantiesMaterial)
@retry_on_network_error()
async def handle_panties_material(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if text == 'Назад':
        await delete_previous_slider(message.chat.id, state)
        await make_order(message, state)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return
    # Если пользователь нажал кнопку цвета, находясь в PantiesMaterial,
    # просто перерисовываем слайдер для уже выбранного материала, не заставляя выбирать материал заново.
    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text in allowed_colors:
        data = await state.get_data()
        selected_material = (data.get('selected_material') or '').strip()
        selected_color = text
        if not selected_material:
            await message.answer('Сначала выберите материал трусиков.')
            return

        print(f"🎨 СМЕНА ЦВЕТА: материал='{selected_material}', цвет='{selected_color}'")
        rows = await _get_panties_rows_cached(state)
        material_items = build_panties_material_items_for_slider(selected_material, color=selected_color, all_rows=rows)
        # при смене цвета убираем старый слайдер (так быстрее и чище в UI)
        await delete_previous_slider(message.chat.id, state)
        if not material_items:
            await message.answer(f"К сожалению, материалы '{selected_material}' цвета '{selected_color}' временно недоступны.")
            return

        await state.update_data(
            items=material_items,
            current_index=0,
            current_category='panties_material',
            selected_material=selected_material,
            selected_color=selected_color
        )
        await show_item_slider(message.chat.id, state, material_items, 0, f'Материалы: {selected_material} ({selected_color})')
        # остаёмся в PantiesMaterial
        await state.set_state(Order.PantiesMaterial)
        return


    allowed = {
        'Хлопковые трусики': 'Хлопковые трусики',
        'Трусики из эластичной сетки': 'Трусики из эластичной сетки',
        'Кружевные трусики': 'Кружевные трусики',
        'Трусики с вышивкой': 'Материал трусиков: Вышивка'
    }

    if text not in allowed:
        await message.answer('Пожалуйста, выберите материал из предложенных кнопок.')
        return

    selected_material = allowed[text]
    print(f"🔍 ПОЛЬЗОВАТЕЛЬ ВЫБРАЛ: '{text}' -> преобразуется в '{selected_material}'")

    # Сначала спрашиваем цвет, затем открываем слайдер материалов (уже с фильтром по цвету)
    color_kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await state.update_data(selected_material=selected_material, current_category='panties_material')
    await message.answer('Выберите цвет материала:', reply_markup=color_kb)
    await state.set_state(Order.PantiesColor)



@dp.message(Order.PantiesColor)
@retry_on_network_error()
async def handle_panties_color(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if text == 'Назад':
        # убрать слайдер и вернуться к выбору материала трусиков
        await delete_previous_slider(message.chat.id, state)
        await show_panties_menu(message, state)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return

    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text not in allowed_colors:
        await message.answer('Пожалуйста, выберите цвет из предложенных кнопок.')
        return

    data = await state.get_data()
    selected_material = data.get('selected_material', '')
    selected_color = text
    await state.update_data(selected_color=selected_color)
    remember_user_color(message.from_user.id, selected_color)
    print(f"🎨 ВЫБРАН ЦВЕТ: материал='{selected_material}', цвет='{selected_color}'")

    rows = await _get_panties_rows_cached(state)
    material_items = build_panties_material_items_for_slider(selected_material, color=selected_color, all_rows=rows)

    if not material_items:
        await message.answer(f"К сожалению, материалы '{selected_material}' цвета '{selected_color}' временно недоступны.")
        return

    await state.update_data(
        items=material_items,
        current_index=0,
        current_category='panties_material',
        selected_material=selected_material,
        selected_color=selected_color
    )
    await show_item_slider(message.chat.id, state, material_items, 0, f'Материалы: {selected_material} ({selected_color})')

    # Важно: слайдер материалов работает в состоянии PantiesMaterial (там callbacks add_to_cart_)
    await state.set_state(Order.PantiesMaterial)



@dp.callback_query(Order.PantiesMaterial, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_panties_material_to_cart(call: CallbackQuery, state: FSMContext):
    """Добавляет материал трусиков в состояние (не в корзину) и переходит к выбору типа"""
    try:
        item_id = int(call.data.split('_')[3])
    except Exception:
        await call.answer('Ошибка добавления', show_alert=True)
        return
    data = await state.get_data()
    items = data.get('items', [])
    item = next((x for x in items if x.get('ID') == item_id), None)
    if not item:
        await call.answer('Материал не найден', show_alert=True)
        return
    selected_material = item.get('Материал') or data.get('selected_material') or ''
    await state.update_data(selected_panties_material=item, selected_material=selected_material)
    await delete_previous_slider(call.message.chat.id, state)
    await call.message.answer(f'✅ Материал *{escape_markdown(selected_material)}* выбран!',
                              parse_mode=ParseMode.MARKDOWN)

    # ИСПОЛЬЗУЕМ УМНУЮ КЛАВИАТУРУ
    kb = get_panties_type_keyboard(selected_material)
    await call.message.answer(f'Теперь выберите тип трусиков для материала *{escape_markdown(selected_material)}*:',
                              reply_markup=kb)
    await state.set_state(Order.PantiesType)


@dp.message(Order.PantiesType)
@retry_on_network_error()
async def handle_panties_type(message: Message, state: FSMContext):
    """Обрабатывает выбор типа трусиков"""
    text = (message.text or '').strip()
    if text == 'Назад к материалам':
        await go_back_with_slider_cleanup(message, state, show_panties_menu)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return

    allowed = {'Стринги': 'Стринги', 'Бразильянки': 'Бразильянки', 'Классика': 'Классика', 'Шорты': 'Шорты'}
    if text not in allowed:
        await message.answer('Пожалуйста, выберите тип из предложенных кнопок.')
        return

    selected_type = allowed[text]
    data = await state.get_data()
    selected_material = data.get('selected_material', '')
    print(f"🔍 ВЫБРАН ТИП: материал='{selected_material}', тип='{selected_type}'")

    # ОПРЕДЕЛЯЕМ ДОСТУПНЫЕ ТИПЫ ДЛЯ КАЖДОГО МАТЕРИАЛА
    available_types = {
        'Хлопковые трусики': ['Стринги', 'Бразильянки', 'Шорты'],
        'Трусики из эластичной сетки': ['Стринги', 'Бразильянки', 'Классика'],
        'Кружевные трусики': ['Стринги', 'Бразильянки', 'Классика'],
        'Материал трусиков: Вышивка': ['Стринги', 'Бразильянки', 'Классика']
    }

    material_types = available_types.get(selected_material, [])
    if selected_type not in material_types:
        await message.answer(
            f"Тип '{selected_type}' не доступен для материала '{selected_material}'. Выберите другой тип.")
        return

    print(f"🔍 ВЫЗЫВАЕМ build_panties_type_items_for_slider...")
    rows = await _get_panties_rows_cached(state)
    type_items = build_panties_type_items_for_slider(selected_material, selected_type, all_rows=rows)
    print(f'🔍 Найдено товаров: {len(type_items)}')

    if not type_items:
        await message.answer(
            f"К сожалению, трусики типа '{selected_type}' для материала '{selected_material}' временно недоступны.")
        return

    await state.update_data(items=type_items, current_index=0, current_category='panties_type',
                            selected_type=selected_type)
    await delete_previous_slider(message.chat.id, state)
    print(f"🔍 ПОКАЗЫВАЕМ СЛАЙДЕР С {len(type_items)} ТОВАРАМИ")
    await show_item_slider(message.chat.id, state, type_items, 0, f'Трусики: {selected_material} - {selected_type}')

@dp.callback_query(Order.PantiesType, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_panties_type_to_cart(call: CallbackQuery, state: FSMContext):
    """Добавляет объединенный товар (материал + модель) в корзину"""
    try:
        item_id = int(call.data.split('_')[3])
    except Exception:
        await call.answer('Ошибка добавления', show_alert=True)
        return
    data = await state.get_data()
    items = data.get('items', []) or []
    model_item = next((x for x in items if x.get('ID') == item_id), None)
    if not model_item:
        await call.answer('Трусики не найдены', show_alert=True)
        return
    material_item = data.get('selected_panties_material')
    if not material_item:
        await call.answer('❌ Сначала выберите материал трусиков', show_alert=True)
        return
    selected_color = (data.get('selected_color') or '').strip()
    combined_item = {'ID': model_item.get('ID'), 'Название': model_item.get('Название', ''), 'Цена': model_item.get('Цена', 2400), 'Тип': model_item.get('Тип', ''), 'Модель': model_item.get('Модель', ''), 'Материал': material_item.get('Материал', ''), 'Материал_ID': material_item.get('ID'), 'is_panties': True, 'original_price': safe_convert_price(model_item.get('Цена', 2400)), 'quantity': 1, 'Вариант посадки': model_item.get('Вариант посадки', ''), 'Цвет': selected_color}
    fit_options = model_item.get('Вариант посадки', '').strip()
    print(f"🔍 Варианты посадки для товара: '{fit_options}'")
    if fit_options:
        await state.update_data(selected_combined_item=combined_item)
        print('🔄 Переход к выбору посадки...')
        await ask_fit_option(call.message, combined_item, state)
    else:
        add_item_to_cart(call.from_user.id, combined_item)
        await call.answer(f"Трусики '{combined_item.get('Название')}' добавлены в корзину", show_alert=False)
        await delete_previous_slider(call.message.chat.id, state)
        await call.message.answer(f"✅ *{escape_markdown(combined_item.get('Название', 'Трусики'))}* добавлены в вашу корзину!\n📝 Материал: {escape_markdown(combined_item.get('Материал', ''))}", parse_mode=ParseMode.MARKDOWN)
        kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Выбрать еще трусики'), KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Оформить заказ99'), KeyboardButton(text='Каталог товаров')]])
        await call.message.answer('Вы можете выбрать еще трусики или перейти в корзину.', reply_markup=kb)
        await state.set_state(Order.PantiesView)


# ↓↓↓ ВСТАВЬТЕ ЗДЕСЬ ↓↓↓
def validate_stock_belts_order(cart):
    """Проверяет корректность заказа поясов для чулок"""
    print("🔍 ВАЛИДАЦИЯ ЗАКАЗА ПОЯСОВ:")

    # Находим все пояса в корзине (объединенные и обычные)
    stock_belts_items = [item for item in cart if
                         ('пояс' in str(item.get('Модель', '')).lower() and
                          'чулок' in str(item.get('Модель', '')).lower()) or
                         item.get('is_stock_belt')]

    print(f"🔍 Найдено поясов в корзине: {len(stock_belts_items)}")

    # Если в корзине нет поясов - проверка пройдена
    if not stock_belts_items:
        print("🔍 В корзине нет поясов - проверка пройдена")
        return (True, 'OK')

    # Проверяем каждый пояс
    for belt_item in stock_belts_items:
        # Для объединенных поясов проверяем наличие материала
        if belt_item.get('is_stock_belt'):
            if not belt_item.get('Материал') or not belt_item.get('Материал_ID'):
                print(f"❌ Объединенный пояс без материала: {belt_item.get('Название')}")
                return (False, f'❌ Ошибка в товаре "{belt_item.get("Название")}" - отсутствует материал')
            else:
                print(f"✅ Объединенный пояс корректен: {belt_item.get('Название')}")

    print("✅ Все пояса корректны")
    return (True, 'OK')
def validate_panties_order(cart):
    """Проверяет корректность заказа трусиков"""
    panties_items = [item for item in cart if item.get('is_panties')]
    return (True, 'OK')
    materials = [item for item in panties_items if item.get('Материал') and (not item.get('Модель')) and any((mat in str(item.get('Материал', '')).lower() for mat in ['хлопковые', 'кружевные', 'эластичной сетки']))]
    models = [item for item in panties_items if 'трусики' in str(item.get('Тип', '')).lower() or 'трусики' in str(item.get('Категория', '')).lower() or 'трусики' in str(item.get('Модель', '')).lower() or ('трусики' in str(item.get('Название', '')).lower())]
    if len(materials) > 1:
        return (False, '❌ В корзине не может быть больше одного материала трусиков')
    if len(materials) == 1 and len(models) == 0:
        return (False, '❌ Выбран материал трусиков, но не выбрана модель')
    if len(materials) == 0 and len(models) >= 1:
        return (False, '❌ Выбрана модель трусиков, но не выбран материал')
    return (True, 'OK')

@dp.message(Order.PantiesView, F.text == 'Назад к материалам')
@retry_on_network_error()
async def back_to_panties_materials_from_view(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, show_panties_menu)


@dp.message(Order.PantiesView, F.text == 'Выбрать еще трусики')
@retry_on_network_error()
async def back_to_panties_types_from_view(message: Message, state: FSMContext):
    data = await state.get_data()
    selected_material = data.get('selected_material', '')

    if selected_material:
        # ИСПОЛЬЗУЕМ УМНУЮ КЛАВИАТУРУ С ПРАВИЛЬНЫМИ ТИПАМИ
        kb = get_panties_type_keyboard(selected_material)
        await message.answer(f'Выберите тип трусиков для материала *{escape_markdown(selected_material)}*:',
                             reply_markup=kb)
        await state.set_state(Order.PantiesType)
    else:
        await show_panties_menu(message, state)

@dp.message(Order.PantiesView, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_panties_view(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.PantiesView, F.text == 'Оформить заказ')
@retry_on_network_error()
async def checkout_from_panties_view(message: Message, state: FSMContext):
    await start_checkout(message, state)

@dp.message(Order.PantiesView, F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_panties_view(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.PantiesMaterial, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_panties(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, make_order)

@dp.message(Order.PantiesType, F.text == 'Назад к материалам')
@retry_on_network_error()
async def back_to_panties_materials(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, show_panties_menu)

@dp.message(Order.PantiesMaterial, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_panties_material(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.PantiesType, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_panties_type(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(Order.BustMaterial)
@retry_on_network_error()
async def handle_bust_material(message: Message, state: FSMContext):
    text = (message.text or '').strip()

    # 🔁 Смена цвета уже после открытия слайдера (аналогично трусикам)
    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text in allowed_colors:
        data = await state.get_data()
        selected_material = (data.get('selected_material') or '').strip()
        if not selected_material:
            await message.answer('Сначала выберите материал бюста.')
            return
        selected_color = text
        await state.update_data(bust_selected_color=selected_color)
        remember_user_color(message.from_user.id, selected_color)
        await delete_previous_slider(message.chat.id, state)
        material_items = build_material_items_for_slider(selected_material, color=selected_color)
        if not material_items:
            await message.answer(f"К сожалению, для цвета '{selected_color}' материалы временно недоступны.")
            return
        await state.update_data(
            items=material_items,
            current_index=0,
            current_category='bust_material',
            selected_material=selected_material
        )
        await show_item_slider(message.chat.id, state, material_items, 0, f'Материалы: {selected_material}')
        # оставляем клавиатуру цветов
        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
                [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
                [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
            ]
        )
        await message.answer('Цвет изменён. Листайте материалы:', reply_markup=kb)
        return

    if text == 'Назад':
        await delete_previous_slider(message.chat.id, state)
        await make_order(message, state)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return

    allowed = {
        'Материал бюста: Хлопковый': 'Материал бюста: Хлопковый',
        'Материал бюста: Эластичная сетка': 'Материал бюста: Эластичная сетка',
        'Материал бюста: Кружевной': 'Материал бюста: Кружевной',
        'Материал бюста: Вышивка': 'Материал бюста: Вышивка'
    }
    if text not in allowed:
        await message.answer('Выберите материал бюста из меню.')
        return

    selected_material = allowed[text]

    # 🎨 сначала спрашиваем цвет
    color_kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await state.update_data(selected_material=selected_material, current_category='bust_material')
    await message.answer('Выберите цвет материала:', reply_markup=color_kb)
    await state.set_state(Order.BustColor)



@dp.message(Order.BustColor)
@retry_on_network_error()
async def handle_bust_color(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if text == 'Назад':
        # убрать слайдер и вернуться к выбору материала бюста
        await delete_previous_slider(message.chat.id, state)
        await show_bust_menu(message, state)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return

    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text not in allowed_colors:
        await message.answer('Пожалуйста, выберите цвет кнопкой ниже.')
        return

    data = await state.get_data()
    selected_material = (data.get('selected_material') or '').strip()
    if not selected_material:
        await show_bust_menu(message, state)
        return

    selected_color = text
    await state.update_data(bust_selected_color=selected_color)
    remember_user_color(message.from_user.id, selected_color)

    material_items = build_material_items_for_slider(selected_material, color=selected_color)
    if not material_items:
        await message.answer(f"К сожалению, для цвета '{selected_color}' материалы временно недоступны.")
        return

    await state.update_data(
        items=material_items,
        current_index=0,
        current_category='bust_material',
        selected_material=selected_material
    )
    await show_item_slider(message.chat.id, state, material_items, 0, f'Материалы: {selected_material}')

    # возвращаемся в BustMaterial, чтобы inline-кнопки выбора материала работали как раньше
    await state.set_state(Order.BustMaterial)

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await message.answer('Листайте материалы. Можно сменить цвет кнопками ниже:', reply_markup=kb)


@dp.callback_query(Order.BustMaterial, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_bust_material_to_cart(call: CallbackQuery, state: FSMContext):
    print("🎯 Обработчик выбора материала бюста (без немедленного добавления в корзину)")
    try:
        item_id = int(call.data.split('_')[3])
        print(f"🎯 ID материала: {item_id}")
    except Exception as e:
        print(f"❌ Ошибка парсинга ID: {e}")
        await call.answer('Ошибка выбора материала', show_alert=True)
        return

    data = await state.get_data()
    items = data.get('items', []) or []
    print(f"🎯 Всего items в состоянии: {len(items)}")

    item = next((x for x in items if x.get('ID') == item_id), None)
    if not item:
        print("❌ Материал не найден в items")
        await call.answer('Материал не найден', show_alert=True)
        return

    material_name = item.get('Материал') or data.get('selected_material') or ''
    print(f"🎯 Найден материал: {material_name}")

    # ❗ НЕ добавляем в корзину, только запоминаем во временное состояние
    # ✅ Важно: сохраняем КОПИЮ dict, чтобы флаги (is_panties и т.п.) не "протекали" между ветками
    safe_item = dict(item or {})
    safe_item['is_panties'] = False
    safe_item['is_stock_belt'] = False
    safe_item['is_certificate'] = safe_item.get('is_certificate', False)

    await state.update_data(
        pending_bust_material=safe_item,           # копия записи материала

        selected_material_item=item,
        selected_material=material_name
    )

    # Просто подтверждаем выбор (без фразы "добавлен в корзину")
    await call.answer(f"Материал '{material_name}' выбран", show_alert=False)

    # Убираем старый слайдер
    await delete_previous_slider(call.message.chat.id, state)

    await call.message.answer(
        f'✅ Материал *{escape_markdown(material_name)}* выбран.\n'
        f'Теперь давайте подберём модель бюста 💕',
        parse_mode=ParseMode.MARKDOWN
    )

    # Загружаем модели для выбранного материала
    print("🎯 Загружаем модели бюста...")
    model_items = build_model_items_for_slider(material_name)
    print(f"🎯 Найдено моделей: {len(model_items)}")

    if not model_items:
        print("❌ Модели не найдены")
        kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
            [KeyboardButton(text='Назад к материалам'), KeyboardButton(text='Перейти в корзину')],
            [KeyboardButton(text='Каталог товаров')]
        ])
        await call.message.answer(
            f'К сожалению, модели для материала *{escape_markdown(material_name)}* временно недоступны.',
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(Order.BustMaterial)
        return

    print("🎯 Обновляем состояние и показываем слайдер моделей")
    await state.update_data(
        items=model_items,
        current_index=0,
        current_category='bust_model'
    )

    await show_item_slider(call.message.chat.id, state, model_items, 0, f'Модели для {material_name}')

    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text='Назад к материалам')],
        [KeyboardButton(text='Каталог товаров')]
    ])
    await call.message.answer(
        'Теперь выберите модель бюста:',
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )

    await state.set_state(Order.BustModel)



@dp.callback_query(Order.BustModel, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_bust_model_to_cart(call: CallbackQuery, state: FSMContext):
    print("🎯 Обработчик добавления модели бюста в корзину")
    try:
        item_id = int(call.data.split('_')[3])
        print(f"🎯 ID модели: {item_id}")
    except Exception as e:
        print(f"❌ Ошибка парсинга ID: {e}")
        await call.answer('Ошибка добавления', show_alert=True)
        return

    data = await state.get_data()
    items = data.get('items', []) or []
    pending_material = data.get('pending_bust_material')  # 🚩 то, что мы сохранили на шаге выбора материала

    item = next((x for x in items if x.get('ID') == item_id), None)
    if not item:
        print("❌ Модель не найдена")
        await call.answer('Модель не найдена', show_alert=True)
        return

    user_id = call.from_user.id
    cart = user_carts.get(user_id) or []
    material_added_now = False  # добавили/инкрементнули материал для текущей модели

    print(f"🎯 Проверка наличия материала бюста (pending или в корзине)...")
    print(f"🎯 Содержимое корзины: {[i.get('Название', 'ID: ' + str(i.get('ID'))) for i in cart]}")

    has_material = False
    material_in_cart = None

    # 1️⃣ СНАЧАЛА ПРОБУЕМ ВЗЯТЬ pending_bust_material
    if pending_material:
        print(f"✅ Найден pending материал: {pending_material.get('Материал')} (ID: {pending_material.get('ID')})")

        # Проверяем, нет ли уже такого материала в корзине
        # Всегда учитываем материал на каждую добавленную модель бюста:
        # если материал уже есть в корзине, add_item_to_cart увеличит quantity.
        pending_material_safe = dict(pending_material or {})
        pending_material_safe['is_panties'] = False
        pending_material_safe['is_stock_belt'] = False
        add_item_to_cart(user_id, pending_material_safe)
        material_added_now = True
        print("🛒 Материал бюста учтён (add/increment) из pending_bust_material")

        has_material = True
        material_in_cart = pending_material

        # Сбрасываем pending — он уже отработал
        await state.update_data(pending_bust_material=None)

    else:
        # 2️⃣ ФОЛБЭК: СТАРАЯ ЛОГИКА — ИЩЕМ МАТЕРИАЛ В КОРЗИНЕ
        for item_cart in cart:
            is_bust_material = (
                    item_cart.get('Материал') and
                    (not item_cart.get('Модель')) and
                    any((mat in str(item_cart.get('Материал', '')).lower() for mat in [
                        'материал бюста: хлопковый',
                        'материал бюста: кружевной',
                        'материал бюста: эластичная сетка',
                        'материал бюста: вышивка',
                        'хлопковый',
                        'кружевной',
                        'эластичной сетки',
                        'эластичная сетка',
                        'вышивка'
                    ])) and
                    ('бюст' in str(item_cart.get('Тип', '')).lower() or
                     'бюст' in str(item_cart.get('Категория', '')).lower() or
                     'материал:' in str(item_cart.get('Название', '')).lower())
            )

            if is_bust_material:
                has_material = True
                material_in_cart = item_cart
                print(f"✅ Найден материал бюста в корзине: {material_in_cart.get('Материал')} (ID: {material_in_cart.get('ID')})")
                break

    if not has_material or not material_in_cart:
        print("❌ В корзине нет подходящего материала бюста")
        await call.answer('❌ Сначала выберите материал бюста', show_alert=True)
        return

    # 🔹 (опционально) Проставим материал в самой модели, если его там нет
    if not item.get('Материал'):
        item['Материал'] = material_in_cart.get('Материал')

    # ✅ ВАЖНО: на каждую модель бюста должен приходиться "свой" материал.
    # Если материал уже лежит в корзине (pending уже сброшен/не используется),
    # то при добавлении очередной модели мы добавляем этот материал ещё раз (увеличиваем quantity),
    # чтобы:
    # 1) валидация (materials == models) проходила корректно,
    # 2) объединение в show_cart могло спарить каждую модель со своим материалом.
    if (not material_added_now) and material_in_cart:
        add_item_to_cart(user_id, material_in_cart)
        print("🛒 Добавили материал бюста ещё раз (quantity++) для соответствия модели")

    # Добавляем модель в корзину
    item["is_lingerie_set"] = True
    item["is_panties"] = False
    # Пробрасываем ID выбранного материала бюста (нужно для админа/Google Sheets)
    mat_src = material_in_cart or (data.get("selected_material_item") or {})
    if mat_src and not item.get("Материал_ID"):
        item["Материал_ID"] = mat_src.get("ID")
    add_item_to_cart(user_id, item)
    print(f"✅ Модель бюста добавлена в корзину: {item.get('Название')}")

    await call.answer(f"Модель '{item.get('Название')}' добавлена в корзину", show_alert=False)
    await delete_previous_slider(call.message.chat.id, state)

    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text='Перейти в корзину')],
        [KeyboardButton(text='Каталог товаров')]
    ])

    await call.message.answer(
        f"✅ *{escape_markdown(item.get('Название', 'Модель'))}* добавлена в вашу корзину!\n\n"
        f"Вы можете выбрать что-то ещё или перейти в корзину.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )

    await state.set_state(Order.BustView)


@retry_on_network_error()
def get_corsets():
    return load_data_from_master_cached(product_type='Корсет', cache_key='corsets')

@retry_on_network_error()
def get_corset_with_painting():
    return load_data_from_master_cached(product_type='Корсет', model='Корсет с картиной', cache_key='corset_painting')

@retry_on_network_error()
def get_corset_tapestry():
    return load_data_from_master_cached(product_type='Корсет', model='Корсет из полотен', cache_key='corset_tapestry')

@retry_on_network_error()
def get_corset_denim():
    return load_data_from_master_cached(product_type='Корсет', model='Корсет из джинсы', cache_key='corset_denim')

@retry_on_network_error()
def get_corset_mesh():
    return load_data_from_master_cached(product_type='Корсет', model='Корсет из корсетной сетки', cache_key='corset_mesh')

@retry_on_network_error()
def get_lingerie_sets():
    return load_data_from_master_cached(product_type='Комплект нижнего белья', cache_key='lingerie_sets')

@retry_on_network_error()
def get_accessories():
    return load_data_from_master_cached(product_type='Аксессуары', cache_key='accessories')

@retry_on_network_error()
def get_stock_belts():
    accessories = get_accessories()
    return [item for item in accessories if item.get('Модель') == 'Пояс для чулок']

@retry_on_network_error()
def get_lace_stock_belts():
    return load_data_from_master_cached(product_type='Аксессуары', model='Кружевной пояс для чулок', cache_key='lace_stock_belts')

@retry_on_network_error()
def get_mesh_stock_belts():
    return load_data_from_master_cached(product_type='Аксессуары', model='Пояс для чулок из эластичной сетки', cache_key='mesh_stock_belts')

@retry_on_network_error()
def get_other_accessories():
    accessories = get_accessories()
    items = [item for item in accessories if item.get('Модель') != 'Пояс для чулок']
    # FIX: другие аксессуары не должны получать "материал трусиков" и не участвуют в акции трусиков
    cleaned = []
    for it in items:
        it2 = dict(it)
        it2['is_panties'] = False
        it2.pop('promo_applied', None)
        cleaned.append(it2)
    return cleaned

@retry_on_network_error()
def get_sale_panties():
    return load_data_from_master_cached(product_type='Трусики по акции', cache_key='sale_panties')

@retry_on_network_error()
def get_certificates():
    return load_data_from_master_cached(product_type='Сертификат', cache_key='certificates')

@retry_on_network_error()
def get_busts():
    return load_data_from_master_cached(product_type='Бюст', cache_key='busts')


# Для корсетов продолжаем из функции get_bust
@dp.message(Order.Bust)
@retry_on_network_error()
async def get_bust(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(bust=message.text)

    data = await state.get_data()
    needed_measurements = set(data.get('needed_measurements', []))
    needed_measurements.discard('bust')

    if 'underbust' in needed_measurements:
        await message.answer('Введите обхват под грудью (в см):')
        await state.set_state(Order.Underbust)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        await proceed_to_order_notes(message, state)


# Продолжение для корсетов после под грудью
@dp.message(Order.Underbust)
@retry_on_network_error()
async def get_underbust(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(underbust=message.text)

    data = await state.get_data()
    needed_measurements = set(data.get('needed_measurements', []))
    needed_measurements.discard('underbust')

    if 'waist' in needed_measurements:
        await message.answer('Введите обхват талии (в см):')
        await state.set_state(Order.Waist)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        await proceed_to_order_notes(message, state)

@retry_on_network_error()
def get_measurements_guide():
    cached_data = data_cache.get('measurements_guide')
    if cached_data is not None:
        return cached_data
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        ws = client.open_by_key(SPREADSHEET_ID).worksheet('Catalog')
        all_data = ws.get_all_records()
        measurements_data = []
        for row in all_data:
            if row.get('Тип') == 'Замеры' or row.get('Категория') == 'Замеры':
                # Каноническое поле картинки: берём из любого подходящего столбца
                raw_photo = (
                    (row.get('Изображение') or '').strip() if isinstance(row.get('Изображение'), str) else ''
                )
                if not raw_photo:
                    raw_photo = (
                        (row.get('Изображение модели') or '').strip() if isinstance(row.get('Изображение модели'), str) else ''
                    )
                if not raw_photo:
                    raw_photo = (
                        (row.get('Изображение материала') or '').strip() if isinstance(row.get('Изображение материала'), str) else ''
                    )
                if not raw_photo:
                    raw_photo = (
                        (row.get('ModelPhotoId') or '').strip() if isinstance(row.get('ModelPhotoId'), str) else ''
                    )

                if raw_photo:
                    # если это не URL — считаем что это Google Drive file_id
                    if not raw_photo.startswith(('http://', 'https://')):
                        if re.match(r'^[a-zA-Z0-9_-]{20,}$', raw_photo):
                            raw_photo = f"https://drive.google.com/uc?export=view&id={raw_photo}"
                    row['Изображение'] = raw_photo
                else:
                    row['Изображение'] = None

                measurements_data.append(row)

        data_cache.set('measurements_guide', measurements_data)
        return measurements_data
    except Exception as e:
        print(f'Ошибка чтения данных мерки: {e}')
        return []

# --- Promotion settings (cached) ---
PROMO_SETTINGS: dict = {}
PROMO_SETTINGS_LOADED_AT: float | None = None

def get_promo_settings() -> dict:
    """Возвращает текущие настройки промо из памяти (без обращения к Google Sheets)."""
    if PROMO_SETTINGS:
        return PROMO_SETTINGS
    # fallback на дефолт
    return get_default_promotion_settings()

def refresh_promo_settings_from_sheets() -> dict:
    """Принудительно перечитывает настройки промо из Google Sheets и обновляет PROMO_SETTINGS.
    Важно: функция синхронная (использовать через asyncio.to_thread в async-коде).
    """
    global PROMO_SETTINGS, PROMO_SETTINGS_LOADED_AT
    settings = _fetch_promotion_settings_from_sheets()
    PROMO_SETTINGS = settings
    PROMO_SETTINGS_LOADED_AT = time.time()
    # Положим также в data_cache для совместимости (если где-то ещё используется)
    try:
        data_cache.set('promotion_settings', settings, ttl=24*3600)  # сутки
    except Exception:
        pass
    return settings

def _fetch_promotion_settings_from_sheets() -> dict:
    """Читает настройки промо из листа 'Настройки' в Google Sheets.
    При ошибке возвращает дефолт.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        try:
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet('Настройки')
            settings_data = worksheet.get_all_records()
            settings: dict = {}
            for row in settings_data:
                key = row.get('Параметр', '')
                value = row.get('Значение', '')
                if not key or value is None:
                    continue
                if key in ('PANTIES_PROMO_ACTIVE',):
                    if isinstance(value, str):
                        value = value.strip().upper() in ('TRUE', 'ДА', 'YES', '1')
                    else:
                        value = bool(value)
                if key in ('PANTIES_PROMO_PRICE', 'PANTIES_PROMO_COUNT'):
                    try:
                        value = int(value)
                    except Exception:
                        pass
                settings[key] = value
            return settings or get_default_promotion_settings()
        except gspread.WorksheetNotFound:
            return get_default_promotion_settings()
    except Exception as e:
        print(f'Ошибка загрузки настроек акции: {e}')
        return get_default_promotion_settings()

def load_promotion_settings():
    """Совместимость: ранее здесь было чтение Google Sheets. Теперь возвращаем кэш в памяти.
    Для принудительного обновления используйте refresh_promo_settings_from_sheets() (через админ-панель).
    """
    return get_promo_settings()

def get_default_promotion_settings():
    return {'PANTIES_PROMO_PRICE': 6500, 'PANTIES_PROMO_COUNT': 3, 'PANTIES_PROMO_ACTIVE': True, 'PANTIES_PROMO_TEXT': '🖤 АКЦИЯ! 3 трусика за 6000 руб'}


def apply_panties_promotion(user_id: int):
    cart = user_carts.get(user_id)
    if not cart:
        return

    promo_settings = get_promo_settings()
    if not promo_settings.get('PANTIES_PROMO_ACTIVE', True):
        for item in cart:
            if item.get('is_panties') and 'promo_applied' in item:
                del item['promo_applied']
            if item.get('is_panties') and 'promo_unit_price' in item:
                del item['promo_unit_price']
            if item.get('is_panties') and 'promo_unit_price' in item:
                del item['promo_unit_price']
        user_carts.set(user_id, cart)
        return

    # ИСКЛЮЧАЕМ ХЛОПКОВЫЕ ШОРТЫ ИЗ АКЦИИ, НО ВКЛЮЧАЕМ ВЫШИВКУ
    panties_items = [item for item in cart if item.get('is_panties')]

    # Фильтруем только товары, участвующие в акции (исключаем хлопковые шорты, но включаем вышивку)
    eligible_panties = []
    for item in panties_items:
        # Проверяем, является ли товар хлопковыми шортами
        is_cotton_shorts = (
                'хлопковые' in str(item.get('Материал', '')).lower() and
                'шорты' in str(item.get('Модель', '')).lower()
        )

        # Проверяем, является ли товар трусиками с вышивкой
        is_embroidery = (
                'вышивка' in str(item.get('Материал', '')).lower() or
                'вышивка' in str(item.get('Модель', '')).lower() or
                'вышивка' in str(item.get('Название', '')).lower()
        )

        # Если это не хлопковые шорты ИЛИ это вышивка - добавляем в список для акции
        if not is_cotton_shorts or is_embroidery:
            eligible_panties.append(item)
            print(f"✅ Товар участвует в акции: {item.get('Название')} (Материал: {item.get('Материал')})")
        else:
            print(f"❌ Товар НЕ участвует в акции (хлопковые шорты): {item.get('Название')}")

    if not eligible_panties:
        # Сбрасываем акцию для всех товаров, если нет подходящих
        for item in panties_items:
            if 'promo_applied' in item:
                del item['promo_applied']
        user_carts.set(user_id, cart)
        return

    promo_price = promo_settings.get('PANTIES_PROMO_PRICE', 6500)
    promo_count = promo_settings.get('PANTIES_PROMO_COUNT', 3)
    promo_unit_price = int(promo_price // promo_count) if promo_count else 0
    total_eligible_count = sum((item['quantity'] for item in eligible_panties))

    if total_eligible_count < promo_count:
        # Сбрасываем акцию, если недостаточно товаров
        for item in panties_items:
            if 'promo_applied' in item:
                del item['promo_applied']
        user_carts.set(user_id, cart)
        return

    # Сортируем по цене для применения акции к самым дешевым
    # --- SAFETY: ensure original_price exists for all eligible panties (can be missing when added from other flows)
    for _p in eligible_panties:
        if 'original_price' not in _p:
            _p['original_price'] = _p.get('Цена') or _p.get('price') or _p.get('Цена (руб)') or 0
    eligible_panties_sorted = sorted(eligible_panties, key=lambda x: x.get('original_price', 0))
    promo_sets = total_eligible_count // promo_count
    remaining_panties = total_eligible_count % promo_count

    # Сбрасываем все предыдущие применения акции
    for item in panties_items:
        if 'promo_applied' in item:
            del item['promo_applied']
        if 'promo_unit_price' in item:
            del item['promo_unit_price']

    # Применяем акцию только к eligible товарам
    applied_count = 0
    for item in eligible_panties_sorted:
        if applied_count >= promo_sets * promo_count:
            break
        quantity_to_apply = min(item['quantity'], promo_sets * promo_count - applied_count)
        if quantity_to_apply > 0:
            item['promo_applied'] = quantity_to_apply
            item['promo_unit_price'] = promo_unit_price
            applied_count += quantity_to_apply
        else:
            # на всякий случай чистим, если раньше было применено
            if 'promo_unit_price' in item:
                del item['promo_unit_price']

    user_carts.set(user_id, cart)


def add_item_to_cart(user_id: int, item: dict):
    cart = user_carts.get(user_id)
    if not cart:
        cart = []
        user_carts.set(user_id, cart)

    print(f"🛒 Добавление в корзину: {item.get('Название')} (ID: {item.get('ID')})")
    print(f"🛒 Тип товара: {item.get('Тип', 'Не указан')}")
    print(f"🛒 Модель: {item.get('Модель', 'Не указана')}")
    print(f"🛒 Материал: {item.get('Материал', 'Не указан')}")

    is_certificate = item.get('is_certificate', False)
    is_panties = item.get('is_panties', False)


    # --- Нормализация ID модели/материала (важно для админа и Google Sheets) ---
    title = str(item.get('Название') or '')
    model_val = str(item.get('Модель') or '')
    is_material_marker = title.strip().lower().startswith('материал:') or (model_val.strip().lower() in ('не указана', '') and 'материал' in title.lower())

    # Для "материал"-позиций сохраняем ID как ID материала
    if is_material_marker and item.get('ID') is not None and not item.get('Материал_ID'):
        item['Материал_ID'] = item.get('ID')

    # Для обычных товаров, если нет Материал_ID — пытаемся вывести из уже добавленного материала в корзине
    if (not is_material_marker) and (not item.get('Материал_ID')):
        mat_name = str(item.get('Материал') or '').strip()
        if mat_name:
            for prev in reversed(cart):
                prev_title = str(prev.get('Название') or '')
                prev_model = str(prev.get('Модель') or '')
                prev_is_material = prev_title.strip().lower().startswith('материал:') or (prev_model.strip().lower() in ('не указана', '') and 'материал' in prev_title.lower())
                if prev_is_material and str(prev.get('Материал') or '').strip() == mat_name and prev.get('ID') is not None:
                    item['Материал_ID'] = prev.get('ID')
                    break
    # Подстраховка: если в item не проставили цвет в момент добавления,
    # пробуем взять последний выбранный цвет пользователя.
    if not (item.get('Цвет') or '').strip():
        last_color = (USER_LAST_COLOR.get(user_id) or '').strip()
        if last_color:
            # Цвет применяем только для категорий/веток, где у нас реально есть выбор цвета
            # (бюсты/трусики/комплекты/пояса для чулок).
            material_text = str(item.get('Материал', '') or '')
            cat_text = str(item.get('Категория', '') or '')
            type_text = str(item.get('Тип', '') or '')
            model_text = str(item.get('Модель', '') or '')
            likely_color_item = (
                is_panties
                or 'трус' in (type_text + cat_text).lower()
                or 'бюст' in (type_text + material_text + model_text).lower()
                or 'комплект' in (type_text + cat_text).lower()
                or ('пояс' in model_text.lower() and 'чулок' in model_text.lower())
            )
            if likely_color_item:
                item['Цвет'] = last_color

    # ДЛЯ МОДЕЛЕЙ ПОЯСОВ - ОБЪЕДИНЯЕМ С МАТЕРИАЛОМ
    is_stock_belt_model = (
            'пояс' in str(item.get('Модель', '')).lower() and
            'чулок' in str(item.get('Модель', '')).lower() and
            item.get('Тип') == 'Пояс для чулок'
    )

    if is_stock_belt_model:
        print("🛒 Это модель пояса - ищем материал в корзине для объединения")

        # Ищем соответствующий материал пояса в корзине (ТОЧНЫЙ ПОИСК)
        belt_material = None
        for cart_item in cart:
            is_belt_material = (
                    cart_item.get('Материал') and
                    (not cart_item.get('Модель')) and
                    (
                    ('материал пояса' in str(cart_item.get('Материал', '')).lower()) or cart_item.get('is_stock_belt_material')
            ) and
                    cart_item.get('Тип') in ['Аксессуары', 'Пояс для чулок']
            )

            if is_belt_material:
                belt_material = cart_item
                print(f"🛒 Найден материал пояса: {belt_material.get('Материал')} (ID: {belt_material.get('ID')})")
                break

        if belt_material:
            print(f"🛒 Объединяем модель '{item.get('Название')}' с материалом '{belt_material.get('Материал')}'")
            # Удаляем старый материал из корзины
            cart.remove(belt_material)

            # Создаем объединенный товар
            combined_item = {
                'ID': item.get('ID'),  # ID модели
                'Название': item.get('Название', ''),
                'Цена': item.get('Цена', 0),
                'Тип': item.get('Тип', ''),
                'Модель': item.get('Модель', ''),
                'Материал': belt_material.get('Материал', ''),
                'Материал_ID': belt_material.get('ID'),  # ID материала
                'Цвет': (str(item.get('Цвет') or belt_material.get('Цвет') or '')).strip(),
                'is_stock_belt': True,  # пометка что это объединенный пояс
                'quantity': 1
            }

            # Проверяем, есть ли уже такой объединенный товар в корзине
            existing_combined = next((x for x in cart if
                                      x.get('is_stock_belt') and
                                      x.get('ID') == combined_item['ID'] and
                                      x.get('Материал_ID') == combined_item['Материал_ID']), None)

            if existing_combined:
                existing_combined['quantity'] += 1
                print(f"🛒 Увеличиваем количество существующего объединенного пояса")
            else:
                cart.append(combined_item)
                print(f"🛒 Добавлен новый объединенный пояс")

            user_carts.set(user_id, cart)
            return
        else:
            print("⚠️ Материал пояса не найден, добавляем только модель")
            # Если материала нет, добавляем просто модель
            existing_item = next((x for x in cart if x.get('ID') == item.get('ID')), None)
            if existing_item:
                existing_item['quantity'] += 1
            else:
                item_with_quantity = item.copy()
                item_with_quantity['quantity'] = 1
                cart.append(item_with_quantity)
            user_carts.set(user_id, cart)
            return

    # МАТЕРИАЛЫ ПОЯСОВ - добавляем как обычно (они будут объединены позже с моделями)
    is_stock_belts_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any((mat in str(item.get('Материал', '')).lower() for mat in [
                'материал пояса: кружевной',
                'материал пояса: эластичная сетка',
                'кружевной',
                'эластичной сетки',
                'эластичная сетка',
                'сетка'
            ])) and
            ('аксессуар' in str(item.get('Тип', '')).lower() or
             'пояс' in str(item.get('Тип', '')).lower() or
             'пояс' in str(item.get('Категория', '')).lower())
    )

    # МАТЕРИАЛЫ БЮСТА - разрешаем несколько материалов
    is_bust_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any((mat in str(item.get('Материал', '')).lower() for mat in [
                'материал бюста: хлопковый',
                'материал бюста: кружевной',
                'материал бюста: эластичная сетка',
                'материал бюста: вышивка',
                'хлопковый',
                'кружевной',
                'эластичной сетки',
                'эластичная сетка',
                'вышивка'  # ДОБАВЛЕНО
            ])) and
            ('бюст' in str(item.get('Тип', '')).lower() or
             'бюст' in str(item.get('Категория', '')).lower() or
             'материал:' in str(item.get('Название', '')).lower())
    )

    if is_bust_material:
        print("🛒 Это материал бюста - РАЗРЕШАЕМ НЕСКОЛЬКО МАТЕРИАЛОВ")
        # ПРОСТО ДОБАВЛЯЕМ МАТЕРИАЛ БЕУД УДАЛЕНИЯ
        item_with_quantity = item.copy()
        item_with_quantity['quantity'] = 1
        cart.append(item_with_quantity)
        user_carts.set(user_id, cart)
        return
    elif is_panties:
        existing_item = next((x for x in cart if
                              x.get('is_panties') and
                              x.get('ID') == item.get('ID') and
                              (x.get('Посадка') == item.get('Посадка')) and
                              (x.get('Материал_ID') == item.get('Материал_ID'))), None)
        if existing_item:
            existing_item['quantity'] += 1
        else:
            item_with_quantity = item.copy()
            item_with_quantity['quantity'] = 1
            cart.append(item_with_quantity)
        user_carts.set(user_id, cart)
        apply_panties_promotion(user_id)
        return
    else:
        existing_item = next((x for x in cart if x.get('ID') == item.get('ID')), None)
        if existing_item:
            existing_item['quantity'] += 1
        else:
            item_with_quantity = item.copy()
            item_with_quantity['quantity'] = 1
            cart.append(item_with_quantity)
        user_carts.set(user_id, cart)


def calculate_cart_total(user_id: int):
    cart = user_carts.get(user_id)
    if not cart:
        return 0

    promo_settings = get_promo_settings()
    promo_price = promo_settings.get('PANTIES_PROMO_PRICE', 6500)
    promo_count = promo_settings.get('PANTIES_PROMO_COUNT', 3)

    total = 0

    # Фильтруем трусики, участвующие в акции (включая вышивку)
    panties_items = [item for item in cart if item.get('is_panties')]
    eligible_panties = []
    for item in panties_items:
        is_cotton_shorts = (
                'хлопковые' in str(item.get('Материал', '')).lower() and
                'шорты' in str(item.get('Модель', '')).lower()
        )

        is_embroidery = (
                'вышивка' in str(item.get('Материал', '')).lower() or
                'вышивка' in str(item.get('Модель', '')).lower() or
                'вышивка' in str(item.get('Название', '')).lower()
        )

        # Включаем в акцию все, кроме хлопковых шорт, но включаем вышивку
        if not is_cotton_shorts or is_embroidery:
            eligible_panties.append(item)

    total_eligible_count = sum((item['quantity'] for item in eligible_panties)) if eligible_panties else 0

    # ПРОВЕРЯЕМ УСЛОВИЯ АКЦИИ
    is_promo_applicable = (
            promo_settings.get('PANTIES_PROMO_ACTIVE', True) and
            len(eligible_panties) > 0 and
            total_eligible_count >= promo_count
    )

    if is_promo_applicable:
        promo_sets = total_eligible_count // promo_count
        remaining_panties = total_eligible_count % promo_count

        # Сумма за акционные наборы
        promo_total = promo_price * promo_sets

        # Сумма за оставшиеся трусики по обычной цене
        remaining_total = 0
        for item in eligible_panties:
            if 'promo_applied' in item:
                remaining_quantity = item['quantity'] - item['promo_applied']
                if remaining_quantity > 0:
                    remaining_total += item['original_price'] * remaining_quantity
            else:
                remaining_total += item['original_price'] * item['quantity']

        # Добавляем акционную сумму и сумму оставшихся трусиков
        total += promo_total + remaining_total

        # Добавляем стоимость всех остальных товаров (не участвующих в акции)
        for item in cart:
            if not item.get('is_panties') or item in [p for p in panties_items if p not in eligible_panties]:
                price = safe_convert_price(item.get('Цена', 0))
                quantity = item.get('quantity', 1)
                total += price * quantity
    else:
        # Если акция не применяется, считаем все по обычной цене
        for item in cart:
            price = safe_convert_price(item.get('Цена', 0))
            quantity = item.get('quantity', 1)
            total += price * quantity

    # Применяем сертификат
    applied_certificate = user_carts.get_applied_certificate(user_id)
    if applied_certificate and applied_certificate.get('valid'):
        total = max(total - applied_certificate['amount'], 0)

    return round(total)


def calculate_original_total(user_id: int):
    cart = user_carts.get(user_id)
    if not cart:
        return 0

    promo_settings = get_promo_settings()
    promo_price = promo_settings.get('PANTIES_PROMO_PRICE', 6500)
    promo_count = promo_settings.get('PANTIES_PROMO_COUNT', 3)

    total = 0

    # Фильтруем трусики, участвующие в акции (включая вышивку)
    panties_items = [item for item in cart if item.get('is_panties')]
    eligible_panties = []
    for item in panties_items:
        is_cotton_shorts = (
                'хлопковые' in str(item.get('Материал', '')).lower() and
                'шорты' in str(item.get('Модель', '')).lower()
        )

        is_embroidery = (
                'вышивка' in str(item.get('Материал', '')).lower() or
                'вышивка' in str(item.get('Модель', '')).lower() or
                'вышивка' in str(item.get('Название', '')).lower()
        )

        if not is_cotton_shorts or is_embroidery:
            eligible_panties.append(item)

    total_eligible_count = sum((item['quantity'] for item in eligible_panties)) if eligible_panties else 0

    # ПРОВЕРЯЕМ УСЛОВИЯ АКЦИИ
    is_promo_applicable = (
            promo_settings.get('PANTIES_PROMO_ACTIVE', True) and
            len(eligible_panties) > 0 and
            total_eligible_count >= promo_count
    )

    if is_promo_applicable:
        promo_sets = total_eligible_count // promo_count
        remaining_panties = total_eligible_count % promo_count

        # Сумма за акционные наборы
        promo_total = promo_price * promo_sets

        # Сумма за оставшиеся трусики по обычной цене
        remaining_total = 0
        for item in eligible_panties:
            if 'promo_applied' in item:
                remaining_quantity = item['quantity'] - item['promo_applied']
                if remaining_quantity > 0:
                    remaining_total += item['original_price'] * remaining_quantity
            else:
                remaining_total += item['original_price'] * item['quantity']

        # Добавляем акционную сумму и сумму оставшихся трусиков
        total += promo_total + remaining_total

        # Добавляем стоимость всех остальных товаров (не участвующих в акции)
        for item in cart:
            if not item.get('is_panties') or item in [p for p in panties_items if p not in eligible_panties]:
                price = safe_convert_price(item.get('Цена', 0))
                quantity = item.get('quantity', 1)
                total += price * quantity
    else:
        # Если акция не применяется, считаем все по обычной цене
        for item in cart:
            price = safe_convert_price(item.get('Цена', 0))
            quantity = item.get('quantity', 1)
            total += price * quantity

    return round(total)

@dp.message(Command('start', 'help'))
@retry_on_network_error()
async def cmd_start(message: Message, state: FSMContext):
    is_new_user = user_stats.add_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name
    )
    await state.clear()
    user_name = message.from_user.first_name

    if is_new_user and user_stats.should_send_notification():
        await send_stats_to_admin()
        user_stats.mark_notification_sent()

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Сделать заказ')],
            [KeyboardButton(text='Частые вопросы'), KeyboardButton(text='О боте')]
        ]
    )

    await message.answer(f'Привет, {user_name}!', reply_markup=kb)
    await state.set_state(Order.MainMenu)

@dp.message(F.text == 'О боте')
@retry_on_network_error()
async def about_bot(message: Message, state: FSMContext):
    text = (
        "🤖 <b>О боте</b>\n"
        "Этот  бот создан для структурированного оформления заказов и оптимизации внутренних процессов.\n\n"

        "👨‍💻 <b>Разработчик</b>\n"
        "Сергей — разработчик Telegram-ботов и решений для автоматизации бизнеса.\n"
        "Специализация: оптимизация рабочих процессов, каталогизация, обработка заказов, интеграции.\n\n"

        "📬 <b>Контакты</b>\n"
        "Telegram: <b>@fort1991</b>\n\n"

        "💼 <b>Стоимость разработки</b>\n"
        "• Индивидуальные проекты от <b>25 000 ₽</b>\n"
        "• Комплексные решения и автоматизация от <b>70 000 ₽</b>\n\n"

        "Все разработки выполняются по техническому заданию и соответствуют требованиям законодательства РФ.\n"
    )

    await message.answer(text, parse_mode=ParseMode.HTML)


@dp.message(F.text.in_(['Главное меню', 'В главное меню', '🏠 В главное меню']))
@retry_on_network_error()
async def global_main_menu(message: Message, state: FSMContext):
    await state.clear()
    await cmd_start(message, state)

@dp.message(Order.MainMenu, F.text == 'Сделать заказ')
@retry_on_network_error()
async def make_order(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Корсет'), KeyboardButton(text='Бюст')], [KeyboardButton(text='Аксессуары'), KeyboardButton(text='Трусики')], [KeyboardButton(text='Комплект белья')], [KeyboardButton(text='Сертификат')], [KeyboardButton(text='Главное меню'), KeyboardButton(text='Корзина')]])
    await message.answer('Выберите категорию товара:', reply_markup=kb)
    await state.set_state(Order.OrderMenu)

@dp.message(Order.MainMenu, F.text == 'Частые вопросы')
@retry_on_network_error()
async def show_faq(message: Message):
    faq_text = '❓ *Часто задаваемые вопросы:*\n\n1. *Как проходит процесс снятия мерок?*\nМерки вы снимаете самостоятельно по подробной инструкции, которая будет вам предложена на этапе оформлении заказа.\n\n2. *Сколько времени занимает пошив?*\nМаксимальный срок пошива до 15 дней, в среднем 5-7 дней в зависимости от сложности модели и загруженности мастерской. Подробную информацию о сроках вам предоставит менеджер после оформления заказа.\n\n3. *А если мне нужно заказ срочно?*\nНа такие случаи предусмотрен экспресс-пошив 1-3 дня, за дополнительную плату от +1500₽ к стоимости изделия. Подробную информацию о дополнительной стоимости вам предоставит менеджер после оформления заказа.\n\n4. *Можно ли подарить сертификат на индивидуальный пошив?*\nДа, можно оформить подарочный сертификат на любую сумму — получатель сам выберет модель и материалы.\n\n5. *Делаете ли вы бельё для особых случаев (свадьба, фотосессия и т. д.)?*\nКонечно, мы создаём эксклюзивные комплекты под конкретный образ или событие.\n\n6. *А если в каталоге товаров нет того, что я хочу?*\nЕсли вы не выбрали то, что может вам понравится, пожалуйста, напишите нашему менеджеру.\n\n7. *Что делать, если белье не село по размеру?*\nМы сожалеем, что так получилось, напишите нашему менеджеру.\n\n8. *Как оплачивается заказ?*\n100% предоплата переводом по реквизитам, по запросу менеджер вышлет вам онлайн чек.\n\n'
    contact_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='📞 Написать менеджеру', url='https://t.me/Natali_siali')], [InlineKeyboardButton(text='💬 Связаться через бота', callback_data='contact_admin')]])
    await message.answer(faq_text, reply_markup=contact_kb, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == 'contact_admin')
@retry_on_network_error()
async def contact_admin_handler(call: CallbackQuery):
    user_info = f'@{call.from_user.username}' if call.from_user.username else call.from_user.first_name
    await call.answer('✅ Администратор уведомлен! Он свяжется с вами в ближайшее время.', show_alert=True)
    admin_text = f'👤 *Новый запрос на связь!*\n\n*Пользователь:* {user_info}\n*ID:* {call.from_user.id}\n*Имя:* {call.from_user.full_name}\n\nПользователь хочет связаться с вами через кнопку в FAQ!'
    try:
        await bot.send_message(ADMIN_CHAT_ID, admin_text, parse_mode=ParseMode.MARKDOWN)
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='💌 Ответить пользователю', url=f'tg://user?id={call.from_user.id}')]])
        await bot.send_message(ADMIN_CHAT_ID, '📩 Нажмите кнопку ниже чтобы ответить:', reply_markup=admin_kb)
    except Exception as e:
        print(f'Ошибка отправки уведомления админу: {e}')
        await call.answer('❌ Ошибка связи. Попробуйте позже.', show_alert=True)

@dp.message(Order.MainMenu, F.text == 'Корзина')
@retry_on_network_error()
async def show_cart_menu(message: Message, state: FSMContext):
    await show_cart(message, state)


def combine_cart_for_display(cart, promo_settings=None):
    """
    Строит единый список товаров для отображения:
    - объединяет бюсты (модель + материал) -> is_combined_bust
    - объединяет пояса (модель + материал) -> is_combined_belt
    - помечает модели без материала -> missing_material = True
    - оставляет остальные товары как есть
    Ничего не форматирует в текст — только структура данных.
    """
    if promo_settings is None:
        try:
            promo_settings = get_promo_settings()
        except Exception:
            promo_settings = {}

    display_items = []
    bust_materials = []
    bust_models = []
    stock_belts_materials = []
    stock_belts_models = []

    # --- РАЗБИРАЕМ КОРЗИНУ НА ГРУППЫ ---
    for item in cart:
        material_val = str(item.get('Материал', '') or '').lower()
        model_val = str(item.get('Модель', '') or '').lower()
        type_val = str(item.get('Тип', '') or '').lower()
        category_val = str(item.get('Категория', '') or '').lower()
        title_val = str(item.get('Название', '') or '').lower()

        # Материалы бюста (отдельная строка, без Модели)
        is_bust_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any(mat in material_val for mat in [
                'материал бюста: хлопковый',
                'материал бюста: кружевной',
                'материал бюста: эластичная сетка',
                'материал бюста: вышивка',
                'хлопковый',
                'кружевной',
                'эластичной сетки',
                'эластичная сетка',
                'вышивк',
            ]) and
            any(term in type_val or term in category_val or term in title_val
                for term in ['бюст', 'материал:'])
        )

        # Модели бюста
        is_bust_model = (
            item.get('Модель') and any(
                'бюст' in val for val in [type_val, category_val, model_val, title_val]
            )
        )

        # Материалы поясов
        is_stock_belts_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any(mat in material_val for mat in [
                'материал пояса: кружевной',
                'материал пояса: эластичная сетка',
            ]) and
            str(item.get('Тип', '')).lower() == 'аксессуары'
        )

        # Модели поясов
        is_stock_belts_model = (
            item.get('Модель') and
            ('пояс' in model_val and 'чулок' in model_val) and
            str(item.get('Тип', '')).lower() == 'аксессуары'
        )

        if is_bust_material:
            bust_materials.append(item)
        elif is_bust_model:
            bust_models.append(item)
        elif is_stock_belts_material:
            stock_belts_materials.append(item)
        elif is_stock_belts_model:
            stock_belts_models.append(item)
        else:
            display_items.append(item)

    # --- ОБЪЕДИНЯЕМ БЮСТЫ (модель + материал) ---
    for model in bust_models:
        model_name = str(model.get('Модель', '') or '').lower()
        matched_material = None

        for material in bust_materials:
            material_id = material.get('ID')
            material_name = str(material.get('Материал', '') or '').lower()

            # Вышивка: и в модели, и в материале есть "вышивк"
            is_embroidery_match = (
                ('вышивк' in model_name) and ('вышивк' in material_name)
            )

            if (
                ('хлопковый' in model_name and 'хлопковый' in material_name) or
                ('кружевной' in model_name and 'кружевной' in material_name) or
                ('эластичной сетки' in model_name and 'эластичной сетки' in material_name) or
                ('эластичной сетки' in model_name and 'эластичная сетка' in material_name) or
                is_embroidery_match
            ):
                if material_id not in used_bust_materials:
                    matched_material = material
                    used_bust_materials.add(material_id)
                    break

        if matched_material:
            combined_item = dict(model)  # копия модели
            combined_item['is_combined_bust'] = True
            combined_item['Материал'] = matched_material.get('Материал')
            combined_item['Материал_ID'] = matched_material.get('ID')
            display_items.append(combined_item)
        else:
            # модели без соответствующего материала помечаем
            model['missing_material'] = True
            display_items.append(model)

    # --- ОБЪЕДИНЯЕМ ПОЯСА ДЛЯ ЧУЛОК ---
    used_belt_materials = set()
    for model in stock_belts_models:
        model_name = str(model.get('Модель', '') or '').lower()
        matched_material = None

        for material in stock_belts_materials:
            material_id = material.get('ID')
            material_name = str(material.get('Материал', '') or '').lower()

            if (
                ('кружевной' in model_name and 'кружевной' in material_name) or
                ('эластичной сетки' in model_name and 'эластичной сетки' in material_name) or
                ('эластичной сетки' in model_name and 'эластичная сетка' in material_name)
            ):
                if material_id not in used_belt_materials:
                    matched_material = material
                    used_belt_materials.add(material_id)
                    break

        if matched_material:
            combined_item = dict(model)
            combined_item['is_combined_belt'] = True
            combined_item['Материал'] = matched_material.get('Материал')
            combined_item['Материал_ID'] = matched_material.get('ID')
            display_items.append(combined_item)
        else:
            model['missing_material'] = True
            display_items.append(model)

    return display_items





@retry_on_network_error()
async def show_cart(message: Message, state: FSMContext):
    user_id = message.from_user.id
    cart = user_carts.get(user_id)
    if not cart:
        await message.answer('Ваша корзина пуста.')
        return

    # --- ОТПРАВКА ФОТО ИЗ КОРЗИНЫ ---
    try:
        # Загружаем все данные из таблицы
        all_rows = load_data_from_master_cached(cache_key='all_products_all_rows')
        if not all_rows:
            all_rows = _load_data_from_master_impl()

        print(f"🔍 Загружено строк из таблицы: {len(all_rows)}")

        # Собираем ВСЕ ID из корзины
        cart_model_ids = set()
        cart_material_ids = set()

        for item in cart:
            is_material_item = (
                    str(item.get('Название') or '').strip().startswith('Материал:') or
                    (
                        item.get('Материал') and
                        str(item.get('Модель') or '').strip() in ('', 'Не указана') and
                        any(mat in str(item.get('Материал', '')).lower() for mat in [
                            'материал бюста:', 'материал пояса:'
                        ])
                    )
            )

            if is_material_item:
                if item.get('ID'):
                    try:
                        material_id = int(float(item['ID']))
                        cart_material_ids.add(material_id)
                        print(f"🔍 Материал добавлен в material_ids: {material_id} - {item.get('Материал')}")
                    except (ValueError, TypeError):
                        pass
            else:
                if item.get('ID'):
                    try:
                        model_id = int(float(item['ID']))
                        cart_model_ids.add(model_id)
                        print(f"🔍 Модель добавлена в model_ids: {model_id} - {item.get('Название')}")
                    except (ValueError, TypeError):
                        pass

            if item.get('Материал_ID'):
                try:
                    material_id_from_field = int(float(item['Материал_ID']))
                    cart_material_ids.add(material_id_from_field)
                    print(f"🔍 Material_ID добавлен в material_ids: {material_id_from_field}")
                except (ValueError, TypeError):
                    pass

        print(f"🔍 ID моделей в корзине: {cart_model_ids}")
        print(f"🔍 ID материалов в корзине: {cart_material_ids}")

        # Ищем фото для каждого ID в корзине
        images_ordered = []
        seen_images = set()

        for row in all_rows:
            # Проверяем основной ID (для моделей)
            row_id = None
            try:
                if row.get('ID'):
                    row_id = int(float(row['ID']))
            except (ValueError, TypeError):
                continue

            # Если этот ID есть в корзине моделей - ищем фото модели
            if row_id and row_id in cart_model_ids:
                model_image = row.get('Изображение модели') or row.get('Изображение')
                if model_image:
                    if isinstance(model_image, str) and model_image.strip():
                        if model_image.startswith(('http://', 'https://')):
                            image_url = model_image
                        elif re.match('^[a-zA-Z0-9_-]{20,200}$', model_image.strip()):
                            image_url = f'https://drive.google.com/uc?export=view&id={model_image.strip()}'
                        else:
                            image_url = None

                        if image_url and image_url not in seen_images:
                            images_ordered.append(image_url)
                            seen_images.add(image_url)
                            print(f"✅ Найдено фото МОДЕЛИ для ID {row_id}: {image_url}")

            # Проверяем ID 2 (для материалов)
            row_id2 = None
            try:
                if row.get('ID 2'):
                    row_id2 = int(float(row['ID 2']))
            except (ValueError, TypeError):
                continue

            # Если этот ID 2 есть в корзине материалов - ищем фото материала
            if row_id2 and row_id2 in cart_material_ids:
                material_image = row.get('Изображение материала') or row.get('Изображение')
                if material_image:
                    if isinstance(material_image, str) and material_image.strip():
                        if material_image.startswith(('http://', 'https://')):
                            image_url = material_image
                        elif re.match('^[a-zA-Z0-9_-]{20,200}$', material_image.strip()):
                            image_url = f'https://drive.google.com/uc?export=view&id={material_image.strip()}'
                        else:
                            image_url = None

                        if image_url and image_url not in seen_images:
                            images_ordered.append(image_url)
                            seen_images.add(image_url)
                            print(f"✅ Найдено фото МАТЕРИАЛА для ID 2 {row_id2}: {image_url}")
                else:
                    print(f"⚠️ Для материала ID 2 {row_id2} не найдено изображение в строке ID {row_id}")

        # ДОПОЛНИТЕЛЬНЫЙ ПОИСК ДЛЯ МАТЕРИАЛОВ
        if cart_material_ids and len(images_ordered) == len(cart_model_ids):
            print(f"🔍 ДОПОЛНИТЕЛЬНЫЙ ПОИСК ДЛЯ МАТЕРИАЛОВ: {cart_material_ids}")
            for material_id in cart_material_ids:
                print(f"🔍 Ищем материал с ID 2 = {material_id}")
                for row in all_rows:
                    row_id2 = None
                    try:
                        if row.get('ID 2'):
                            row_id2 = int(float(row['ID 2']))
                    except (ValueError, TypeError):
                        continue

                    if row_id2 == material_id:
                        material_image = row.get('Изображение материала') or row.get('Изображение')
                        material_name = row.get('Материал', 'Неизвестно')
                        print(
                            f"🔍 Найдена строка для материала {material_id}: ID={row.get('ID')}, Материал='{material_name}', Изображение='{material_image}'")

                        if material_image:
                            if isinstance(material_image, str) and material_image.strip():
                                if material_image.startswith(('http://', 'https://')):
                                    image_url = material_image
                                elif re.match('^[a-zA-Z0-9_-]{20,200}$', material_image.strip()):
                                    image_url = f'https://drive.google.com/uc?export=view&id={material_image.strip()}'
                                else:
                                    image_url = None

                                if image_url and image_url not in seen_images:
                                    images_ordered.append(image_url)
                                    seen_images.add(image_url)
                                    print(
                                        f"✅ ДОПОЛНИТЕЛЬНО: Найдено фото МАТЕРИАЛА для ID 2 {material_id}: {image_url}")
                        break

        print(f"🔍 Всего найдено изображений: {len(images_ordered)}")

        # Сохраняем точный список картинок, чтобы отправить админу ТО ЖЕ САМОЕ
        try:
            await state.update_data(order_images=images_ordered)
        except Exception as _e:
            print(f"⚠️ Не удалось сохранить order_images в state: {_e}")

        # Отправляем изображения
        if images_ordered:
            # Заголовок (без MARKDOWN, чтобы не ловить ошибки форматирования)
            try:
                await message.answer("📸 Фото товаров из вашей корзины:")
            except Exception as _e:
                print(f"⚠️ Не удалось отправить заголовок фото: {_e}")

            async def _send_media_group_with_retries(_media_group, _max_attempts: int = 3) -> bool:
                for _attempt in range(1, _max_attempts + 1):
                    try:
                        await message.answer_media_group(_media_group)
                        return True
                    except Exception as _e:
                        print(f"❌ Ошибка отправки media_group (attempt {_attempt}/{_max_attempts}): {_e}")
                        if _attempt < _max_attempts:
                            await asyncio.sleep(1.0 * _attempt)
                return False

            for i in range(0, len(images_ordered), 10):
                batch = images_ordered[i:i + 10]
                media_group = []

                for j, image_url in enumerate(batch):
                    # Превращаем внешний URL (Drive) в Telegram file_id через канал-кэш
                    try:
                        media_id = await ensure_photo_in_channel(image_url)
                        if media_id:
                            if j == 0:
                                media_group.append(InputMediaPhoto(media=media_id))
                            else:
                                media_group.append(InputMediaPhoto(media=media_id))
                    except Exception as _e:
                        print(f"❌ Ошибка ensure_photo_in_channel для {image_url}: {_e}")

                if not media_group:
                    continue

                ok = await _send_media_group_with_retries(media_group, _max_attempts=3)
                if ok:
                    print(f"✅ Успешно отправлено {len(media_group)} фото")
                else:
                    # Фолбэк: пробуем отправить по одному (чаще проходит при временных сетевых сбоях)
                    for media in media_group:
                        sent = False
                        for _attempt in range(1, 4):
                            try:
                                await message.answer_photo(media.media)
                                sent = True
                                break
                            except Exception as _e:
                                print(f"❌ Ошибка отправки фото по одному (attempt {_attempt}/3): {_e}")
                                if _attempt < 3:
                                    await asyncio.sleep(1.0 * _attempt)
                        if not sent:
                            print("⚠️ Не удалось отправить одно из фото даже по одному.")
        else:
            print("⚠️ Не найдено изображений для товаров в корзине")

    except Exception as e:
        print(f'❌ Ошибка формирования фотоальбома корзины: {e}')
        import traceback
        traceback.print_exc()

    # --- ТЕКСТ КОРЗИНЫ ---
    print(f"🛒 СОДЕРЖИМОЕ КОРЗИНЫ ДЛЯ ПОЛЬЗОВАТЕЛЯ {user_id}:")
    for i, cart_item in enumerate(cart, 1):
        print(f"  {i}. Название: {cart_item.get('Название', 'Нет названия')}")
        print(f"     ID: {cart_item.get('ID')}")
        print(f"     Тип: {cart_item.get('Тип', 'Не указан')}")
        print(f"     Модель: {cart_item.get('Модель', 'Не указана')}")
        print(f"     Материал: {cart_item.get('Материал', 'Не указан')}")
        print(f"     is_panties: {cart_item.get('is_panties', False)}")
        print(f"     is_stock_belt: {cart_item.get('is_stock_belt', False)}")
        print(f"     ---")

    # Валидации с обработкой ошибок
    try:
        is_valid, error_msg = validate_bust_order(cart)
        if not is_valid:
            await message.answer(f'⚠️ {error_msg}')
    except Exception as e:
        print(f'❌ Ошибка валидации бюста: {e}')

    try:
        is_valid_panties, error_msg_panties = validate_panties_order(cart)
        if not is_valid_panties:
            await message.answer(f'⚠️ {error_msg_panties}')
    except Exception as e:
        print(f'❌ Ошибка валидации трусиков: {e}')

    try:
        is_valid_belts, error_msg_belts = validate_stock_belts_order(cart)
        if not is_valid_belts:
            await message.answer(f'⚠️ {error_msg_belts}')
    except Exception as e:
        print(f'❌ Ошибка валидации поясов: {e}')

    promo_settings = get_promo_settings()
    promo_price = promo_settings.get('PANTIES_PROMO_PRICE', 6500)
    promo_count = promo_settings.get('PANTIES_PROMO_COUNT', 3)
    original_total = calculate_original_total(user_id)
    total_amount = calculate_cart_total(user_id)
    applied_certificate = user_carts.get_applied_certificate(user_id)

    # -----------------------------
    # УПРОЩЕННЫЙ ВИЗУАЛ КОРЗИНЫ (UX)
    # Эмодзи по просьбе:
    # - 🖤 и 🔥 только для акции
    # - 💰 только для общей суммы
    # Остальные эмодзи в тексте корзины не используем.
    # -----------------------------

    def _human_price(v: float) -> str:
        try:
            vv = float(v)
        except Exception:
            vv = 0.0
        iv = int(vv)
        return str(iv) if abs(vv - iv) < 1e-9 else str(vv)

    # Считаем базовую сумму товаров БЕЗ скидок (для красивого итога)
    base_total = 0
    for it in cart:
        q = int(it.get('quantity', 1) or 1)
        if it.get('is_panties'):
            unit = it.get('original_price')
            if unit is None:
                unit = safe_convert_price(it.get('Цена', 0))
            base_total += safe_convert_price(unit) * q
        else:
            base_total += safe_convert_price(it.get('Цена', 0)) * q

    # Скидка по акции
    # Раньше считали поштучно через promo_unit_price (и иногда ловили расхождения из-за округления).
    # Сейчас считаем надежно: сумма товаров (по оригинальным ценам) - (к оплате + скидка сертификата).
    promo_discount = 0

    cert_discount = 0
    if applied_certificate and applied_certificate.get('valid'):
        try:
            cert_discount = int(applied_certificate.get('amount') or 0)
        except Exception:
            cert_discount = 0

    # Итоговая скидка по акции (без сертификата):
    # total_amount уже учитывает сертификат, поэтому добавляем cert_discount назад,
    # чтобы получить сумму "после акции, но до сертификата".
    try:
        promo_discount = int(round(max(0, float(base_total) - (float(total_amount) + float(cert_discount)))))
    except Exception:
        promo_discount = 0

    cart_text = '*Ваша корзина*\n\n'
    if applied_certificate and applied_certificate.get('valid'):
        cart_text += f"Сертификат применен: -{cert_discount} ₽\n\n"

    # ИСПРАВЛЕННАЯ ЛОГИКА АКЦИИ - ПРОВЕРЯЕМ УСЛОВИЯ ПЕРЕД ПОКАЗОМ
    panties_items = [item for item in cart if item.get('is_panties')]

    # Фильтруем только товары, участвующие в акции (исключаем хлопковые шорты)
    eligible_panties = []
    for item in panties_items:
        is_cotton_shorts = (
                'хлопковые' in str(item.get('Материал', '')).lower() and
                'шорты' in str(item.get('Модель', '')).lower()
        )
        if not is_cotton_shorts:
            eligible_panties.append(item)

    total_eligible_count = sum((item['quantity'] for item in eligible_panties)) if eligible_panties else 0

    # ПОКАЗЫВАЕМ АКЦИЮ ТОЛЬКО ЕСЛИ ВЫПОЛНЕНЫ ВСЕ УСЛОВИЯ:
    # 1. Акция активна
    # 2. Есть участвующие товары
    # 3. Общее количество >= promo_count
    show_promo_message = (
            promo_settings.get('PANTIES_PROMO_ACTIVE', True) and
            len(eligible_panties) > 0 and
            total_eligible_count >= promo_count
    )

    if show_promo_message:
        promo_sets = total_eligible_count // promo_count
        remaining_panties = total_eligible_count % promo_count

        cart_text += f"🖤 *АКЦИЯ:* {promo_count} трусика за {promo_price} ₽ (наборов: {promo_sets})\n"
        if remaining_panties > 0:
            cart_text += f"🔥 Дополнительно по обычной цене: {remaining_panties} шт.\n"
        cart_text += '\n'

    # Минимальный список товаров для клиента
    cart_text += build_user_order_items_minimal(cart) + "\n"

    # ИТОГОВАЯ СУММА
    cart_text += '──────────────\n'
    cart_text += f"💰 Сумма товаров: {_human_price(round(base_total))} ₽\n"
    if promo_discount > 0:
        cart_text += f"🔥 Скидка по акции: -{_human_price(promo_discount)} ₽\n"
    if cert_discount > 0:
        cart_text += f"Скидка по сертификату: -{_human_price(cert_discount)} ₽\n"
    cart_text += f"💰 К оплате: {_human_price(total_amount)} ₽"

    # КЛАВИАТУРА
    keyboard = []
    if applied_certificate and applied_certificate.get('valid'):
        keyboard.append([KeyboardButton(text='❌ Убрать сертификат')])
    else:
        keyboard.append([KeyboardButton(text='🎫 Применить сертификат')])
    keyboard.extend([[KeyboardButton(text='Оформить заказ'), KeyboardButton(text='Очистить корзину')],
                     [KeyboardButton(text='Главное меню'), KeyboardButton(text='Назад')]])
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=keyboard)

    try:
        await message.answer(cart_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f'Markdown error in cart: {e}')
        plain_text = re.sub('\\*([^*]+)\\*', '\\1', cart_text)
        plain_text = re.sub('🛒|🎫|💳|🖤|➕|💰|🆔|📏|👕|📝|📧|🏠|⚠️|❌|✅', '', plain_text)
        await message.answer(plain_text, reply_markup=kb)

    await state.set_state(Order.CartView)

@dp.message(Order.CartView, F.text == 'Очистить корзину')
@retry_on_network_error()
async def clear_cart(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user_carts.clear(user_id)
    await message.answer('✅ Корзина очищена.')
    await show_cart(message, state)

@dp.message(Order.CartView, F.text == '🎫 Применить сертификат')
@retry_on_network_error()
async def apply_certificate(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад в корзину')], [KeyboardButton(text='Главное меню')]])
    await message.answer('🎫 *Применение сертификата*\n\nВведите номер сертификата:', reply_markup=kb)
    await state.set_state(Order.ApplyCertificate)

@dp.message(Order.ApplyCertificate)
@retry_on_network_error()
async def process_certificate_input(message: Message, state: FSMContext):
    if message.text == '🔙 Назад в корзину':
        await show_cart(message, state)
        return
    elif message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    certificate_number = message.text.strip()
    validation_result = certificate_manager.validate_certificate(certificate_number)
    if validation_result['valid']:
        user_carts.set_applied_certificate(message.from_user.id, {'valid': True, 'amount': validation_result['amount'], 'number': certificate_number, 'message': validation_result['message']})
        await message.answer(f"✅ {validation_result['message']}\n\n💳 Скидка {validation_result['amount']} руб. применена к вашему заказу!")
        await show_cart(message, state)
    else:
        await message.answer(f"❌ {validation_result['message']}\n\nПожалуйста, проверьте номер сертификата и попробуйте снова:")

@dp.message(Order.CartView, F.text == '❌ Убрать сертификат')
@retry_on_network_error()
async def remove_certificate(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user_carts.clear_applied_certificate(user_id)
    await message.answer('✅ Сертификат удален из заказа.')
    await show_cart(message, state)

@dp.message(Order.CartView, F.text == 'Назад')
@retry_on_network_error()
async def cart_continue_shopping(message: Message, state: FSMContext):
    await make_order(message, state)

@retry_on_network_error()

@retry_on_network_error()
async def edit_item_slider_message(message: Message, state: FSMContext, items: list, idx: int, category_name: str):
    """Обновляет (редактирует) уже отправленный слайдер вместо удаления/отправки нового сообщения."""
    if not items or idx >= len(items):
        try:
            await message.edit_text('Товары временно недоступны.')
        except Exception:
            pass
        return

    item = items[idx]
    tot = len(items)

    data = await state.get_data()


    # микро-оптимизация: если тот же слайд уже отрисован (часто при двойных кликах) — не трогаем Telegram лишний раз
    _slide_key = f"{category_name}|{idx}|{(items[idx] or {}).get('ID')}"
    if data.get('_last_slider_render') == _slide_key:
        return
    await state.update_data(_last_slider_render=_slide_key)
    nav_buttons = []
    if tot > 1:
        nav_buttons = [
            InlineKeyboardButton(text='⬅️', callback_data=f'item_prev_{idx}'),
            InlineKeyboardButton(text=f'{idx + 1}/{tot}', callback_data='noop'),
            InlineKeyboardButton(text='➡️', callback_data=f'item_next_{idx}')
        ]

    action_buttons = [[InlineKeyboardButton(text='✅ Добавить в корзину', callback_data=f"add_to_cart_{item.get('ID')}")]]
    keyboard = []
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.extend(action_buttons)
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)

    # Режим карточки: по умолчанию mini, но если уже выбран материал/цвет — context
    mode = "mini"
    if (data.get("selected_color") or data.get("stock_belts_selected_color") or data.get("selected_material") or data.get("stockbelts_selected_material")):
        mode = "context"
    caption = format_item_caption(item, data, mode=mode)

    # Определяем картинку (логика как в show_item_slider)
    image_url = item.get('Изображение')
    current_category = data.get('current_category', '') or ''
    if 'corset' in current_category.lower() or 'корсет' in str(category_name).lower():
        model_image = item.get('Изображение модели')
        if model_image and isinstance(model_image, str) and model_image.strip():
            if model_image.startswith(('http://', 'https://')):
                image_url = model_image
            elif re.match('^[a-zA-Z0-9_-]{20,200}$', model_image.strip()):
                image_url = f'https://drive.google.com/uc?export=view&id={model_image.strip()}'

    media_obj = None
    try:
        if image_url and isinstance(image_url, str) and image_url.strip():
            file_id = await ensure_photo_in_channel(image_url)
            if file_id:
                media_obj = InputMediaPhoto(media=file_id, caption=caption, parse_mode=ParseMode.MARKDOWN)
            elif image_url.startswith(('http://', 'https://')):
                media_obj = InputMediaPhoto(media=URLInputFile(image_url), caption=caption, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f'⚠️ edit_item_slider_message: не удалось подготовить media: {e}')
        media_obj = None

    # Пытаемся обновить именно слайдер-сообщение (media/caption/text)
    try:
        if media_obj:
            await message.edit_media(media=media_obj, reply_markup=kb)
        else:
            # Если у сообщения есть фото — меняем подпись, иначе текст
            if getattr(message, 'photo', None):
                await message.edit_caption(caption=caption, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            else:
                await message.edit_text(caption, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await state.update_data(last_slider_message_id=message.message_id)
    except Exception as e:
        # Дубль того же контента — безопасно игнорируем
        if 'message is not modified' in str(e).lower():
            return
        print(f'⚠️ edit_item_slider_message: ошибка редактирования: {e}')
        # Фолбек: пробуем хотя бы обновить клавиатуру
        try:
            await message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

@retry_on_network_error()
async def delete_previous_slider(chat_id: int, state: FSMContext):
    """Удаляет предыдущее сообщение-слайдер (если оно было сохранено в FSM)."""
    data = await state.get_data()
    msg_id = data.get("last_slider_message_id")
    if not msg_id:
        return
    try:
        await bot.delete_message(chat_id, msg_id)
    except Exception:
        pass
    # Удаляем ключ, чтобы не пытаться удалять повторно
    data.pop("last_slider_message_id", None)
    await state.set_data(data)



async def go_back_with_slider_cleanup(message: Message, state: FSMContext, back_handler, *args, **kwargs):
    """Единый helper для кнопки 'Назад': удаляет последний слайдер и возвращает в предыдущее меню.

    back_handler — coroutine/function, который рисует нужное меню (например show_accessories_menu).
    """
    try:
        await delete_previous_slider(message.chat.id, state)
    except Exception:
        pass

    # Сбрасываем индекс слайдера, чтобы не залипать на старых данных
    try:
        data = await state.get_data()
        data.pop("slider_index", None)
        await state.set_data(data)
    except Exception:
        pass

    # Вызываем обработчик меню
    res = back_handler(message, state, *args, **kwargs)
    if hasattr(res, "__await__"):
        return await res
    return res


async def show_item_slider(chat_id: int, state: FSMContext, items: list, idx: int, category_name: str):
    """Отрисовывает карточку товара (слайдер) с защитой от двойной отрисовки/гонок."""
    lock = _slider_locks.setdefault(chat_id, asyncio.Lock())
    async with lock:
        cur_state = await state.get_state()
        print(f"🔍 show_item_slider: категория='{category_name}', cur_state='{cur_state}', items_count={len(items) if items else 0}, idx={idx}")
        if not items or idx >= len(items):
            await bot.send_message(chat_id, 'Товары временно недоступны.')
            return

        data = await state.get_data()
        last_id = data.get('last_slider_message_id')

        item = items[idx]
        tot = len(items)

        item_id = (item or {}).get('ID') or (item or {}).get('Id') or (item or {}).get('id')

        # Дедуп одного и того же кадра (часто при дублях апдейтов/кликах)
        _slide_key = f"{category_name}|{cur_state}|{idx}|{item_id}|{tot}"
        if data.get('_last_slider_render') == _slide_key and last_id:
            return
        await state.update_data(_last_slider_render=_slide_key)

        nav_buttons = []
        if tot > 1:
            nav_buttons = [
                InlineKeyboardButton(text='⬅️', callback_data=f'item_prev_{idx}'),
                InlineKeyboardButton(text=f'{idx + 1}/{tot}', callback_data='noop'),
                InlineKeyboardButton(text='➡️', callback_data=f'item_next_{idx}')
            ]
        action_buttons = [[InlineKeyboardButton(text='✅ Добавить в корзину', callback_data=f"add_to_cart_{item_id}")]]
        keyboard = []
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.extend(action_buttons)
        kb = InlineKeyboardMarkup(inline_keyboard=keyboard)

        # Режим карточки: по умолчанию mini, но если уже выбран материал/цвет — context
        mode = "mini"
        if (data.get("selected_color") or data.get("stock_belts_selected_color") or data.get("selected_material") or data.get("stockbelts_selected_material")):
            mode = "context"
        caption = format_item_caption(item, data, mode=mode)

        try:
            image_url = item.get('Изображение')
            current_category = (data.get('current_category', '') or '').lower()

            # Для корсетов может подменяться на "изображение модели"
            if 'corset' in current_category or 'корсет' in str(category_name).lower():
                model_image = item.get('Изображение модели')
                if model_image and isinstance(model_image, str) and model_image.strip():
                    if model_image.startswith(('http://', 'https://')):
                        image_url = model_image
                    elif re.match('^[a-zA-Z0-9_-]{20,200}$', model_image.strip()):
                        image_url = f'https://drive.google.com/uc?export=view&id={model_image.strip()}'

            file_id = None
            if image_url and isinstance(image_url, str) and image_url.strip():
                file_id = await ensure_photo_in_channel(image_url)

            if last_id:
                try:
                    if file_id:
                        media = InputMediaPhoto(media=file_id, caption=caption, parse_mode=ParseMode.MARKDOWN)
                        await bot.edit_message_media(chat_id=chat_id, message_id=last_id, media=media, reply_markup=kb)
                    else:
                        await bot.edit_message_text(caption, chat_id=chat_id, message_id=last_id, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                    return
                except Exception as e_edit:
                    if 'message is not modified' in str(e_edit).lower():
                        return
                    print(f"⚠️ Не удалось отредактировать слайдер, пересоздаю: {e_edit}")
                    try:
                        await state.update_data(last_slider_message_id=None)
                        last_id = None
                    except Exception:
                        pass

            await delete_previous_slider(chat_id, state)

            if file_id:
                message = await bot.send_photo(chat_id, file_id, caption=caption, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            elif image_url and isinstance(image_url, str) and image_url.strip() and image_url.startswith(('http://', 'https://')):
                message = await bot.send_photo(chat_id, URLInputFile(image_url.strip()), caption=caption, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            else:
                message = await bot.send_message(chat_id, caption, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

            await state.update_data(last_slider_message_id=message.message_id)

        except Exception as e:
            print(f'Ошибка отправки слайдера: {e}')
            try:
                simple_caption = f"{item.get('Название', '')}\n\nЦена: {item.get('Цена', 0)} ₽\nID: {item_id}"
                if last_id:
                    try:
                        await bot.edit_message_text(simple_caption, chat_id=chat_id, message_id=last_id, reply_markup=kb)
                        return
                    except Exception:
                        pass
                message = await bot.send_message(chat_id, simple_caption, reply_markup=kb)
                await state.update_data(last_slider_message_id=message.message_id)
            except Exception as e2:
                print(f'Ошибка fallback слайдера: {e2}')


async def bust_navigation_handler(call: CallbackQuery, state: FSMContext):
    """Обработчик навигации для материалов и моделей бюста"""
    data = await state.get_data()
    items = data.get('items', [])

    if not items:
        await call.answer('Товары недоступны')
        return

    # микро-оптимизация UX: убираем "часики" сразу
    try:
        await call.answer(cache_time=1)
    except Exception:
        pass

    try:
        if call.data.startswith('item_prev_'):
            idx = int(call.data.split('_')[2])
            new_idx = (idx - 1) % len(items)
        else:  # item_next_
            idx = int(call.data.split('_')[2])
            new_idx = (idx + 1) % len(items)
    except Exception as e:
        print(f"❌ Ошибка обработки навигации: {e}")
        return

    await state.update_data(current_index=new_idx)
    await edit_item_slider_message(call.message, state, items, new_idx, data.get('current_category', ''))


async def back_to_stock_belts_materials(message: Message, state: FSMContext):
    """Назад на шаг выбора материала (слайдер материалов по выбранному типу и цвету)."""
    data = await state.get_data()

    selected_material = (data.get('stockbelts_selected_material') or data.get('selected_material') or '').strip()
    selected_color = (data.get('stock_belts_selected_color') or data.get('selected_color') or '').strip()

    if not selected_material:
        # если по какой-то причине нет выбранного типа — возвращаем в меню поясов
        await go_back_with_slider_cleanup(message, state, show_stock_belts_menu)
        return

    # перестраиваем список материалов под выбранный цвет
    try:
        material_items = build_stock_belts_material_items_for_slider(selected_material, color=selected_color or None)
    except Exception:
        material_items = []

    # удаляем слайдер моделей
    try:
        await delete_previous_slider(message.chat.id, state)
    except Exception:
        pass

    if not material_items:
        # если материалов нет — возвращаем к выбору цвета
        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
                [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
                [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
            ]
        )
        await state.update_data(items=[], current_index=0, current_category=None)
        await message.answer('Материалы временно недоступны. Выберите другой цвет:', reply_markup=kb)
        await state.set_state(Order.StockBeltsColor)
        return

    await state.update_data(
        items=material_items,
        current_index=0,
        current_category='stock_belts_material',
        selected_material=selected_material
    )
    await show_item_slider(message.chat.id, state, material_items, 0, 'Материалы: Пояса для чулок')
    await state.set_state(Order.StockBeltsMaterial)

    # клавиатура цветов остаётся снизу
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await message.answer('Листайте материалы. Можно сменить цвет кнопками ниже:', reply_markup=kb)

@dp.message(Order.StockBeltsModel, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_stock_belts_model(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.StockBeltsModel, F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_stock_belts_model(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.callback_query(F.data.startswith('item_prev_'))
@retry_on_network_error()
async def item_prev_handler(call: CallbackQuery, state: FSMContext):
    try:
        idx = int(call.data.split('_')[2])
    except Exception:
        await call.answer()
        return

    # микро-оптимизация UX: убираем "часики" сразу
    try:
        await call.answer(cache_time=1)
    except Exception:
        pass
    data = await state.get_data()
    items = data.get('items', [])
    if not items:
        await call.answer('Товары недоступны')
        return
    new_idx = (idx - 1) % len(items)
    await state.update_data(current_index=new_idx)
    await edit_item_slider_message(call.message, state, items, new_idx, data.get('current_category', ''))
@dp.callback_query(F.data.startswith('item_next_'))
@retry_on_network_error()
async def item_next_handler(call: CallbackQuery, state: FSMContext):
    try:
        idx = int(call.data.split('_')[2])
    except Exception:
        await call.answer()
        return

    # микро-оптимизация UX: убираем "часики" сразу
    try:
        await call.answer(cache_time=1)
    except Exception:
        pass
    data = await state.get_data()
    items = data.get('items', [])
    if not items:
        await call.answer('Товары недоступны')
        return
    new_idx = (idx + 1) % len(items)
    await state.update_data(current_index=new_idx)
    await edit_item_slider_message(call.message, state, items, new_idx, data.get('current_category', ''))
@dp.callback_query(F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def debug_add_to_cart_handler(call: CallbackQuery, state: FSMContext):
    """УНИВЕРСАЛЬНЫЙ обработчик с детальной отладкой"""
    _lock = get_action_lock(call.from_user.id, "debug_add_to_cart_handler")
    if _lock.locked():
        try:
            await call.answer('⏳ Уже добавляю...', show_alert=False)
        except Exception:
            pass
        return
    await _lock.acquire()
    try:
        current_state = await state.get_state()
        print(f'🔍 DEBUG: Обработчик вызван. Состояние: {current_state}')
        print(f'🔍 DEBUG: Данные callback: {call.data}')
        if current_state == Order.StockBeltsMaterial.state:
            print('🔍 DEBUG: Это состояние StockBeltsMaterial - обрабатываем материал пояса')
            await add_stock_belts_material_to_cart(call, state)
            return
        elif current_state == Order.StockBeltsModel.state:
            print('🔍 DEBUG: Это состояние StockBeltsModel - обрабатываем модель пояса')
            await add_stock_belts_model_to_cart(call, state)
            return
        elif current_state == LingerieSet.BustMaterial.state:
            print("🔍 DEBUG: Это состояние LingerieSet.BustMaterial - обрабатываем выбор материала бюста в комплекте")
            await lingerie_set_select_bust_material(call, state)
            return
        elif current_state == LingerieSet.BustModel.state:
            print("🔍 DEBUG: Это состояние LingerieSet.BustModel - обрабатываем выбор модели бюста в комплекте")
            await lingerie_set_add_bust_model(call, state)
            return
        elif current_state == LingerieSet.PantiesModel.state:
            print("🔍 DEBUG: Это состояние LingerieSet.PantiesModel - обрабатываем выбор модели трусиков в комплекте")
            await lingerie_set_add_panties_model(call, state)
            return
        else:
            print(f'🔍 DEBUG: Другое состояние {current_state} - стандартная обработка')
            try:
                item_id = int(call.data.split('_')[3])
            except Exception:
                await call.answer('Ошибка добавления')
                return
            data = await state.get_data()
            items = data.get('items', [])
            item = next((x for x in items if x.get('ID') == item_id), None)
            if not item:
                await call.answer('Товар не найден')
                return
            is_panties = 'Трусики' in str(item.get('Тип', '')) or 'Трусики' in str(item.get('Категория', '')) or 'Трусики по акции' in str(item.get('Тип', '')) or ('Трусики по акции' in str(item.get('Категория', '')))
            if is_panties and item.get('Вариант посадки'):
                await state.update_data(selected_item=item)
                await ask_fit_option(call.message, item, state)
                await call.answer()
            else:
                # Добавляем выбранный цвет в товар, чтобы он отображался в корзине
                sel_color = (
                    data.get('lingerie_set_color')
                    or data.get('bust_selected_color')
                    or data.get('panties_selected_color')
                    or data.get('selected_color')
                    or data.get('stock_belts_selected_color')
                    or ''
                ).strip()
                if sel_color:
                    item = item.copy()
                # Если у трусиков в карточке пустой материал — подставляем из выбранного материала комплекта
                # --- FIX: подставляем материал комплекта ТОЛЬКО для трусиков ---
                is_panties = bool(item.get('is_panties'))
                if not is_panties:
                    _t = str(item.get('Тип') or '').lower()
                    is_panties = ('трус' in _t) or (_t in ('стринги', 'бразильянки', 'классика', 'шорты'))
                if is_panties and not (item.get('Материал') or '').strip():
                    _set_mat = (
                        (data.get('lingerie_set_material') or '')
                        or (data.get('set_material') or '')
                        or ''
                    )
                    _base = _set_mat.split(':', 1)[-1].strip() if ':' in _set_mat else _set_mat.strip()
                    if _base:
                        item = item.copy()
                        item['Материал'] = f"трусиков: {_base}"
                    item['Цвет'] = item.get('Цвет') or sel_color
                add_item_to_cart(call.from_user.id, item)
                await call.answer(f"Товар {item.get('Название', '')} добавлен в корзину")
                await delete_previous_slider(call.message.chat.id, state)
                await call.message.answer(f"✅ *{escape_markdown(item.get('Название', ''))}* добавлен в вашу корзину!")
                kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Каталог товаров')]])
                await call.message.answer('Вы можете выбрать что-то еще или перейти в корзину.', reply_markup=kb)

    finally:
        if _lock.locked():
            _lock.release()
@retry_on_network_error()
@retry_on_network_error()
async def ask_fit_option(message: Message, item: dict, state: FSMContext):
    fit_options = item.get('Вариант посадки', '').strip()
    print(f"🔍 Функция ask_fit_option вызвана с вариантами: '{fit_options}'")
    if not fit_options:
        print('❌ Варианты посадки пустые, добавляем сразу в корзину')
        data = await state.get_data()
        sel_color = (data.get('selected_color') or '').strip()
        if sel_color:
            item = item.copy()
            item.setdefault('Цвет', sel_color)
        add_item_to_cart(message.from_user.id, item)
        await message.answer(f"✅ *{escape_markdown(item.get('Название', ''))}* добавлены в вашу корзину!")
        return
    options = [opt.strip() for opt in fit_options.split(',') if opt.strip()]
    print(f'📋 Разобранные варианты посадки: {options}')
    if not options:
        print('❌ Нет валидных вариантов посадки, добавляем сразу в корзину')
        data = await state.get_data()
        sel_color = (data.get('selected_color') or '').strip()
        if sel_color:
            item = item.copy()
            item.setdefault('Цвет', sel_color)
        add_item_to_cart(message.from_user.id, item)
        await message.answer(f"✅ *{escape_markdown(item.get('Название', ''))}* добавлены в вашу корзину!")
        return
    keyboard = []
    for option in options:
        keyboard.append([InlineKeyboardButton(text=option, callback_data=f'fit_{option}')])

    # сохраняем id текущего слайдера, чтобы потом удалить его при 'Назад'
    data = await state.get_data()
    if data.get('last_slider_message_id'):
        await state.update_data(panties_slider_msg_id=data.get('last_slider_message_id'))
    kb = InlineKeyboardMarkup(inline_keyboard=keyboard)
    try:
        fit_msg = await message.answer(f"📏 *{escape_markdown(item.get('Название', ''))}*\n\n📝 Материал: {escape_markdown(item.get('Материал', ''))}\n\nПожалуйста, выберите вариант посадки:", reply_markup=kb)
        print('✅ Сообщение с выбором посадки отправлено')
        await state.update_data(panties_fit_msg_id=fit_msg.message_id)
        action_msg = await message.answer("Выберите действие:", reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]]))
        await state.update_data(panties_fit_action_msg_id=action_msg.message_id)
    except Exception as e:
        print(f'❌ Ошибка при отправке сообщения с выбором посадки: {e}')
    await state.set_state(Order.PantiesFit)


@dp.message(Order.PantiesFit, F.text == "Назад")
@retry_on_network_error()
async def panties_back_from_fit(message: Message, state: FSMContext):
    """Назад со стадии выбора посадки: удаляем сообщение с inline-кнопками посадки и возвращаем к выбору типа."""
    data = await state.get_data()

    # удаляем inline-сообщение с выбором посадки (и сервисное сообщение с ReplyKeyboard)
    fit_msg_id = data.get("panties_fit_msg_id")
    action_msg_id = data.get("panties_fit_action_msg_id")
    for mid in [fit_msg_id, action_msg_id, data.get('panties_slider_msg_id')]:
        if mid:
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=mid)
            except Exception:
                pass

    await state.update_data(panties_fit_msg_id=None, panties_fit_action_msg_id=None, panties_slider_msg_id=None)

    # удаляем слайдер моделей, который был показан до выбора посадки
    try:
        await delete_previous_slider(message.chat.id, state)
    except Exception:
        pass

    # возвращаем к выбору типа (для текущего материала)
    selected_material = (data.get("selected_material") or "").strip()
    if selected_material:
        kb = get_panties_type_keyboard(selected_material)
        await message.answer(
            f'Выберите тип трусиков для материала *{escape_markdown(selected_material)}*:',
            reply_markup=kb
        )
        await state.set_state(Order.PantiesType)
    else:
        # если материал не сохранён — откатываемся в меню трусиков
        await show_panties_menu(message, state)


@dp.message(Order.PantiesFit, F.text == "Корзина")
@retry_on_network_error()
async def panties_cart_from_fit(message: Message, state: FSMContext):
    data = await state.get_data()
    # по желанию — можно также удалить inline, чтобы не висело
    fit_msg_id = data.get("panties_fit_msg_id")
    action_msg_id = data.get("panties_fit_action_msg_id")
    for mid in [fit_msg_id, action_msg_id]:
        if mid:
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=mid)
            except Exception:
                pass
    await state.update_data(panties_fit_msg_id=None, panties_fit_action_msg_id=None)
    await show_cart(message, state)


@dp.callback_query(Order.PantiesFit, F.data.startswith('fit_'))
@retry_on_network_error()
async def handle_fit_selection(call: CallbackQuery, state: FSMContext):
    selected_fit = call.data.replace('fit_', '')
    print(f"🔍 Выбрана посадка: '{selected_fit}'")
    data = await state.get_data()
    combined_item = data.get('selected_combined_item')
    if not combined_item:
        print('❌ Ошибка: объединенный товар не найден в состоянии')
        await call.answer('Ошибка: товар не найден')
        return
    combined_item_with_fit = combined_item.copy()
    combined_item_with_fit['Посадка'] = selected_fit
    print(f"✅ Добавляем товар в корзину с посадкой: '{selected_fit}'")
    add_item_to_cart(call.from_user.id, combined_item_with_fit)
    apply_panties_promotion(call.from_user.id)
    try:
        await call.message.delete()
        print('✅ Сообщение с выбором посадки удалено')
    except Exception as e:
        print(f'⚠️ Не удалось удалить сообщение: {e}')
    await call.message.answer(f"✅ *{escape_markdown(combined_item.get('Название', ''))}* \n📏 Посадка: {selected_fit}\n📝 Материал: {escape_markdown(combined_item.get('Материал', ''))}\nДобавлен в вашу корзину!", parse_mode=ParseMode.MARKDOWN)
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Выбрать еще трусики'), KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Каталог товаров')]])
    await call.message.answer('Вы можете выбрать что-то еще или перейти в корзину.', reply_markup=kb)
    await state.set_state(Order.PantiesView)
    await call.answer()

@retry_on_network_error()
def build_panties_models_by_type_all(panties_type: str, material_filter: str | None = None, all_rows: list | None = None) -> list:
    """Возвращает ВСЕ модели трусиков выбранного типа (без фильтрации по материалу/цвету).
    Нужна для раздела 'Комплект белья' по требованиям: тип -> все модели этого типа.
    """
    if all_rows is None:
        all_rows = _load_all_panties_rows()
    type_norm = (panties_type or "").strip()
    type_lower = type_norm.lower()

    # В 'Комплекте белья' можем дополнительно отфильтровать модели по материалу комплекта.
    # material_filter ожидается в человеко-читаемом виде: 'Хлопковый' / 'Кружевной' / 'Эластичная сетка' / 'Вышивка'
    material_norm = (material_filter or "").strip()
    material_lower = material_norm.lower()
    material_tokens: list[str] = []
    if material_lower:
        # В таблице "Материал" иногда пустой, поэтому дополнительно смотрим в "Модель".
        # Делаем "мягкое" сопоставление по подстрокам.
        _tok_map = {
            "хлопковый": ["хлопков"],
            "кружевной": ["кружевн"],
            "эластичная сетка": ["эластичн", "сетка"],
            "вышивка": ["вышив"],
        }
        material_tokens = _tok_map.get(material_lower, [material_lower])


    panties_data: list[dict] = []
    for row in all_rows:
        row_type = str(row.get("Тип", "") or "").strip()
        if row_type.lower() != type_lower:
            continue

        row_model = str(row.get("Модель", "") or "").strip()
        row_material = str(row.get("Материал", "") or "").strip()

        # Если задан фильтр материала (материал комплекта) — оставляем только совпадающие позиции.
        if material_tokens:
            rm = (row_material or "").strip().lower()
            rmo = (row_model or "").strip().lower()
            if not any((tok in rm) or (tok in rmo) for tok in material_tokens):
                continue


        rec: dict = {}
        try:
            rec["ID"] = int(float(row.get("ID")))
        except Exception:
            rec["ID"] = abs(hash((row_model or row.get("Название", "") or row_type) + "|" + row_material)) % 10**9

        rec["Тип"] = row_type
        rec["Материал"] = row_material
        rec["Модель"] = row_model
        rec["Название"] = row.get("Название") or row_model or f"{row_type}"
        rec["Описание"] = f"Модель: {row_model}" if row_model else f"Тип: {row_type}"
        rec["Цена"] = row.get("Цена") or 2400
        rec["Вариант посадки"] = row.get("Вариант посадки", "")

        img = row.get("Изображение модели") or row.get("Изображение") or ""
        if isinstance(img, str) and img.strip():
            rec["Изображение"] = _normalize_image_source(img.strip())
        else:
            rec["Изображение"] = None

        panties_data.append(rec)

    panties_data.sort(key=lambda x: (x.get("Модель") or x.get("Название") or "", x.get("ID") or 0))
    return panties_data
def _lingerie_set_material_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Хлопковый"), KeyboardButton(text="Кружевной")],
            [KeyboardButton(text="Эластичная сетка"), KeyboardButton(text="Вышивка")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]
        ]
    )


def _lingerie_set_color_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Черный"), KeyboardButton(text="Красный")],
            [KeyboardButton(text="Белый"), KeyboardButton(text="Другие")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]
        ]
    )


def _lingerie_set_simple_back_cart_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]]
    )


def _lingerie_set_panties_type_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Стринги"), KeyboardButton(text="Бразильянки")],
            [KeyboardButton(text="Классика"), KeyboardButton(text="Шорты")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]
        ]
    )

def _lingerie_set_fit_kb() -> ReplyKeyboardMarkup:
    """Клавиатура для стадии выбора посадки (оставляем только Назад и корзину)."""
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]]
    )



# --- Комплект белья: "липкие" кнопки (цвет и тип) без необходимости жать Назад ---

_LINGERIE_SET_COLORS = {"Черный", "Красный", "Белый", "Другие"}
_LINGERIE_SET_PANTIES_TYPES = {"Стринги", "Бразильянки", "Классика", "Шорты"}


def _lingerie_set_sticky_color_kb() -> ReplyKeyboardMarkup:
    """Цвет всегда доступен (используем на шагах слайдеров/выбора типа)."""
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Черный"), KeyboardButton(text="Красный")],
            [KeyboardButton(text="Белый"), KeyboardButton(text="Другие")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")],
        ],
    )


def _lingerie_set_sticky_type_kb() -> ReplyKeyboardMarkup:
    """Тип трусиков + цвет всегда доступны (на шаге выбора модели трусиков)."""
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Стринги"), KeyboardButton(text="Бразильянки")],
            [KeyboardButton(text="Классика"), KeyboardButton(text="Шорты")],
            [KeyboardButton(text="Черный"), KeyboardButton(text="Красный")],
            [KeyboardButton(text="Белый"), KeyboardButton(text="Другие")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")],
        ],
    )




def _lingerie_set_bust_model_kb() -> ReplyKeyboardMarkup:
    """Клавиатура на шаге выбора модели бюста (без кнопок цвета)."""
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Назад к материалам"), KeyboardButton(text="Корзина")],
        ],
    )


def _lingerie_set_sticky_type_kb() -> ReplyKeyboardMarkup:
    """Тип трусиков всегда доступен (на шаге слайдера моделей трусиков), без кнопок цвета."""
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Стринги"), KeyboardButton(text="Бразильянки")],
            [KeyboardButton(text="Классика"), KeyboardButton(text="Шорты")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")],
        ],
    )

def _update_lingerie_set_items_color_in_cart(user_id: int, new_color: str) -> None:
    """Если пользователь сменил цвет уже после добавления части комплекта — синхронизируем цвет у items комплекта."""
    try:
        cart = user_carts.get(user_id) or []
        changed = False
        for it in cart:
            if it.get("is_lingerie_set"):
                it["Цвет"] = new_color
                changed = True
        if changed:
            user_carts.set(user_id, cart)
    except Exception as e:
        print(f"⚠️ Не удалось обновить цвет комплекта в корзине: {e}")

@dp.message(Order.OrderMenu, F.text == "Комплект белья")
@retry_on_network_error()
async def show_lingerie_set_menu(message: Message, state: FSMContext):
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите материал комплекта:", reply_markup=_lingerie_set_material_kb())
    await state.set_state(LingerieSet.MaterialMenu)
    await state.update_data(
        lingerie_set_material=None,
        lingerie_set_color=None,
        selected_color=None,
        bust_selected_color=None,
        panties_selected_color=None,
        stock_belts_selected_color=None,

        lingerie_set_panties_type=None,
        pending_bust_material=None,
        selected_material=None,
        selected_material_item=None,
        selected_combined_item=None
    )


@dp.message(LingerieSet.MaterialMenu)
@retry_on_network_error()
async def lingerie_set_handle_material_menu(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if text == "Назад":
        await delete_previous_slider(message.chat.id, state)
        await make_order(message, state)
        return
    if text == "Корзина":
        await show_cart(message, state)
        return

    mapping = {
        "Хлопковый": "Материал бюста: Хлопковый",
        "Кружевной": "Материал бюста: Кружевной",
        "Эластичная сетка": "Материал бюста: Эластичная сетка",
        "Вышивка": "Материал бюста: Вышивка"
    }
    if text not in mapping:
        await message.answer("Пожалуйста, выберите материал кнопками ниже.", reply_markup=_lingerie_set_material_kb())
        return

    bust_material = mapping[text]
    await state.update_data(lingerie_set_material=text, lingerie_set_bust_material=bust_material)
    _invalidate_reply_keyboard_cache(message.chat.id)
    await message.answer("Выберите цвет комплекта:", reply_markup=_lingerie_set_color_kb())
    await state.set_state(LingerieSet.ColorMenu)


@dp.message(LingerieSet.ColorMenu)
@retry_on_network_error()
async def lingerie_set_handle_color_menu(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if text == "Назад":
        await delete_previous_slider(message.chat.id, state)
        await message.answer("Выберите материал комплекта:", reply_markup=_lingerie_set_material_kb())
        await state.set_state(LingerieSet.MaterialMenu)
        return
    if text == "Корзина":
        await show_cart(message, state)
        return

    allowed = {"Черный", "Красный", "Белый", "Другие"}
    if text not in allowed:
        await message.answer("Пожалуйста, выберите цвет кнопками ниже.", reply_markup=_lingerie_set_color_kb())
        return

    data = await state.get_data()
    bust_material = (data.get("lingerie_set_bust_material") or "").strip()
    if not bust_material:
        await message.answer("Сначала выберите материал комплекта.", reply_markup=_lingerie_set_material_kb())
        await state.set_state(LingerieSet.MaterialMenu)
        return

    await state.update_data(lingerie_set_color=text)
    remember_user_color(message.from_user.id, text)

    material_items = build_material_items_for_slider(bust_material, color=text)
    await delete_previous_slider(message.chat.id, state)

    if not material_items:
        await message.answer(f"К сожалению, материалы для цвета '{text}' временно недоступны. Выберите другой цвет.", reply_markup=_lingerie_set_color_kb())
        return

    await state.update_data(items=material_items, current_index=0, current_category="lingerie_set_bust_material", selected_material=bust_material)
    await show_item_slider(message.chat.id, state, material_items, 0, f"Материалы: {bust_material}")
    # Как в разделе "Трусики": не шлём "пустышку" для reply-клавиатуры.
    # Просто прикрепляем клавиатуру к обычному сообщению.
    await message.answer(
        "Листайте материалы. Можно сменить цвет кнопками ниже:",
        reply_markup=_lingerie_set_sticky_color_kb(),
    )
    await state.set_state(LingerieSet.BustMaterial)



@dp.message(LingerieSet.BustMaterial, F.text.in_(_LINGERIE_SET_COLORS))
@dp.message(LingerieSet.BustModel, F.text.in_(_LINGERIE_SET_COLORS))
@dp.message(LingerieSet.PantiesType, F.text.in_(_LINGERIE_SET_COLORS))
@dp.message(LingerieSet.PantiesModel, F.text.in_(_LINGERIE_SET_COLORS))
@retry_on_network_error()
async def lingerie_set_change_color_without_back(message: Message, state: FSMContext):
    """Позволяет сменить цвет комплекта на любом шаге, не возвращаясь назад."""
    new_color = (message.text or '').strip()
    if new_color not in _LINGERIE_SET_COLORS:
        return

    await state.update_data(
        lingerie_set_color=new_color,
        selected_color=new_color,
        bust_selected_color=new_color,
        panties_selected_color=new_color,
    )
    remember_user_color(message.from_user.id, new_color)
    _update_lingerie_set_items_color_in_cart(message.from_user.id, new_color)

    current_state = await state.get_state()

    # Если мы на шагах выбора/слайдера бюста — переоткроем слайдер материалов бюста под новый цвет
    if current_state in (LingerieSet.BustMaterial.state, LingerieSet.BustModel.state):
        data = await state.get_data()
        # ВАЖНО: для фильтрации по таблице нам нужна "каноническая" строка материала
        # (например: "Материал бюста: Хлопковый").
        # Раньше тут собиралась строка типа "Хлопковый бюст", из-за этого
        # build_material_items_for_slider() иногда возвращал пусто и бот писал
        # "цвет недоступен" — а со второй попытки (через ColorMenu) всё работало.
        bust_material = (data.get('selected_material') or data.get('lingerie_set_bust_material') or '').strip()
        if not bust_material:
            await message.answer('Сначала выберите материал комплекта.', reply_markup=_lingerie_set_material_kb())
            await state.set_state(LingerieSet.MaterialMenu)
            return

        material_items = build_material_items_for_slider(bust_material, color=new_color)

        await delete_previous_slider(message.chat.id, state)
        if not material_items:
            await message.answer('К сожалению, для этого цвета нет материалов. Выберите другой цвет.', reply_markup=_lingerie_set_sticky_color_kb())
            await state.set_state(LingerieSet.ColorMenu)
            return

        await state.update_data(
            items=material_items,
            current_index=0,
            current_category='lingerie_set_bust_material',
            selected_material=bust_material,
        )
        await show_item_slider(message.chat.id, state, material_items, 0, f"Материалы: {bust_material}")
        # Как в разделе "Трусики": при смене цвета просто перерисовываем слайдер,
        # без дополнительных сообщений/пустышек.
        await state.set_state(LingerieSet.BustMaterial)
        return

    # На шагах трусиков: просто подтверждаем смену цвета (модели не зависят от цвета)

@dp.message(LingerieSet.BustMaterial, F.text == "Назад")
@retry_on_network_error()
async def lingerie_set_back_from_bust_material(message: Message, state: FSMContext):
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите цвет комплекта:", reply_markup=_lingerie_set_color_kb())
    await state.set_state(LingerieSet.ColorMenu)


@dp.message(LingerieSet.BustMaterial, F.text == "Корзина")
@retry_on_network_error()
async def lingerie_set_cart_from_bust_material(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.BustModel, F.text.in_({"Назад", "Назад к материалам"}))
@retry_on_network_error()
async def lingerie_set_back_to_bust_materials(message: Message, state: FSMContext):
    data = await state.get_data()
    bust_material = (data.get("selected_material") or data.get("lingerie_set_bust_material") or "").strip()
    color = (data.get("lingerie_set_color") or "").strip() or None

    await state.update_data(
        pending_bust_material=None,
        selected_material_item=None
    )

    material_items = build_material_items_for_slider(bust_material, color=color) if bust_material else []
    await delete_previous_slider(message.chat.id, state)

    if not material_items:
        await message.answer("Материалы временно недоступны. Выберите другой цвет:", reply_markup=_lingerie_set_color_kb())
        await state.set_state(LingerieSet.ColorMenu)
        return

    await state.update_data(items=material_items, current_index=0, current_category="lingerie_set_bust_material", selected_material=bust_material)
    await show_item_slider(message.chat.id, state, material_items, 0, f"Материалы: {bust_material}")
    # Как в разделе "Трусики": не шлём "пустышку" для reply-клавиатуры.
    # Просто прикрепляем клавиатуру к обычному сообщению.
    await message.answer(
        "Листайте материалы. Можно сменить цвет кнопками ниже:",
        reply_markup=_lingerie_set_sticky_color_kb(),
    )
    await state.set_state(LingerieSet.BustMaterial)


@dp.message(LingerieSet.BustModel, F.text == "Корзина")
@retry_on_network_error()
async def lingerie_set_cart_from_bust_model(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.BustModel, F.text == "Каталог товаров")
@retry_on_network_error()
async def lingerie_set_catalog_from_bust_model(message: Message, state: FSMContext):
    await state.clear()
    await make_order(message, state)


@dp.message(LingerieSet.PantiesType, F.text == "Корзина")
@retry_on_network_error()
async def lingerie_set_cart_from_panties_type(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.PantiesType, F.text == "Назад")
@retry_on_network_error()
async def lingerie_set_back_from_panties_type(message: Message, state: FSMContext):
    # Возвращаемся к выбору модели бюста (последний шаг перед трусиками)
    data = await state.get_data()
    bust_material = (data.get("selected_material") or "").strip()
    if not bust_material:
        await message.answer("Выберите цвет комплекта:", reply_markup=_lingerie_set_color_kb())
        await state.set_state(LingerieSet.ColorMenu)
        return

    model_items = build_model_items_for_slider(bust_material)
    await delete_previous_slider(message.chat.id, state)

    if not model_items:
        await message.answer("Модели бюста временно недоступны. Выберите материал заново.", reply_markup=_lingerie_set_material_kb())
        await state.set_state(LingerieSet.MaterialMenu)
        return

    await state.update_data(items=model_items, current_index=0, current_category="lingerie_set_bust_model")
    await show_item_slider(message.chat.id, state, model_items, 0, f"Модели для {bust_material}")
    kb = _lingerie_set_bust_model_kb()
    _invalidate_reply_keyboard_cache(message.chat.id)
    await message.answer("Теперь выберите модель бюста:", reply_markup=kb)
    await state.set_state(LingerieSet.BustModel)


@dp.message(LingerieSet.PantiesType)
@retry_on_network_error()
async def lingerie_set_handle_panties_type(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if text in {"Назад", "Корзина"}:
        # эти случаи уже обработаны отдельными хэндлерами выше
        return

    allowed_types = {"Стринги", "Бразильянки", "Классика", "Шорты"}
    if text not in allowed_types:
        await message.answer("Выберите тип трусиков кнопками ниже:", reply_markup=_lingerie_set_panties_type_kb())
        return

    await state.update_data(lingerie_set_panties_type=text)
    data = await state.get_data()
    set_mat = (data.get("lingerie_set_material") or "").strip()
    panties_items = build_panties_models_by_type_all(text, set_mat)

    await delete_previous_slider(message.chat.id, state)

    if not panties_items:
        await message.answer("К сожалению, модели этого типа временно недоступны. Выберите другой тип.", reply_markup=_lingerie_set_panties_type_kb())
        await state.set_state(LingerieSet.PantiesType)
        return

    await state.update_data(items=panties_items, current_index=0, current_category="lingerie_set_panties_model")
    await show_item_slider(message.chat.id, state, panties_items, 0, f"Трусики: {text}")
    await message.answer("Листайте модели трусиков и выбирайте подходящую:", reply_markup=_lingerie_set_sticky_type_kb())
    await state.set_state(LingerieSet.PantiesModel)



@dp.message(LingerieSet.PantiesModel, F.text.in_(_LINGERIE_SET_PANTIES_TYPES))
@retry_on_network_error()
async def lingerie_set_change_panties_type_without_back(message: Message, state: FSMContext):
    """Позволяет сменить тип трусиков прямо на слайдере моделей трусиков."""
    new_type = (message.text or '').strip()
    if new_type not in _LINGERIE_SET_PANTIES_TYPES:
        return

    await state.update_data(lingerie_set_panties_type=new_type)
    data = await state.get_data()
    set_mat = (data.get('lingerie_set_material') or '').strip()
    panties_items = build_panties_models_by_type_all(new_type, set_mat)

    await delete_previous_slider(message.chat.id, state)
    if not panties_items:
        await message.answer("К сожалению, моделей такого типа сейчас нет. Выберите другой тип.", reply_markup=_lingerie_set_sticky_type_kb())
        await state.set_state(LingerieSet.PantiesType)
        return

    await state.update_data(items=panties_items, current_index=0, current_category="lingerie_set_panties_model")
    await show_item_slider(message.chat.id, state, panties_items, 0, f"Модели: {new_type}")

    await message.answer(f"✅ Тип трусиков изменен на *{escape_markdown(new_type)}*. Листайте и выбирайте модель:", reply_markup=_lingerie_set_sticky_type_kb(), parse_mode=ParseMode.MARKDOWN)
    await state.set_state(LingerieSet.PantiesModel)

@dp.message(LingerieSet.PantiesModel, F.text == "Назад")
@retry_on_network_error()
async def lingerie_set_back_from_panties_model(message: Message, state: FSMContext):
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите тип трусиков:", reply_markup=_lingerie_set_panties_type_kb())
    await state.set_state(LingerieSet.PantiesType)


@dp.message(LingerieSet.PantiesModel, F.text == "Корзина")
@retry_on_network_error()
async def lingerie_set_cart_from_panties_model(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.PantiesModel, F.text == "Выбрать еще трусики")
@retry_on_network_error()
async def lingerie_set_choose_more_panties(message: Message, state: FSMContext):
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите тип трусиков:", reply_markup=_lingerie_set_panties_type_kb())
    await state.set_state(LingerieSet.PantiesType)


@dp.message(LingerieSet.PantiesModel, F.text == "Перейти в корзину")
@retry_on_network_error()
async def lingerie_set_go_cart_after_panties(message: Message, state: FSMContext):
    await show_cart(message, state)


@dp.message(LingerieSet.PantiesModel, F.text == "Каталог товаров")
@retry_on_network_error()
async def lingerie_set_catalog_after_panties(message: Message, state: FSMContext):
    await state.clear()
    await make_order(message, state)


async def lingerie_set_select_bust_material(call: CallbackQuery, state: FSMContext):
    """Выбор материала бюста в комплекте (без немедленного добавления в корзину)."""
    try:
        item_id = int(call.data.split("_")[3])
    except Exception:
        await call.answer("Ошибка выбора материала", show_alert=True)
        return

    data = await state.get_data()
    items = data.get("items", []) or []
    item = next((x for x in items if x.get("ID") == item_id), None)
    if not item:
        await call.answer("Материал не найден", show_alert=True)
        return

    item = item.copy()
    # ВАЖНО: элементы материалов бюста в исходном каталоге почему-то могут иметь is_panties=True.
    # Для комплекта белья материал бюста НЕ должен участвовать в акции на трусики.
    item["is_panties"] = False
    item.pop("promo_applied", None)
    # original_price для материала бюста не нужен, но уберём на всякий случай
    item.pop("original_price", None)
    # Цвет комплекта сохраняем прямо в материал (чтобы потом гарантированно попал и в материал, и в модель)
    set_color = (data.get("lingerie_set_color") or data.get("selected_color") or data.get("bust_selected_color") or "").strip()
    if set_color:
        item.setdefault("Цвет", set_color)


    material_name = item.get("Материал") or data.get("selected_material") or ""
    await state.update_data(
        pending_bust_material=item,
        selected_material_item=item,
        selected_material=material_name,
        lingerie_set_material_id=item.get("ID"),
        lingerie_set_material_name=material_name
    )

    await call.answer(f"Материал '{material_name}' выбран", show_alert=False)
    await delete_previous_slider(call.message.chat.id, state)

    model_items = build_model_items_for_slider(material_name)
    if not model_items:
        await call.message.answer(
            f"К сожалению, модели для материала *{escape_markdown(material_name)}* временно недоступны.",
            reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Корзина")]]),
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(LingerieSet.BustMaterial)
        return

    await state.update_data(items=model_items, current_index=0, current_category="lingerie_set_bust_model")
    await show_item_slider(call.message.chat.id, state, model_items, 0, f"Модели для {material_name}")

    kb = _lingerie_set_bust_model_kb()
    _invalidate_reply_keyboard_cache(call.message.chat.id)
    await call.message.answer("Теперь выберите модель бюста:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(LingerieSet.BustModel)


async def lingerie_set_add_bust_model(call: CallbackQuery, state: FSMContext):
    """Добавление модели бюста в корзину и переход к выбору типа трусиков."""
    try:
        item_id = int(call.data.split("_")[3])
    except Exception:
        await call.answer("Ошибка добавления", show_alert=True)
        return

    data = await state.get_data()
    items = data.get("items", []) or []
    pending_material = data.get("pending_bust_material")

    item = next((x for x in items if x.get("ID") == item_id), None)
    if not item:
        await call.answer("Модель не найдена", show_alert=True)
        return

    user_id = call.from_user.id
    cart = user_carts.get(user_id) or []

    material_in_cart = None

    # 1) pending материал
    if pending_material:
        # ВАЖНО: материал бюста в комплекте не должен считаться трусиками (акция).
        pm = dict(pending_material)
        pm["is_panties"] = False
        pm.pop("promo_applied", None)
        pm.pop("original_price", None)
        pm["is_lingerie_set"] = True

        # цвет комплекта прокидываем в бюст
        set_color = (data.get("lingerie_set_color") or data.get("selected_color") or data.get("bust_selected_color") or "").strip()
        if set_color:
            pm.setdefault("Цвет", set_color)

        already_in_cart = any(
            it.get("ID") == pm.get("ID") and it.get("Материал") == pm.get("Материал") and not it.get("Модель")
            for it in cart
        )
        if not already_in_cart:
            add_item_to_cart(user_id, pm)
        material_in_cart = pm
        await state.update_data(pending_bust_material=None)
    else:
        # 2) fallback: ищем материал бюста в корзине
        for it in cart:
            is_bust_material = (
                it.get("Материал") and (not it.get("Модель")) and
                any((mat in str(it.get("Материал", "")).lower() for mat in [
                    "материал бюста: хлопковый",
                    "материал бюста: кружевной",
                    "материал бюста: эластичная сетка",
                    "материал бюста: вышивка",
                    "хлопковый",
                    "кружевной",
                    "эластичная сетка",
                    "вышивка"
                ]))
            )
            if is_bust_material:
                material_in_cart = it
                break

    if not material_in_cart:
        await call.answer("❌ Сначала выберите материал бюста", show_alert=True)
        return

    if not item.get("Материал"):
        item["Материал"] = material_in_cart.get("Материал")

    set_color = (data.get("lingerie_set_color") or data.get("selected_color") or data.get("bust_selected_color") or "").strip()
    if set_color:
        item.setdefault("Цвет", set_color)

    item["is_lingerie_set"] = True
    item["is_panties"] = False
    add_item_to_cart(user_id, item)

    await call.answer("Модель добавлена", show_alert=False)
    await delete_previous_slider(call.message.chat.id, state)

    _invalidate_reply_keyboard_cache(call.message.chat.id)
    await call.message.answer(
        f"✅ *{escape_markdown(item.get('Название', 'Модель'))}* добавлена в корзину!\n\nТеперь выберите тип трусиков для комплекта:",
        reply_markup=_lingerie_set_panties_type_kb(),
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(LingerieSet.PantiesType)


async def lingerie_set_ask_fit_option(message: Message, item: dict, state: FSMContext):
    fit_options = (item.get("Вариант посадки", "") or "").strip()
    if not fit_options:
        add_item_to_cart(message.from_user.id, item)
        await message.answer(f"✅ *{escape_markdown(item.get('Название', ''))}* добавлены в корзину!", parse_mode=ParseMode.MARKDOWN)
        return

    options = [opt.strip() for opt in fit_options.split(",") if opt.strip()]
    if not options:
        add_item_to_cart(message.from_user.id, item)
        await message.answer(f"✅ *{escape_markdown(item.get('Название', ''))}* добавлены в корзину!", parse_mode=ParseMode.MARKDOWN)
        return

    # сохраняем id текущего слайдера, чтобы потом удалить его при 'Назад'
    data = await state.get_data()
    if data.get('last_slider_message_id'):
        await state.update_data(ls_slider_msg_id=data.get('last_slider_message_id'))

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=opt, callback_data=f"fit_{opt}")] for opt in options])
    fit_msg = await message.answer(
        f"📏 *{escape_markdown(item.get('Название', ''))}*\n\n📝 Материал: {escape_markdown(item.get('Материал', ''))}\n\nПожалуйста, выберите вариант посадки:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )
    await state.update_data(ls_fit_msg_id=fit_msg.message_id)
    action_msg = await message.answer("Выберите действие:", reply_markup=_lingerie_set_fit_kb())
    await state.update_data(ls_fit_action_msg_id=action_msg.message_id)
    await state.set_state(LingerieSet.PantiesFit)


async def lingerie_set_add_panties_model(call: CallbackQuery, state: FSMContext):
    try:
        item_id = int(call.data.split("_")[3])
    except Exception:
        await call.answer("Ошибка добавления")
        return

    data = await state.get_data()
    items = data.get("items", []) or []
    item = next((x for x in items if x.get("ID") == item_id), None)
    if not item:
        await call.answer("Товар не найден")
        return

    # ВАЖНО: трусики из комплекта должны участвовать в акции
    item = item.copy()
    
    item['Цвет'] = (data.get('lingerie_set_color') or '').strip()
    item['is_panties'] = True
    item['is_lingerie_set'] = True

    # Пробрасываем ID материала комплекта (нужно для админа/Google Sheets)
    ls_mat_id = data.get("lingerie_set_material_id")
    if ls_mat_id and not item.get("Материал_ID"):
        item["Материал_ID"] = ls_mat_id
    # Подготовка полей для акции (как в обычном разделе "Трусики")
    item.setdefault("original_price", safe_convert_price(item.get("Цена", 0)))
    item.setdefault("quantity", 1)
    # Если нужна посадка — спрашиваем
    if (item.get("Вариант посадки") or "").strip():
        await state.update_data(selected_combined_item=item)
        await call.answer()
        try:
            await call.message.delete()
        except Exception:
            pass
        await lingerie_set_ask_fit_option(call.message, item, state)
        return

    add_item_to_cart(call.from_user.id, item)
    apply_panties_promotion(call.from_user.id)
    await call.answer("Добавлено")
    await delete_previous_slider(call.message.chat.id, state)

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Выбрать еще трусики"), KeyboardButton(text="Перейти в корзину")],
            [KeyboardButton(text="Каталог товаров")]
        ]
    )
    await call.message.answer(
        f"✅ *{escape_markdown(item.get('Название', ''))}* добавлены в корзину!",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(LingerieSet.PantiesModel)





@dp.message(LingerieSet.PantiesFit, F.text == "Назад")
@retry_on_network_error()
async def lingerie_set_back_from_fit(message: Message, state: FSMContext):
    """Возврат со стадии выбора посадки назад к слайдеру моделей трусиков (по текущему типу)."""
    data = await state.get_data()

    # удаляем сообщения выбора посадки (inline) и сервисное сообщение с reply-кнопками
    for mid in [data.get('ls_fit_msg_id'), data.get('ls_fit_action_msg_id'), data.get('ls_slider_msg_id')]:
        if mid:
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=mid)
            except Exception:
                pass
    await state.update_data(ls_fit_msg_id=None, ls_fit_action_msg_id=None, ls_slider_msg_id=None)


    await delete_previous_slider(message.chat.id, state)

    data = await state.get_data()
    panties_type = (data.get("lingerie_set_panties_type") or "").strip()
    set_mat = (data.get("lingerie_set_material") or "").strip()

    if not panties_type:
        await message.answer("Выберите тип трусиков:", reply_markup=_lingerie_set_panties_type_kb())
        await state.set_state(LingerieSet.PantiesType)
        return

    panties_items = build_panties_models_by_type_all(panties_type, set_mat)
    if not panties_items:
        await message.answer("Нет моделей для выбранного типа.", reply_markup=_lingerie_set_panties_type_kb())
        await state.set_state(LingerieSet.PantiesType)
        return

    await state.update_data(items=panties_items, current_index=0, current_category=f"Трусики: {panties_type}")
    await show_item_slider(message.chat.id, state, panties_items, 0, f"Трусики: {panties_type}")
    await state.set_state(LingerieSet.PantiesModel)


@dp.message(LingerieSet.PantiesFit, F.text.in_({"Корзина", "Перейти в корзину"}))
@retry_on_network_error()
async def lingerie_set_cart_from_fit(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(LingerieSet.PantiesFit, F.text == "Выбрать еще трусики")
@retry_on_network_error()
async def lingerie_set_choose_more_panties_from_fit(message: Message, state: FSMContext):
    await delete_previous_slider(message.chat.id, state)
    await message.answer("Выберите тип трусиков:", reply_markup=_lingerie_set_panties_type_kb())
    await state.set_state(LingerieSet.PantiesType)

@dp.callback_query(LingerieSet.PantiesFit, F.data.startswith("fit_"))
@retry_on_network_error()
async def lingerie_set_handle_fit_selection(call: CallbackQuery, state: FSMContext):
    selected_fit = call.data.replace("fit_", "")
    data = await state.get_data()
    combined_item = data.get("selected_combined_item")
    if not combined_item:
        await call.answer("Ошибка: товар не найден", show_alert=True)
        return

    combined_item_with_fit = combined_item.copy()
    combined_item_with_fit["Посадка"] = selected_fit

    # ВАЖНО: корректно выставляем поля для акции на трусики
    combined_item_with_fit["is_panties"] = True
    combined_item_with_fit["original_price"] = safe_convert_price(combined_item_with_fit.get("Цена", 0))
    combined_item_with_fit.setdefault("quantity", 1)

    add_item_to_cart(call.from_user.id, combined_item_with_fit)
    apply_panties_promotion(call.from_user.id)

    try:
        await call.message.delete()
    except Exception:
        pass

    await call.message.answer(
        f"✅ *{escape_markdown(combined_item_with_fit.get('Название', ''))}*\n"
        f"📏 Посадка: {escape_markdown(selected_fit)}\n"
        f"📝 Материал: {escape_markdown(combined_item_with_fit.get('Материал', ''))}\n"
        f"Добавлен в корзину!",
        parse_mode=ParseMode.MARKDOWN
    )

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="Выбрать еще трусики"), KeyboardButton(text="Перейти в корзину")],
            [KeyboardButton(text="Каталог товаров")]
        ]
    )
    await call.message.answer("Вы можете выбрать еще трусики или перейти в корзину.", reply_markup=kb)
    await state.set_state(LingerieSet.PantiesView)
@dp.message(Order.OrderMenu, F.text == 'Бюст')
@retry_on_network_error()
async def show_bust_menu(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Материал бюста: Хлопковый')],
            [KeyboardButton(text='Материал бюста: Эластичная сетка')],
            [KeyboardButton(text='Материал бюста: Кружевной')],
            [KeyboardButton(text='Материал бюста: Вышивка')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await message.answer('Выберите материал бюста:', reply_markup=kb)
    await state.set_state(Order.BustMaterial)

@dp.message(Order.BustModel, F.text == 'Назад к материалам')
@retry_on_network_error()
async def back_to_bust_materials(message: Message, state: FSMContext):
    # 🧹 Чистим выбранный, но ещё не закреплённый материал бюста
    await state.update_data(
        pending_bust_material=None,
        selected_material=None,
        selected_material_item=None
    )
    await go_back_with_slider_cleanup(message, state, show_bust_menu)


@dp.message(Order.BustView, F.text == 'Назад к материалам')
@retry_on_network_error()
async def back_to_bust_materials_from_view(message: Message, state: FSMContext):
    # 🧹 На всякий случай тоже чистим pending-материал
    await state.update_data(
        pending_bust_material=None,
        selected_material=None,
        selected_material_item=None
    )
    await go_back_with_slider_cleanup(message, state, show_bust_menu)


@dp.message(Order.BustModel, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_bust_model(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.BustView, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_bust_view(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.BustMaterial, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_bust(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, make_order)

@dp.message(Order.BustMaterial, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_bust_material(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.BustView, F.text == 'Оформить заказ')
@retry_on_network_error()
async def checkout_from_bust_view(message: Message, state: FSMContext):
    await start_checkout(message, state)

@dp.message(Order.BustView, F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_bust_view(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.OrderMenu, F.text == 'Корсет')
@retry_on_network_error()
async def show_corset_menu(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Корсет с картиной'), KeyboardButton(text='Корсет из полотен')], [KeyboardButton(text='Корсет из джинсы'), KeyboardButton(text='Корсет из корсетной сетки')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer('Выберите тип корсета:', reply_markup=kb)
    await state.set_state(Order.CorsetMenu)

@dp.message(Order.OrderMenu, F.text == 'Аксессуары')
@retry_on_network_error()
async def show_accessories_menu(message: Message, state: FSMContext):
    """Показывает меню выбора модели аксессуаров"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Пояс для чулок'), KeyboardButton(text='Другие аксессуары')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer('Выберите модель аксессуара:', reply_markup=kb)
    await state.set_state(Order.AccessoriesMenu)

@dp.message(Order.AccessoriesMenu, F.text == 'Пояс для чулок')
@retry_on_network_error()
async def show_stock_belts_menu(message: Message, state: FSMContext):
    """Показывает меню выбора материала пояса для чулок"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Кружевной пояс для чулок'), KeyboardButton(text='Пояс для чулок из эластичной сетки')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer('Выберите материал пояса для чулок:', reply_markup=kb)
    await state.set_state(Order.StockBeltsMaterial)

@dp.message(Order.AccessoriesMenu, F.text == 'Другие аксессуары')
@retry_on_network_error()
async def show_other_accessories(message: Message, state: FSMContext):
    """Показывает слайдер с другими аксессуарами"""
    other_accessories = get_other_accessories()
    # микро-оптимизация: не пытаемся редактировать "старый" слайдер из другого меню
    await delete_previous_slider(message.chat.id, state)
    if not other_accessories:
        await message.answer('Другие аксессуары временно недоступны.')
        return

    await state.update_data(items=other_accessories, current_index=0, current_category='other_accessories')
    await show_item_slider(message.chat.id, state, other_accessories, 0, 'Другие аксессуары')
    await state.set_state(Order.OtherAccessoriesView)

    # ✅ Важно: меняем reply-клавиатуру, иначе остаются "старые" кнопки без обработчиков
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await message.answer('Листайте аксессуары. Нажмите «✅ Добавить в корзину» на карточке товара.', reply_markup=kb)

@retry_on_network_error()
def _load_all_accessories_rows():
    """Возвращает все строки категории 'Аксессуары'"""
    return load_data_from_master_cached(product_type='Аксессуары', cache_key='accessories_all_rows')


@retry_on_network_error()
def build_stock_belts_material_items_for_slider(material_name: str, color: str | None = None) -> list:
    """Создает элементы материалов поясов для чулок для слайдера.
    Если указан color — фильтрует по колонке 'Цвет' в той же строке.
    'Изображение материала' считается Telegram file_id или URL (не Drive).
    """
    all_rows = _load_all_accessories_rows()
    material_name_norm = (material_name or '').strip().lower()
    color_norm = (color or '').strip() if color else None
    items: list[dict] = []
    seen: set[str] = set()

    def _to_bool(v, default=True):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y", "да"):
            return True
        if s in ("false", "0", "no", "n", "нет"):
            return False
        return default

    for row in all_rows:
        mat_active = row.get('MaterialActive')
        if mat_active is not None and not _to_bool(mat_active, default=True):
            continue
        row_material = str(row.get('Материал', '') or '').strip()
        if not row_material:
            continue
        if row_material.lower() != material_name_norm:
            continue

        if color_norm:
            row_color = str(row.get('Цвет', '') or '').strip()
            if row_color != color_norm:
                continue

        rec: dict = {}
        id2 = row.get('ID 2')
        main_id = row.get('ID')
        try:
            if id2 and str(id2).strip() and (int(float(id2)) != 0):
                rec['ID'] = int(float(id2))
            elif main_id:
                # Если в таблице материалов нет 'ID 2', пытаемся стабильно получить его из MaterialSKU (например MAT0100 -> 100)
                sku_raw = row.get('MaterialSKU') or row.get('material_sku') or row.get('Артикул материала') or row.get('Артикул') or row.get('SKU')
                sku_s = str(sku_raw).strip() if sku_raw is not None else ''
                m_sku = re.search(r'(\d+)', sku_s) if sku_s else None
                if m_sku:
                    rec['ID'] = int(m_sku.group(1))
                else:
                    rec['ID'] = int(float(main_id)) * 1000
            else:
                rec['ID'] = abs(hash(row_material)) % 10**9
        except Exception:
            rec['ID'] = abs(hash(row_material)) % 10**9

        rec['Материал'] = row_material
        rec['Название'] = f'Материал: {row_material}'
        rec['Описание'] = f'Выбран материал: {row_material}' + (f' (цвет: {color_norm})' if color_norm else '')
        rec['Цена'] = 0
        rec['Тип'] = 'Аксессуары'
        rec['Категория'] = 'Пояс для чулок'

        img_raw = row.get('Изображение материала') or row.get('Изображение') or ''
        img = img_raw.strip() if isinstance(img_raw, str) else ''
        rec['Изображение'] = _normalize_image_source(img) if img else None

        dedupe_key = rec['Изображение'] or f"{row_material}|{color_norm or ''}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        items.append(rec)

    items.sort(key=lambda x: x.get('ID') or 0)
    return items


@retry_on_network_error()
def build_stock_belts_model_items_for_slider(selected_material: str) -> list:
    """Создает элементы моделей поясов для чулок для слайдера с фильтрацией по материалу"""
    print(f'🔍 ФУНКЦИЯ build_stock_belts_model_items_for_slider вызвана с материалом: {selected_material}')
    all_rows = _load_all_accessories_rows()
    models = []
    print(f'🔍 Всего строк в аксессуарах: {len(all_rows)}')

    # СЛОВАРЬ СООТВЕТСТВИЯ МАТЕРИАЛОВ И МОДЕЛЕЙ
    material_to_model_map = {
        'материал пояса: кружевной': 'кружевной пояс для чулок',
        'материал пояса: эластичная сетка': 'пояс для чулок из эластичной сетки',
        'кружевной материал': 'кружевной пояс для чулок',
        'эластичная сетка': 'пояс для чулок из эластичной сетки'
    }

    # Получаем целевую модель для выбранного материала
    target_model = material_to_model_map.get(selected_material.lower(), '')
    print(f'🔍 Целевая модель для материала "{selected_material}": "{target_model}"')

    for i, row in enumerate(all_rows):
        row_model = str(row.get('Модель', '') or '').strip()
        row_material = str(row.get('Материал', '') or '').strip()
        row_type = str(row.get('Тип', '') or '').strip()

        print(f"🔍 Строка {i}: Модель='{row_model}', Материал='{row_material}', Тип='{row_type}'")

        # ФИЛЬТРАЦИЯ: ищем модели, соответствующие ВЫБРАННОМУ МАТЕРИАЛУ
        # Не смотрим на row_material, а только на соответствие модели целевому материалу
        is_stock_belt = (
                'пояс' in row_model.lower() and
                'чулок' in row_model.lower() and
                target_model.lower() in row_model.lower()
        )

        if is_stock_belt:
            print(f'✅ Найден подходящий пояс: {row_model}')
            rec = {}
            try:
                rec['ID'] = int(float(row.get('ID')))
            except Exception:
                rec['ID'] = abs(hash(row_model)) % 10 ** 9
            rec['Модель'] = row_model
            rec['Название'] = row.get('Название') or row_model
            rec['Описание'] = f"Модель пояса: {rec['Название']}"
            rec['Цена'] = row.get('Цена') or 2500

            # ВАЖНО: устанавливаем ПРАВИЛЬНЫЙ материал из выбранного, а не из строки таблицы
            rec['Материал'] = selected_material  # Используем выбранный материал, а не из таблицы

            rec['Тип'] = 'Пояс для чулок'

            img = row.get('Изображение модели') or row.get('Изображение') or ''
            if isinstance(img, str) and img.strip():
                if img.startswith(('http://', 'https://')):
                    rec['Изображение'] = img
                elif re.match('^[a-zA-Z0-9_-]{20,200}$', img):
                    rec['Изображение'] = f'https://drive.google.com/uc?export=view&id={img}'
                else:
                    rec['Изображение'] = None
            else:
                rec['Изображение'] = None

            models.append(rec)
            print(f"✅ Добавлена модель: {rec['Название']} (ID: {rec['ID']}) с материалом: {rec['Материал']}")

    print(f'🔍 ИТОГО найдено моделей для материала "{selected_material}": {len(models)}')

    # Если модели не найдены, создаем тестовую модель
    if not models:
        print('⚠️ Модели не найдены, создаем тестовую')
        test_model = {
            'ID': 999999,
            'Модель': target_model or 'Пояс для чулок',
            'Название': target_model or 'Пояс для чулок',
            'Описание': f'Тестовая модель пояса для материала {selected_material}',
            'Цена': 2500,
            'Материал': selected_material,  # Используем выбранный материал
            'Тип': 'Аксессуары',
            'Изображение': None
        }
        models.append(test_model)

    models.sort(key=lambda x: (x.get('Название') or '', x.get('ID')))
    return models

@retry_on_network_error()
def get_other_accessories():
    """
    Возвращает список товаров, у которых в таблице:
    Тип = 'Другие аксессуары'
    """
    # грузим все строки по аксессуарам (как и раньше)
    all_rows = _load_all_accessories_rows()
    other_accessories: list[dict] = []

    TARGET_TYPE = 'Другие аксессуары'

    for row in all_rows:
        row_type = str(row.get('Тип', '') or '').strip()
        row_model = str(row.get('Модель', '') or '').strip()

        # 🔹 берём ТОЛЬКО те строки, где Тип == 'Другие аксессуары'
        if row_type.lower() != TARGET_TYPE.lower():
            continue

        # если в таблице вдруг пустая модель — пропускаем
        if not row_model:
            continue

        rec: dict = {}

        # ID — как и раньше, из таблицы, а если его нет – генерим
        try:
            rec['ID'] = int(float(row.get('ID')))
        except Exception:
            rec['ID'] = abs(hash(f"{row_type}_{row_model}")) % 10**9

        rec['Тип'] = row_type
        rec['Модель'] = row_model
        rec['Название'] = row.get('Название') or row_model
        rec['Описание'] = f"Аксессуар: {rec['Название']}"
        rec['Цена'] = row.get('Цена') or 1500

        # обработка картинки, как и было
        img = row.get('Изображение модели') or row.get('Изображение') or ''
        if isinstance(img, str) and img.strip():
            if img.startswith(('http://', 'https://')):
                rec['Изображение'] = img
            elif re.match(r'^[a-zA-Z0-9_-]{20,200}$', img):
                rec['Изображение'] = f"https://drive.google.com/uc?export=view&id={img}"
            else:
                rec['Изображение'] = None
        else:
            rec['Изображение'] = None

        other_accessories.append(rec)

    # сортировка — по названию и ID, чтобы слайдер был стабильным
    other_accessories.sort(key=lambda x: (x.get('Название') or '', x.get('ID')))
    return other_accessories



@dp.message(Order.StockBeltsMaterial, F.text == 'Кружевной пояс для чулок')
@retry_on_network_error()
async def show_lace_stock_belts_material(message: Message, state: FSMContext):
    # 🎨 сначала спросим цвет
    selected_material = 'Материал пояса: Кружевной'
    color_kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await state.update_data(selected_material=selected_material, current_category='stock_belts_material')
    await message.answer('Выберите цвет материала:', reply_markup=color_kb)
    await state.set_state(Order.StockBeltsColor)


@dp.message(Order.StockBeltsMaterial, F.text == 'Пояс для чулок из эластичной сетки')
@retry_on_network_error()
async def show_mesh_stock_belts_material(message: Message, state: FSMContext):
    # 🎨 сначала спросим цвет
    selected_material = 'Материал пояса: Эластичная сетка'
    color_kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await state.update_data(selected_material=selected_material, current_category='stock_belts_material')
    await message.answer('Выберите цвет материала:', reply_markup=color_kb)
    await state.set_state(Order.StockBeltsColor)


@dp.message(Order.StockBeltsColor)
@retry_on_network_error()
async def handle_stock_belts_color(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if text == 'Назад':
        await delete_previous_slider(message.chat.id, state)
        await show_stock_belts_menu(message, state)
        return
    if text == 'Корзина':
        await show_cart(message, state)
        return

    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text not in allowed_colors:
        await message.answer('Пожалуйста, выберите цвет кнопкой ниже.')
        return

    data = await state.get_data()
    selected_material = (data.get('selected_material') or '').strip()
    if not selected_material:
        await show_stock_belts_menu(message, state)
        return

    selected_color = text
    await state.update_data(stock_belts_selected_color=selected_color)
    remember_user_color(message.from_user.id, selected_color)
    remember_user_color(message.from_user.id, selected_color)

    # по твоему новому правилу — при смене/выборе цвета слайдер исчезает и показывается заново
    await delete_previous_slider(message.chat.id, state)
    material_items = build_stock_belts_material_items_for_slider(selected_material, color=selected_color)
    if not material_items:
        await message.answer(f"К сожалению, для цвета '{selected_color}' материалы временно недоступны.")
        return

    await state.update_data(
        items=material_items,
        current_index=0,
        current_category='stock_belts_material',
        selected_material=selected_material
    )
    title = 'Материалы: Пояса для чулок'
    await show_item_slider(message.chat.id, state, material_items, 0, title)

    await state.set_state(Order.StockBeltsMaterial)

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
            [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
            [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
        ]
    )
    await message.answer('Листайте материалы. Можно сменить цвет кнопками ниже:', reply_markup=kb)


@dp.message(Order.StockBeltsMaterial, F.text.in_({'Черный','Красный','Белый','Другие'}))
@retry_on_network_error()
async def handle_stock_belts_color_switch(message: Message, state: FSMContext):
    """Позволяет менять цвет пояса для чулок кнопками Черный/Красный/Белый/Другие после открытия слайдера."""
    text = (message.text or '').strip()
    allowed_colors = {'Черный', 'Красный', 'Белый', 'Другие'}
    if text not in allowed_colors:
        return

    data = await state.get_data()
    selected_material = (data.get('selected_material') or '').strip()
    if not selected_material:
        await message.answer('Сначала выберите материал пояса для чулок.')
        return

    selected_color = text
    await state.update_data(stock_belts_selected_color=selected_color)

    await delete_previous_slider(message.chat.id, state)
    material_items = build_stock_belts_material_items_for_slider(selected_material, color=selected_color)
    if not material_items:
        await message.answer(f"К сожалению, для цвета '{selected_color}' материалы временно недоступны.")
        return

    await state.update_data(
        items=material_items,
        current_index=0,
        current_category='stock_belts_material',
        selected_material=selected_material
    )
    await show_item_slider(message.chat.id, state, material_items, 0, 'Материалы: Пояса для чулок')




@dp.callback_query(Order.StockBeltsMaterial, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_stock_belts_material_to_cart(call: CallbackQuery, state: FSMContext):
    """Выбор материала пояса (в слайдере).

    ❗️Важно: материал НЕ добавляем в корзину на этом шаге.
    Сохраняем как pending и добавляем/объединяем только после выбора модели.
    """
    print('🎯 ВХОД В ОБРАБОТЧИК МАТЕРИАЛОВ ПОЯСОВ (add_stock_belts_material_to_cart)')
    try:
        item_id = int(call.data.split('_')[3])
        print(f'🎯 ID материала: {item_id}')
    except Exception as e:
        print(f'❌ Ошибка парсинга ID материала пояса: {e}')
        await call.answer('Ошибка выбора материала', show_alert=True)
        return

    data = await state.get_data()
    items = data.get('items', []) or []
    material_item = next((x for x in items if x.get('ID') == item_id), None)
    if not material_item:
        await call.answer('Материал не найден', show_alert=True)
        return

    # Формируем pending-материал (распознаваемый как "материал пояса", без модели)
    pending_material = material_item.copy()
    pending_material['Тип'] = 'Аксессуары'
    pending_material['Категория'] = 'Пояс для чулок'
    pending_material['Модель'] = ''  # материал — НЕ модель
    pending_material['is_stock_belt_material'] = True

    # На всякий случай чистим "висящие" материалы поясов из корзины (могли остаться со старых сессий)
    try:
        remove_previous_stock_belts_items(call.from_user.id)
    except Exception as e:
        print(f'⚠️ Не удалось очистить предыдущие материалы поясов: {e}')

    # Сохраняем выбранный материал в state, но НЕ добавляем в корзину
    selected_material = str(material_item.get('Материал') or '').strip()
    if not selected_material:
        await call.answer('Ошибка: не найдено название материала', show_alert=True)
        return

    await state.update_data(
        pending_stock_belt_material=pending_material,
        stockbelts_selected_material=selected_material
    )

    print('🎯 Загружаем модели для материала пояса...')
    model_items = build_stock_belts_model_items_for_slider(selected_material)
    print(f'🎯 Найдено моделей: {len(model_items)}')

    if not model_items:
        await call.answer('Модели для этого материала не найдены', show_alert=True)
        return

    # Удаляем слайдер материалов и показываем слайдер моделей
    try:
        await delete_previous_slider(call.message.chat.id, state)
    except Exception:
        pass

    await state.update_data(items=model_items, cur_index=0)
    await state.set_state(Order.StockBeltsModel)

    await show_item_slider(call.message.chat.id, state, model_items, 0, 'Модели: Пояса для чулок')

    # ❗️На этапе выбора модели не показываем кнопки выбора цвета
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Назад к материалам')],
            [KeyboardButton(text='Перейти в корзину'), KeyboardButton(text='Каталог товаров')]
        ]
    )
    await call.message.answer('Теперь выберите модель пояса:', reply_markup=kb)

    await call.answer('✅ Материал выбран. Теперь выберите модель.', show_alert=False)


@dp.callback_query(Order.StockBeltsModel, F.data.startswith('add_to_cart_'))
@retry_on_network_error()
async def add_stock_belts_model_to_cart(call: CallbackQuery, state: FSMContext):
    """Добавляет модель пояса в корзину.

    Логика:
    - Если на шаге материалов был выбран материал, он лежит в state как pending_stock_belt_material.
      Его добавляем в корзину прямо здесь (если ещё не добавлен).
    - Без материала модель добавлять нельзя.
    """
    try:
        item_id = int(call.data.split('_')[3])
        print(f'🎯 Добавление модели пояса с ID: {item_id}')
    except Exception as e:
        print(f'❌ Ошибка парсинга ID модели: {e}')
        await call.answer('Ошибка добавления', show_alert=True)
        return

    data = await state.get_data()
    items = data.get('items', []) or []
    print(f'🎯 Всего items в состоянии: {len(items)}')

    # Цвет пояса (выбирается на шаге StockBeltsColor). Важно записать его в сам товар,
    # чтобы он корректно отображался в корзине независимо от текущего FSM-состояния.
    selected_color = (data.get('stock_belts_selected_color') or data.get('selected_color') or '').strip()

    item = next((x for x in items if x.get('ID') == item_id), None)
    if not item:
        print(f'❌ Модель с ID {item_id} не найдена')
        await call.answer('Модель не найдена', show_alert=True)
        return

    print(f"🎯 Найдена модель пояса: {item.get('Модель')} (ID: {item.get('ID')})")

    user_id = call.from_user.id
    cart = user_carts.get(user_id) or []

    # 1) Пробуем взять pending материал из state (как в бюстах)
    pending_material = data.get('pending_stock_belt_material')
    if pending_material:
        print(f"✅ Найден pending материал пояса: {pending_material.get('Материал')} (ID: {pending_material.get('ID')})")

        if selected_color and not str(pending_material.get('Цвет') or '').strip():
            pending_material['Цвет'] = selected_color

        # если ещё не в корзине — добавляем
        already_in_cart = any(
            (it.get('ID') == pending_material.get('ID')) and
            (it.get('Материал') == pending_material.get('Материал')) and
            (not it.get('Модель')) and
            (it.get('is_stock_belt_material') or 'материал пояса' in str(it.get('Материал', '')).lower())
            for it in cart
        )
        if not already_in_cart:
            add_item_to_cart(user_id, pending_material)
            print("🛒 Материал пояса добавлен в корзину (из pending_stock_belt_material)")
            cart = user_carts.get(user_id) or []  # освежаем после add_item_to_cart

        # pending отработал — сбрасываем
        await state.update_data(pending_stock_belt_material=None)

    # 2) Проверяем, что материал пояса реально есть в корзине (иначе не даём добавить модель)
    has_material = False
    material_in_cart = None

    for item_cart in (cart or []):
        is_belt_material = (
            item_cart.get('Материал')
            and (not item_cart.get('Модель'))
            and (
                ('материал пояса' in str(item_cart.get('Материал', '')).lower())
                or item_cart.get('is_stock_belt_material')
            )
            and item_cart.get('Тип') in ['Аксессуары', 'Пояс для чулок']
        )
        if is_belt_material:
            has_material = True
            material_in_cart = item_cart
            print(f"✅ Найден материал пояса: {material_in_cart.get('Материал')} (ID: {material_in_cart.get('ID')})")
            break

    print(f"🎯 Проверка материала пояса в корзине: {has_material}")
    if not has_material:
        print('❌ В корзине нет материала пояса')
        await call.answer('❌ Сначала выберите материал пояса', show_alert=True)
        return

    # 3) Добавляем модель в корзину (функция add_item_to_cart у вас умеет объединять модель+материал)
    item["is_lingerie_set"] = True
    item["is_panties"] = False
    if selected_color and not str(item.get('Цвет') or '').strip():
        item['Цвет'] = selected_color
    add_item_to_cart(user_id, item)
    print(f"✅ Модель пояса добавлена в корзину: {item.get('Название')}")

    await call.answer(f"Модель '{item.get('Название')}' добавлена в корзину", show_alert=False)
    await delete_previous_slider(call.message.chat.id, state)

    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text='Перейти в корзину')],
        [KeyboardButton(text='Каталог товаров')]
    ])

    await call.message.answer(
        f"✅ *{escape_markdown(item.get('Название', 'Модель'))}* добавлена в вашу корзину!\n\n"
        f"Вы можете выбрать что-то ещё или перейти в корзину.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )

    await state.set_state(Order.StockBeltsView)


@dp.message(Order.StockBeltsMaterial, F.text == 'Назад')
@retry_on_network_error()
async def back_to_accessories_menu_from_stock_belts(message: Message, state: FSMContext):
    """Назад в поясах:
    - если открыт слайдер материалов (после выбора цвета) — возвращаемся к выбору цвета
    - если мы в меню выбора типа пояса (4 кнопки) — возвращаемся в меню аксессуаров
    """
    data = await state.get_data()
    current_category = data.get('current_category')
    has_slider = bool(data.get('items')) and current_category == 'stock_belts_material'

    # убираем слайдер (если был)
    try:
        await delete_previous_slider(message.chat.id, state)
    except Exception:
        pass

    if has_slider:
        # шаг назад: к выбору цвета
        selected_material = (data.get('selected_material') or '').strip()
        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Черный'), KeyboardButton(text='Красный')],
                [KeyboardButton(text='Белый'), KeyboardButton(text='Другие')],
                [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]
            ]
        )
        await state.update_data(
            items=[],
            current_index=0,
            current_category=None,
            stock_belts_selected_color=None,
            stockbelts_selected_material=None,
        )
        await message.answer(
            f"Выберите цвет для: {selected_material}" if selected_material else "Выберите цвет пояса для чулок:",
            reply_markup=kb
        )
        await state.set_state(Order.StockBeltsColor)
        return

    # шаг назад: в меню аксессуаров
    await go_back_with_slider_cleanup(message, state, show_accessories_menu)

@dp.message(Order.StockBeltsMaterial, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_stock_belts_material(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.StockBeltsView, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_stock_belts_view(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.StockBeltsView, F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_stock_belts_view(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.OtherAccessoriesView, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_other_accessories(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.OtherAccessoriesView, F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_other_accessories(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.OtherAccessoriesView, F.text == 'Корзина')
@retry_on_network_error()
async def cart_from_other_accessories(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.OtherAccessoriesView, F.text == 'Назад')
@retry_on_network_error()
async def back_from_other_accessories(message: Message, state: FSMContext):
    # Убираем слайдер и возвращаемся в меню аксессуаров
    await go_back_with_slider_cleanup(message, state, show_accessories_menu)

@dp.message(Order.StockBeltsMaterial, F.text.in_({'Перейти в корзину','Каталог товаров'}))
@dp.message(Order.StockBeltsModel, F.text.in_({'Перейти в корзину','Каталог товаров'}))
@dp.message(Order.StockBeltsView, F.text.in_({'Перейти в корзину','Каталог товаров'}))
@dp.message(Order.OtherAccessoriesView, F.text.in_({'Перейти в корзину','Каталог товаров'}))
@retry_on_network_error()
async def handle_accessories_view_buttons(message: Message, state: FSMContext):
    """Обрабатывает кнопки в состояниях просмотра аксессуаров"""
    if message.text == 'Перейти в корзину':
        await show_cart(message, state)
    elif message.text == 'Каталог товаров':
        await make_order(message, state)
    elif message.text == 'Назад к материалам':
        current_state = await state.get_state()
        if current_state == Order.StockBeltsModel:
            await go_back_with_slider_cleanup(message, state, show_stock_belts_menu)
    elif message.text == 'Назад':
        current_state = await state.get_state()
        if current_state == Order.StockBeltsMaterial:
            await go_back_with_slider_cleanup(message, state, show_accessories_menu)
        elif current_state == Order.OtherAccessoriesView:
            await go_back_with_slider_cleanup(message, state, show_accessories_menu)

@dp.message(Order.AccessoriesMenu, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_accessories(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, make_order)

@dp.message(Order.AccessoriesMenu, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_accessories(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.OrderMenu, F.text == 'Сертификат')
@retry_on_network_error()
async def show_certificate_format_menu(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Электронный сертификат'), KeyboardButton(text='Бумажный сертификат')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer('Выберите формат сертификата:', reply_markup=kb)
    await state.set_state(Order.CertificateFormat)

@dp.message(Order.StockBeltsMaterial, F.text == 'Назад')
@retry_on_network_error()
async def back_to_accessories_menu(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, show_accessories_menu)

@dp.message(Order.AccessoriesMenu, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_accessories(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, make_order)

@dp.message(Order.CorsetMenu, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_corset(message: Message, state: FSMContext):
    await go_back_with_slider_cleanup(message, state, make_order)

@dp.message(Order.StockBeltsMaterial, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_stock_belts_material(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.AccessoriesMenu, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_accessories(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.CorsetMenu, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_corset(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.OrderMenu, F.text == 'Корзина')
@retry_on_network_error()
async def back_to_cart_from_order(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.CorsetMenu)
@retry_on_network_error()
async def handle_corset_menu(message: Message, state: FSMContext):
    if message.text == 'Назад':
        await back_to_order_menu_from_corset(message, state)
        return
    elif message.text == 'Корзина':
        await back_to_cart_from_corset(message, state)
        return
    corset_data = None
    category_name = ''
    if message.text == 'Корсет с картиной':
        corset_data = get_corset_with_painting()
        category_name = 'Корсеты с картиной'
        await state.update_data(current_category='corset_painting')
    elif message.text == 'Корсет из джинсы':
        corset_data = get_corset_denim()
        category_name = 'Корсет из джинсы'
        await state.update_data(current_category='corset_denim')
    elif message.text == 'Корсет из полотен':
        corset_data = get_corset_tapestry()
        category_name = 'Корсет из полотен'
        await state.update_data(current_category='corset_tapestry')
    elif message.text == 'Корсет из корсетной сетки':
        corset_data = get_corset_mesh()
        category_name = 'Корсеты из корсетной сетки'
        await state.update_data(current_category='corset_mesh')
    else:
        await message.answer('Пожалуйста, выберите тип корсета из предложенных вариантов.')
        return
    if not corset_data:
        await message.answer('Товары временно недоступны.')
        return
    await state.update_data(current_index=0, items=corset_data)
    await show_item_slider(message.chat.id, state, corset_data, 0, category_name)
    await state.set_state(Order.CorsetView)

@dp.message(Order.CorsetView)
@retry_on_network_error()
async def handle_corset_view_buttons(message: Message, state: FSMContext):
    # ✅ Если пользователь выбирает другой тип корсета, пока открыт слайдер —
    # удаляем текущий слайдер и открываем новый
    corset_type_buttons = {
        'Корсет с картиной',
        'Корсет из джинсы',
        'Корсет из полотен',
        'Корсет из корсетной сетки',
    }
    if message.text in corset_type_buttons:
        await delete_previous_slider(message.chat.id, state)
        await handle_corset_menu(message, state)
        return
    if message.text == 'Назад':
        await go_back_with_slider_cleanup(message, state, show_corset_menu)
    elif message.text == 'Корзина':
        await show_cart(message, state)
    elif message.text == 'Перейти в корзину':
        await show_cart(message, state)
    elif message.text == 'Оформить заказ':
        await start_checkout(message, state)
    elif message.text == 'Каталог товаров':
        await make_order(message, state)
    else:
        await state.set_state(Order.CorsetMenu)
        await handle_corset_menu(message, state)

@dp.message(F.text == 'Каталог товаров')
@retry_on_network_error()
async def global_product_list(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.MainMenu, F.text == 'Назад')
@retry_on_network_error()
async def back_to_order_menu_from_certificate(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.MainMenu, F.text == 'Перейти в корзину')
@retry_on_network_error()
async def go_to_cart_from_main(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.MainMenu, F.text == 'Оформить заказ')
@retry_on_network_error()
async def checkout_from_main(message: Message, state: FSMContext):
    await start_checkout(message, state)

@dp.callback_query(F.data == 'noop')
@retry_on_network_error()
async def noop_handler(call: CallbackQuery):
    try:
        await call.answer()
    except Exception:
        pass

@dp.message(Order.CertificateFormat, F.text == 'Электронный сертификат')
@retry_on_network_error()
async def handle_electronic_certificate(message: Message, state: FSMContext):
    certificate_rules = '📄 *Правила использования сертификата:*\n\n•    Сертификат действителен в течение 1 года с даты покупки.\n•    Если сумма покупки превышает номинал сертификата — разница оплачивается дополнительно.\n•    Если сумма меньше номинала — остаток не возвращается.\n•    Сертификатом можно оплатить только товары магазина, не распространяется на доставку.\n•    Сертификат не подлежит обмену или возврату.\n•    При утере или повреждении сертификата восстановление невозможно.\n•    Для использования необходимо предъявить оригинал сертификата (его фото/скриншот)\n\n•    Продолжая оформление, вы соглашаетесь с условия использования сертификата.'
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='6000'), KeyboardButton(text='12000')], [KeyboardButton(text='Другая сумма'), KeyboardButton(text='Каталог товаров')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer(certificate_rules, reply_markup=kb)
    await state.update_data(certificate_type='electronic')
    await state.set_state(Order.ElectronicCertificate)

@dp.message(Order.CertificateFormat, F.text == 'Бумажный сертификат')
@retry_on_network_error()
async def handle_paper_certificate(message: Message, state: FSMContext):
    certificate_rules = '📄 *Правила использования сертификата:*\n\n•    Сертификат действителен в течение 1 года с даты покупки.\n•    Если сумма покупки превышает номинал сертификата — разница оплачивается дополнительно.\n•    Если сумма меньше номинала — остаток не возвращается.\n•    Сертификатом можно оплатить только товары магазина, не распространяется на доставку.\n•    Сертификат не подлежит обмену или возврату.\n•    При утере или повреждении сертификата восстановление невозможно.\n•    Для использования необходимо предъявить оригинал сертификата (его фото/скриншот)\n\n•    Продолжая оформление, вы соглашаетесь с условия использования сертификата.'
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='6000'), KeyboardButton(text='12000')], [KeyboardButton(text='Другая сумма'), KeyboardButton(text='Каталог товаров')], [KeyboardButton(text='Назад'), KeyboardButton(text='Корзина')]])
    await message.answer(certificate_rules, reply_markup=kb)
    await state.update_data(certificate_type='paper')
    await state.set_state(Order.PaperCertificate)

@dp.message(Order.ElectronicCertificate, F.text == 'Назад')
@retry_on_network_error()
async def back_from_electronic_certificate(message: Message, state: FSMContext):
    await show_certificate_format_menu(message, state)

@dp.message(Order.ElectronicCertificate, F.text == 'Корзина')
@retry_on_network_error()
async def cart_from_electronic_certificate(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.PaperCertificate, F.text == 'Назад')
@retry_on_network_error()
async def back_from_paper_certificate(message: Message, state: FSMContext):
    await show_certificate_format_menu(message, state)

@dp.message(Order.PaperCertificate, F.text == 'Корзина')
@retry_on_network_error()
async def cart_from_paper_certificate(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.CertificateFormat, F.text == 'Назад')
@retry_on_network_error()
async def back_from_certificate_format(message: Message, state: FSMContext):
    await make_order(message, state)

@dp.message(Order.CertificateFormat, F.text == 'Корзина')
@retry_on_network_error()
async def cart_from_certificate_format(message: Message, state: FSMContext):
    await show_cart(message, state)

@dp.message(Order.ElectronicCertificate, F.text.in_(['6000', '12000']))
@retry_on_network_error()
async def handle_electronic_certificate_amount(message: Message, state: FSMContext):
    amount = message.text.replace(',', '').replace(' ', '')
    await state.update_data(certificate_amount=amount)
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Оформить заказ')]])
    await message.answer('Введите электронную почту для получения сертификата:', reply_markup=kb)
    await state.set_state(Order.CertificateEmail)


@dp.message(Order.PaperCertificate, F.text.in_(['6000', '12000']))
@retry_on_network_error()
async def handle_paper_certificate_amount(message: Message, state: FSMContext):
    amount = message.text.replace(',', '').replace(' ', '')
    await state.update_data(certificate_amount=amount)

    # Создаем и добавляем бумажный сертификат в корзину (аналогично электронному)
    certificate_item = {
        'ID': f'certificate_{int(datetime.now().timestamp())}',
        'Название': f'Бумажный сертификат {amount} руб.',
        'Цена': amount,
        'Тип': 'Сертификат',
        'Модель': 'Бумажный сертификат',
        'is_certificate': True,
        'certificate_type': 'paper'
    }
    add_item_to_cart(message.from_user.id, certificate_item)

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')],
            [KeyboardButton(text='Оформить заказ'), KeyboardButton(text='Каталог товаров')]
        ]
    )
    await message.answer(f'✅ Бумажный сертификат номиналом {amount} руб. добавлен в корзину!', reply_markup=kb)
    await state.set_state(Order.MainMenu)

@dp.message(Order.ElectronicCertificate, F.text == 'Другая сумма')
@retry_on_network_error()
async def handle_electronic_custom_amount(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Оформить заказ')]])
    await message.answer('Введите желаемую сумму сертификата:', reply_markup=kb)
    await state.set_state(Order.CertificateAmount)

@dp.message(Order.PaperCertificate, F.text == 'Другая сумма')
@retry_on_network_error()
async def handle_paper_custom_amount(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')],
            [KeyboardButton(text='Оформить заказ')]
        ]
    )
    await message.answer('Введите желаемую сумму сертификата:', reply_markup=kb)
    await state.set_state(Order.CertificateAmount)

@dp.message(Order.CertificateEmail)
@retry_on_network_error()
async def handle_certificate_email(message: Message, state: FSMContext):
    if message.text in ['Назад', 'Перейти в корзину', 'Оформить заказ']:
        if message.text == 'Назад':
            await make_order(message, state)
        elif message.text == 'Перейти в корзину':
            await show_cart(message, state)
        elif message.text == 'Оформить заказ':
            await start_checkout(message, state)
        return
    if message.text == 'Назад':
        await state.set_state(Order.ElectronicCertificate)
        await handle_electronic_certificate(message, state)
        return
    if '@' not in message.text or '.' not in message.text:
        await message.answer('Пожалуйста, введите корректный email адрес:')
        return
    email = message.text
    data = await state.get_data()
    amount = data.get('certificate_amount', '0')
    certificate_item = {'ID': f'certificate_{int(datetime.now().timestamp())}', 'Название': f'Электронный сертификат {amount} руб.', 'Цена': amount, 'Тип': 'Сертификат', 'Модель': 'Электронный сертификат', 'Email': email, 'is_certificate': True, 'certificate_type': 'electronic'}
    add_item_to_cart(message.from_user.id, certificate_item)
    await message.answer(f'✅ Электронный сертификат номиналом {amount} руб. добавлен в корзину!\nСертификат будет отправлен на email: {email}')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')], [KeyboardButton(text='Оформить заказ'), KeyboardButton(text='Каталог товаров')]])
    await message.answer('Вы можете выбрать что-то еще или перейти в корзину.', reply_markup=kb)
    await state.set_state(Order.MainMenu)

@dp.message(F.text == 'Назад к материалам')
@retry_on_network_error()
async def back_to_stock_belts_materials(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state == Order.StockBeltsModel:
        await go_back_with_slider_cleanup(message, state, show_stock_belts_menu)

@dp.message(F.text == 'Перейти в корзину')
@retry_on_network_error()
async def back_to_cart_from_stock_belts_model(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state == Order.StockBeltsModel:
        await show_cart(message, state)

@dp.message(F.text == 'Каталог товаров')
@retry_on_network_error()
async def catalog_from_stock_belts_model(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state == Order.StockBeltsModel:
        await make_order(message, state)


@dp.message(Order.CertificateAmount)
@retry_on_network_error()
async def handle_custom_certificate_amount(message: Message, state: FSMContext):
    if message.text in ['Назад', 'Перейти в корзину', 'Оформить заказ']:
        if message.text == 'Назад':
            await make_order(message, state)
        elif message.text == 'Перейти в корзину':
            await show_cart(message, state)
        elif message.text == 'Оформить заказ':
            await start_checkout(message, state)
        return

    if message.text == 'Назад':
        data = await state.get_data()
        if data.get('certificate_type') == 'electronic':
            await state.set_state(Order.ElectronicCertificate)
            await handle_electronic_certificate(message, state)
        else:
            await state.set_state(Order.PaperCertificate)
            await handle_paper_certificate(message, state)
        return

    if not _is_number(message.text):
        await message.answer('Пожалуйста, введите числовое значение суммы:')
        return

    amount = message.text
    await state.update_data(certificate_amount=amount)
    data = await state.get_data()

    if data.get('certificate_type') == 'electronic':
        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')],
                [KeyboardButton(text='Оформить заказ')]
            ]
        )
        await message.answer('Введите электронную почту для получения сертификата:', reply_markup=kb)
        await state.set_state(Order.CertificateEmail)
    else:
        # Для бумажных сертификатов - сразу добавляем в корзину (аналогично фиксированным суммам)
        certificate_item = {
            'ID': f'certificate_{int(datetime.now().timestamp())}',
            'Название': f'Бумажный сертификат {amount} руб.',
            'Цена': amount,
            'Тип': 'Сертификат',
            'Модель': 'Бумажный сертификат',
            'is_certificate': True,
            'certificate_type': 'paper'
        }
        add_item_to_cart(message.from_user.id, certificate_item)

        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Назад'), KeyboardButton(text='Перейти в корзину')],
                [KeyboardButton(text='Оформить заказ'), KeyboardButton(text='Каталог товаров')]
            ]
        )
        await message.answer(f'✅ Бумажный сертификат номиналом {amount} руб. добавлен в корзину!', reply_markup=kb)
        await state.set_state(Order.MainMenu)

@dp.message(Order.CartView, F.text == 'Оформить заказ')
@retry_on_network_error()
async def start_checkout(message: Message, state: FSMContext):
    _lock = get_action_lock(message.from_user.id, "start_checkout")
    if _lock.locked():
        try:
            await message.answer('⏳ Уже открываю оформление, секунду...')
        except Exception:
            pass
        return
    await _lock.acquire()
    try:
        user_id = message.from_user.id
        cart = user_carts.get(user_id)
        if not cart:
            await message.answer('Ваша корзина пуста.')
            return

        print("🔍 НАЧАЛО ПРОВЕРКИ ЗАКАЗА ПЕРЕД ОФОРМЛЕНИЕМ")
        cart_names = [item.get('Название') or f"ID: {item.get('ID')}" for item in cart]
        print(f"🔍 Содержимое корзины: {cart_names}")

        # Проверка корректности заказа бюста
        is_valid, error_msg = validate_bust_order(cart)
        if not is_valid:
            print(f"❌ Ошибка валидации бюста: {error_msg}")
            await message.answer(f'❌ {error_msg}\n\nПожалуйста, исправьте состав заказа бюста.')
            return

        # Проверка корректности заказа трусиков
        is_valid_panties, error_msg_panties = validate_panties_order(cart)
        if not is_valid_panties:
            print(f"❌ Ошибка валидации трусиков: {error_msg_panties}")
            await message.answer(f'❌ {error_msg_panties}\n\nПожалуйста, исправьте состав заказа трусиков.')
            return

        # Проверка корректности заказа поясов для чулок
        is_valid_belts, error_msg_belts = validate_stock_belts_order(cart)
        if not is_valid_belts:
            print(f"❌ Ошибка валидации поясов: {error_msg_belts}")
            await message.answer(f'❌ {error_msg_belts}\n\nПожалуйста, исправьте состав заказа поясов.')
            return

        print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ, ПЕРЕХОД К ОФОРМЛЕНИЮ")

        privacy_text = '🔒 *ПОЛИТИКА КОНФИДЕНЦИАЛЬНОСТИ*\n\n Для оформления и исполнения вашего заказа нам необходимо обработать некоторые персональные данные, включая:\n• Контактные данные (телефон, e-mail)\n• Адрес доставки (при необходимости)\n• Индивидуальные мерки для пошива изделий (при необходимости)\n• Фотографии, предоставленные для точного подбора размера или модели\n Ваши персональные данные обрабатываются исключительно в целях выполнения заказа, обеспечения обратной связи и улучшения качества обслуживания.\n Вся полученная информация хранится в защищённой системе и не передаётся третьим лицам без вашего согласия, за исключением случаев, предусмотренных законодательством.\n Продолжая оформление заказа, вы подтверждаете согласие на обработку ваших персональных данных в соответствии с настоящей Политикой конфиденциальности.\n'
        kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='✅ Да, согласен')], [KeyboardButton(text='❌ Нет, не согласен')]])
        await message.answer(privacy_text, reply_markup=kb)
        await state.set_state(Order.PrivacyPolicy)


    finally:
        if _lock.locked():
            _lock.release()
@dp.message(Order.PrivacyPolicy, F.text == '✅ Да, согласен')
@retry_on_network_error()
async def privacy_agreed(message: Message, state: FSMContext):
    user_id = message.from_user.id
    cart = user_carts.get(user_id)
    has_only_certificates = all((item.get('is_certificate') for item in cart)) if cart else False

    if has_only_certificates:
        kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='📱 Отправить номер', request_contact=True)],
                                           [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
        await message.answer('✅ *Теперь отправьте номер телефона для связи:*', reply_markup=kb)
        await state.set_state(Order.Phone)
        return

    # ПРОВЕРКА ТОВАРОВ, ТРЕБУЮЩИХ МЕРОК
    has_corsets = any((
        'корсет' in str(item.get('Тип', '')).lower() or
        'корсет' in str(item.get('Категория', '')).lower() or
        'корсет' in str(item.get('Модель', '')).lower() or
        'корсет' in str(item.get('Название', '')).lower()
        for item in cart
    ))

    has_bust_items = any((
        'бюст' in str(item.get('Тип', '')).lower() or
        'бюст' in str(item.get('Категория', '')).lower() or
        'бюст' in str(item.get('Модель', '')).lower() or
        'бюст' in str(item.get('Название', '')).lower() or
        (item.get('Материал') and 'бюст' in str(item.get('Материал', '')).lower())
        for item in cart
    ))

    has_panties = any((
        'трусики' in str(item.get('Тип', '')).lower() or
        'трусики' in str(item.get('Категория', '')).lower() or
        'трусики' in str(item.get('Модель', '')).lower() or
        'трусики' in str(item.get('Название', '')).lower() or
        item.get('is_panties')
        for item in cart
    ))

    has_stock_belts = any((
        'пояс' in str(item.get('Модель', '')).lower() and
        'чулок' in str(item.get('Модель', '')).lower() or
        item.get('is_stock_belt')
        for item in cart
    ))

    print(f"🔍 ДЕТАЛЬНЫЙ АНАЛИЗ КОРЗИНЫ ДЛЯ МЕРОК:")
    print(f"   - Корсеты: {has_corsets}")
    print(f"   - Бюсты: {has_bust_items}")
    print(f"   - Трусики: {has_panties}")
    print(f"   - Пояса: {has_stock_belts}")

    # Если есть товары, требующие мерок - показываем инструкцию
    if has_corsets or has_bust_items or has_panties or has_stock_belts:
        await show_measurement_guide(message.chat.id)

    # ОПРЕДЕЛЯЕМ КАКИЕ МЕРКИ НУЖНЫ ИСХОДЯ ИЗ ТОВАРОВ В КОРЗИНЕ
    needed_measurements = set()

    if has_bust_items:
        needed_measurements.update(['horizontal_arc', 'bust', 'underbust'])

    if has_corsets:
        needed_measurements.update(['bust', 'underbust', 'waist'])

    if has_panties or has_stock_belts:
        needed_measurements.update(['waist', 'hips'])

    print(f"🔍 Нужные мерки: {needed_measurements}")

    # ЗАПРАШИВАЕМ МЕРКИ В ПРАВИЛЬНОЙ ПОСЛЕДОВАТЕЛЬНОСТИ
    if 'horizontal_arc' in needed_measurements:
        await message.answer('Введите горизонтальную дугу (в см):',
                             reply_markup=ReplyKeyboardRemove())
        await state.set_state(Order.HorizontalArc)
        await state.update_data(needed_measurements=list(needed_measurements))
    elif 'bust' in needed_measurements:
        await message.answer('Введите обхват груди (в см):',
                             reply_markup=ReplyKeyboardRemove())
        await state.set_state(Order.Bust)
        await state.update_data(needed_measurements=list(needed_measurements))
    elif 'waist' in needed_measurements:
        await message.answer('Введите обхват талии (в см):',
                             reply_markup=ReplyKeyboardRemove())
        await state.set_state(Order.Waist)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        # Если нет товаров, требующих мерок - переходим к телефону
        kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='📱 Отправить номер', request_contact=True)],
                                           [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
        await message.answer('✅ *Теперь отправьте номер телефона для связи:*', reply_markup=kb)
        await state.set_state(Order.Phone)

@dp.message(Order.PrivacyPolicy, F.text == '❌ Нет, не согласен')
@retry_on_network_error()
async def privacy_declined(message: Message, state: FSMContext):
    remove_kb = ReplyKeyboardRemove()
    await message.answer('❌ Без согласия на обработку персональных данных мы не сможем обработать ваш заказ.\n\nЕсли передумаете, можете оформить заказ позже.', reply_markup=remove_kb)
    await asyncio.sleep(2)
    await cmd_start(message, state)


@dp.message(Order.Waist)
@retry_on_network_error()
async def get_waist(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(waist=message.text)

    data = await state.get_data()
    needed_measurements = set(data.get('needed_measurements', []))
    needed_measurements.discard('waist')

    if 'hips' in needed_measurements:
        await message.answer('Введите обхват бедер (в см):')
        await state.set_state(Order.Hips)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        await proceed_to_order_notes(message, state)


@dp.message(Order.Hips)
@retry_on_network_error()
async def get_hips(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(hips=message.text)
    await proceed_to_order_notes(message, state)

@retry_on_network_error()
async def proceed_to_order_notes(message: Message, state: FSMContext):
    """Переход к пожеланиям после завершения всех мерок"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True,
                             keyboard=[[KeyboardButton(text='Пропустить')], [KeyboardButton(text='Главное меню')]])
    await message.answer('✅ *Все мерки сохранены!*\n\n💭 Пожалуйста, укажите все ваши пожелания (по моделям, цвету, материалам, срокам и т. д.) в одном сообщении, чтобы система могла корректно сохранить всю информацию.',
                         reply_markup=kb)
    await state.set_state(Order.OrderNotes)

@retry_on_network_error()
async def proceed_to_next_step(message: Message, state: FSMContext, needed_measurements: set):
    """Переход к следующему шагу после завершения всех мерок"""
    if needed_measurements:
        # Еще есть мерки для ввода
        if 'bust' in needed_measurements:
            await message.answer('Введите обхват груди (в см):')
            await state.set_state(Order.Bust)
        elif 'waist' in needed_measurements:
            await message.answer('Введите обхват талии (в см):')
            await state.set_state(Order.Waist)
        elif 'hips' in needed_measurements:
            await message.answer('Введите обхват бедер (в см):')
            await state.set_state(Order.Hips)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        # Все мерки завершены
        kb = ReplyKeyboardMarkup(resize_keyboard=True,
                                 keyboard=[[KeyboardButton(text='Пропустить')], [KeyboardButton(text='Главное меню')]])
        await message.answer('✅ *Все мерки сохранены!*\n\n💭 Пожалуйста, укажите все ваши пожелания (по моделям, цвету, материалам, срокам и т. д.) в одном сообщении, чтобы система могла корректно сохранить всю информацию.',
                             reply_markup=kb)
        await state.set_state(Order.OrderNotes)


@dp.message(Order.HorizontalArc)
@retry_on_network_error()
async def get_horizontal_arc(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(horizontal_arc=message.text)

    data = await state.get_data()
    needed_measurements = set(data.get('needed_measurements', []))
    needed_measurements.discard('horizontal_arc')

    if 'bust' in needed_measurements:
        await message.answer('Введите обхват груди (в см):')
        await state.set_state(Order.Bust)
        await state.update_data(needed_measurements=list(needed_measurements))
    else:
        await proceed_to_order_notes(message, state)

@dp.message(Order.Hips)
@retry_on_network_error()
async def get_hips(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    if not _is_number(message.text):
        await message.answer('Введите число:')
        return
    await state.update_data(hips=message.text)
    await proceed_to_order_notes(message, state)

@retry_on_network_error()
async def proceed_to_order_notes(message: Message, state: FSMContext):
    """Переход к пожеланиям после завершения всех мерок"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True,
                             keyboard=[[KeyboardButton(text='Пропустить')], [KeyboardButton(text='Главное меню')]])
    await message.answer('✅ *Все мерки сохранены!*\n\n💭 Пожалуйста, укажите все ваши пожелания (по моделям, цвету, материалам, срокам и т. д.) в одном сообщении, чтобы система могла корректно сохранить всю информацию.',
                         reply_markup=kb)
    await state.set_state(Order.OrderNotes)


@dp.message(Order.OrderNotes)
@retry_on_network_error()
async def get_order_notes(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return

    if message.text == 'Пропустить':
        await state.update_data(order_notes='Не указано')
    else:
        await state.update_data(order_notes=message.text)

    user_id = message.from_user.id
    cart = user_carts.get(user_id)
    has_only_certificates = all((item.get('is_certificate') for item in cart)) if cart else False

    if has_only_certificates:
        await state.update_data(photo_id=None)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text='📱 Отправить номер', request_contact=True)],
                [KeyboardButton(text='Главное меню')]
            ],
            resize_keyboard=True
        )
        await message.answer('✅ *Теперь отправьте номер телефона для связи:*', reply_markup=kb)
        await state.set_state(Order.Phone)
        return

    # УЛУЧШЕННАЯ ПРОВЕРКА ДЛЯ ФОТО
    has_corsets = any((
        'Корсет' in str(item.get('Тип', '')) or
        'Корсет' in str(item.get('Категория', '')) or
        'Комплект' in str(item.get('Тип', '')) or
        ('Комплект' in str(item.get('Категория', '')))
        for item in cart
    ))

    has_bust_items = any((
        'Бюст' in str(item.get('Тип', '')) or
        'Бюст' in str(item.get('Категория', '')) or
        'Бюст' in str(item.get('Модель', '')) or
        ('Бюст' in str(item.get('Название', ''))) or
        (item.get('Материал') and 'бюст' in str(item.get('Материал', '')).lower())
        for item in cart
    ))

    print(f"🔍 Проверка для фото:")
    print(f"   - Корсеты: {has_corsets}")
    print(f"   - Бюсты: {has_bust_items}")

    # Фото ОБЯЗАТЕЛЬНО для корсетов и бюстов
    if has_corsets or has_bust_items:
        kb = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text='Главное меню')]
            ]
        )
        await message.answer(
            '📸 Для пошива обязательно нужно фото в бюстье/купальнике, '
            'где хорошо просматривается зона груди (пример — третье фото в инструкции по снятию мерок).\n\n'
            'Пожалуйста, отправьте фото как обычное фото в чат.',
            reply_markup=kb
        )
        await state.set_state(Order.Photo)
    else:
        await state.update_data(photo_id=None)
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text='📱 Отправить номер', request_contact=True)],
                [KeyboardButton(text='Главное меню')]
            ],
            resize_keyboard=True
        )
        await message.answer('✅ *Теперь отправьте номер телефона для связи:*', reply_markup=kb)
        await state.set_state(Order.Phone)


@dp.message(Order.Photo, F.photo)
@retry_on_network_error()
async def save_photo(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await message.answer('✅ Фото сохранено!')
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='📱 Отправить номер', request_contact=True)], [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
    await message.answer('Теперь отправьте номер телефона для связи:', reply_markup=kb)
    await state.set_state(Order.Phone)

@dp.message(Order.Photo)
@retry_on_network_error()
async def invalid_photo(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='Главное меню')]
        ]
    )
    await message.answer(
        "⚠️ Для пошива это фото обязательно.\n\n"
        "Пожалуйста, отправьте *фото в бюстье/купальнике* "
        "как обычное фото в чат.\n\n"
        "Если хотите прервать оформление — нажмите «Главное меню».",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(Order.Phone, F.contact)
@retry_on_network_error()
async def phone_contact(message: Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await ask_delivery(message, state)

@dp.message(Order.Phone)
@retry_on_network_error()
async def phone_text(message: Message, state: FSMContext):
    if message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    phone = re.sub('\\D', '', message.text)
    if len(phone) < 10:
        await message.answer('Некорректный номер, попробуйте ещё раз:')
        return
    await state.update_data(phone='+' + phone)
    await ask_delivery(message, state)

@retry_on_network_error()
async def ask_delivery(message: Message, state: FSMContext):
    user_id = message.from_user.id
    cart = user_carts.get(user_id)

    # Есть ли вообще корзина
    if not cart:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text='Главное меню')]
            ],
            resize_keyboard=True
        )
        await message.answer("Ваша корзина пуста. Добавьте товары перед оформлением доставки.", reply_markup=kb)
        return

    # Логика сертификатов
    has_only_electronic_certificates = all(
        item.get('is_certificate') and item.get('certificate_type') == 'electronic'
        for item in cart
    )
    has_paper_certificates = any(
        item.get('is_certificate') and item.get('certificate_type') == 'paper'
        for item in cart
    )

    # ✅ Случай: в корзине только электронные сертификаты
    if has_only_electronic_certificates and not has_paper_certificates:
        await state.update_data(delivery='Электронная доставка')

        # Пытаемся взять email из сертификата
        email = None
        for item in cart:
            if item.get('certificate_type') == 'electronic' and item.get('Email'):
                email = item.get('Email')
                break

        address_info = f'Email: {email}' if email else 'Email: не указан'
        await state.update_data(address=address_info)

        await show_confirmation(message, state)
        return

    # ✅ Для всех остальных случаев (бумажные сертификаты, товары, смешанные корзины)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='🚚 СДЭК')],
            [KeyboardButton(text='📮 Почтой РФ')],
            [KeyboardButton(text='🏠 Самовывоз')],
            [KeyboardButton(text='Главное меню')]
        ],
        resize_keyboard=True
    )

    await message.answer('Выберите способ доставки:', reply_markup=kb)
    await state.set_state(Order.Delivery)


@dp.message(Order.Delivery, F.text == '🚚 СДЭК')
@retry_on_network_error()
async def handle_sdek(message: Message, state: FSMContext):
    await state.update_data(delivery='СДЭК')
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Назад к выбору доставки')], [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
    await message.answer('Введите ФИО и адрес ближайшего ПВЗ СДЭК:', reply_markup=kb)
    await state.set_state(Order.SdekAddress)

@dp.message(Order.Delivery, F.text == '📮 Почтой РФ')
@retry_on_network_error()
async def handle_post(message: Message, state: FSMContext):
    await state.update_data(delivery='Почтой РФ')
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Назад к выбору доставки')], [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
    await message.answer('Введите ФИО и адрес с индексом для доставки Почтой РФ:', reply_markup=kb)
    await state.set_state(Order.PostAddress)

@dp.message(Order.Delivery, F.text == '🏠 Самовывоз')
@retry_on_network_error()
async def handle_pickup(message: Message, state: FSMContext):
    await state.update_data(delivery='Самовывоз')
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Назад к выбору доставки')], [KeyboardButton(text='Подтвердить самовывоз')], [KeyboardButton(text='Главное меню')]], resize_keyboard=True)
    await message.answer(f'Самовывоз: {PICKUP_ADDRESS}\n🕒 Дата и время — по согласованию с менеджером.\n\nПожалуйста, подтвердите этот способ получения или выберите другой вариант доставки:', reply_markup=kb)
    await state.set_state(Order.ConfirmPickup)

@dp.message(Order.SdekAddress)
@retry_on_network_error()
async def save_sdek_address(message: Message, state: FSMContext):
    if message.text == 'Назад к выбору доставки':
        await back_to_delivery(message, state)
        return
    elif message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    await state.update_data(address=message.text)
    await show_confirmation(message, state)

@dp.message(Order.PostAddress)
@retry_on_network_error()
async def save_post_address(message: Message, state: FSMContext):
    if message.text == 'Назад к выбору доставки':
        await back_to_delivery(message, state)
        return
    elif message.text in ['Главное меню', 'В главное меню', '🏠 В главное меню']:
        await state.clear()
        await cmd_start(message, state)
        return
    await state.update_data(address=message.text)
    await show_confirmation(message, state)

@dp.message(Order.ConfirmPickup, F.text == 'Подтвердить самовывоз')
@retry_on_network_error()
async def confirm_pickup(message: Message, state: FSMContext):
    await state.update_data(address=PICKUP_ADDRESS)
    await show_confirmation(message, state)

@dp.message(Order.ConfirmPickup, F.text == 'Назад к выбору доставки')
@retry_on_network_error()
async def pickup_back_to_delivery(message: Message, state: FSMContext):
    await back_to_delivery(message, state)

@retry_on_network_error()
async def back_to_delivery(message: Message, state: FSMContext):
    data = await state.get_data()
    for field in ['delivery', 'address']:
        if field in data:
            del data[field]
    await state.set_data(data)
    await ask_delivery(message, state)



def build_admin_order_items_text(cart: list[dict]) -> str:
    """Подробный состав заказа для администратора/таблицы.

    ВАЖНО: "материал-строки" (Название начинается с "Материал:") НЕ выводим отдельной позицией.
    Материал подтягиваем внутрь товара по Материал_ID (или по последней встреченной материал-строке).
    """
    out: list[str] = []
    item_counter = 1

    def _is_material_placeholder(it: dict) -> bool:
        name = str(it.get('Название') or '').strip()
        return name.startswith('Материал:')

    def _looks_like_bust_item(it: dict) -> bool:
        # Чтобы не "размазывать" последний материал на все товары:
        # применять fallback last_material_name только к бюстам.
        hay = " ".join([
            str(it.get('Тип') or ''),
            str(it.get('Категория') or ''),
            str(it.get('Модель') or ''),
            str(it.get('Название') or ''),
        ]).lower()
        return ('бюст' in hay) or ('bust' in hay)

    # карта material_id -> material_name по техническим строкам
    material_map: dict[str, str] = {}
    last_material_name: str | None = None
    for it in cart or []:
        if not _is_material_placeholder(it):
            continue
        mid = it.get('Материал_ID')
        if mid is None:
            mid = it.get('ID')
        mid_s = str(mid) if mid is not None else None

        mat_name = str(it.get('Материал') or '').strip()
        if not mat_name:
            # пробуем вытащить из "Название: Материал: ..."
            title = str(it.get('Название') or '').strip()
            mat_name = title.replace('Материал:', '', 1).strip()

        if mat_name:
            last_material_name = mat_name
            if mid_s:
                material_map[mid_s] = mat_name

    for item in cart or []:
        # пропускаем технические строки материала
        if _is_material_placeholder(item):
            continue

        quantity = int(item.get('quantity', 1) or 1)
        name = str(item.get('Название') or item.get('Модель') or f"ID {item.get('ID')}").strip()

        # сертификаты
        if item.get('is_certificate'):
            cert_type = 'Электронный' if item.get('certificate_type') == 'electronic' else 'Бумажный'
            out.append(f"{item_counter}. 🎫 {escape_markdown(cert_type)} сертификат")
            out.append(f"   Название: {escape_markdown(name)}")
            out.append(f"   Номинал: {escape_markdown(str(item.get('Цена','')))}")
            out.append(f"   Кол-во: {quantity}")
            out.append(f"   ID: {escape_markdown(str(item.get('ID')))}")
            out.append("")
            item_counter += 1
            continue

        out.append(f"{item_counter}. {escape_markdown(name)}")

        if item.get('Категория'):
            out.append(f"   Категория: {escape_markdown(str(item.get('Категория')))}")
        if item.get('Тип'):
            out.append(f"   Тип: {escape_markdown(str(item.get('Тип')))}")
        if item.get('Модель'):
            out.append(f"   Модель: {escape_markdown(str(item.get('Модель')))}")

        # материал: берем из самого товара, иначе подтягиваем по Материал_ID / последнему материалу
        material_name = str(item.get('Материал') or '').strip()
        if not material_name:
            mid = item.get('Материал_ID')
            mid_s = str(mid) if mid is not None else None
            material_name = (material_map.get(mid_s) if mid_s else None) or ''
            # Раньше здесь был глобальный fallback на last_material_name,
            # из-за чего у товаров без материала (например, аксессуаров/сертификатов)
            # мог "подтягиваться" материал от предыдущего товара.
            if (not material_name) and _looks_like_bust_item(item):
                material_name = last_material_name or ''
        tname = str(item.get('Тип') or '').strip().lower()
        model_name = str(item.get('Модель') or '').strip()

        # Для "Другие аксессуары" часто нет материала/цвета,
        # поэтому выводим модель отдельной строкой, чтобы админ понимал что за товар.
        if ('другие аксессуары' in tname) and model_name:
            out.append(f"   Модель: {escape_markdown(model_name)}")

        if material_name:
            out.append(f"   Материал: {escape_markdown(material_name)}")

        if item.get('Цвет'):
            out.append(f"   Цвет: {escape_markdown(str(item.get('Цвет')))}")
        if item.get('Посадка'):
            out.append(f"   Посадка: {escape_markdown(str(item.get('Посадка')))}")
        if item.get('Размер'):
            out.append(f"   Размер: {escape_markdown(str(item.get('Размер')))}")

        # ID/артикулы
        if item.get('ID') is not None:
            out.append(f"   ID модели: {escape_markdown(str(item.get('ID')))}")
        if item.get('Материал_ID') is not None:
            out.append(f"   ID материала: {escape_markdown(str(item.get('Материал_ID')))}")

        # цена
        if item.get('Цена') is not None:
            try:
                price = float(item.get('Цена'))
                out.append(f"   Цена: {round(price)} ₽")
            except Exception:
                out.append(f"   Цена: {escape_markdown(str(item.get('Цена')))}")
        if quantity > 1:
            out.append(f"   Кол-во: {quantity}")

        out.append("")
        item_counter += 1

    return "\n".join(out).rstrip()
def build_sheet_order_items_text(cart: list[dict]) -> str:
    """Состав заказа для Google Sheets (в одну ячейку, без цен).
Формат:
1. Название
посадка - ...
модель - ...
материал - ...
цвет - ...
ID модели - ...
ID материала - ...
"""
    lines: list[str] = []
    n = 1

    for item in cart or []:
        title = str(item.get('Название') or '').strip()
        model_val = str(item.get('Модель') or '').strip()
        is_material_marker = title.lower().startswith('материал:') or (model_val.lower() in ('не указана', '') and 'материал' in title.lower())
        is_certificate = bool(item.get('is_certificate'))

        # материалы-"маркер" в Sheets не пишем как отдельный товар (они нужны только для привязки ID материала)
        if is_material_marker and not is_certificate:
            continue

        name = str(item.get('Название') or item.get('Модель') or f"ID {item.get('ID')}").strip()
        fit = (item.get('Посадка') or item.get('Вариант посадки') or '').strip()
        model_name = (item.get('Модель') or '').strip()
        material_name = (item.get('Материал') or '').strip()
        color = (item.get('Цвет') or '').strip()

        model_id = item.get('ID')
        material_id = item.get('Материал_ID')

        lines.append(f"{n}. {name}")
        if fit:
            lines.append(f"посадка - {fit}")
        if model_name:
            lines.append(f"модель - {model_name}")
        if material_name:
            lines.append(f"материал - {material_name}")
        if color:
            lines.append(f"цвет - {color}")
        if model_id is not None:
            lines.append(f"ID модели - {model_id}")
        if material_id is not None:
            lines.append(f"ID материала - {material_id}")
        lines.append("")
        n += 1

    return "\n".join(lines).rstrip()








def build_user_order_items_minimal(cart: list[dict]) -> str:
    """Минимальный текст состава заказа для клиента (корзина/превью перед подтверждением).

    Требования:
    - НЕ показываем технические строки "Материал: ..." как отдельный товар.
    - Убираем дубль вида "Материал: Материал бюста: ..." -> "Материал: Кружевной" (или "Материал: Кружевной бюст" и т.п.).
    - Не подтягиваем "последний материал" к другим товарам/сертификатам: материал берём только из самого товара
      или по Материал_ID.
    - Показываем цену у каждого товара (и сумму по позиции при qty>1). Для трусиков корректно разделяем промо/обычную часть.
    """
    promo = get_promo_settings()
    promo_active = bool(promo.get('PANTIES_PROMO_ACTIVE', True))

    def _is_material_placeholder(it: dict) -> bool:
        name = str(it.get('Название') or '').strip()
        return name.lower().startswith('материал:')

    def _short_material_name(raw: str) -> str:
        """Приводит к человеку-понятному виду:
        - 'Материал бюста: Кружевной' -> 'Кружевной'
        - 'Материал: Материал бюста: Кружевной' -> 'Кружевной'
        - 'Материал трусиков: ...' -> '...'
        """
        s = (raw or '').strip()
        if not s:
            return s
        # снимаем внешний префикс "Материал:"
        if s.lower().startswith('материал:'):
            s = s.split(':', 1)[-1].strip()
        # снимаем специфические префиксы
        for p in ('материал бюста:', 'материал пояса:', 'материал трусиков:', 'материал комплекта:'):
            if s.lower().startswith(p):
                s = s[len(p):].strip()
                break
        return s

    def _human_price(v: float) -> str:
        try:
            vv = float(v)
        except Exception:
            vv = 0.0
        iv = int(vv)
        return str(iv) if abs(vv - iv) < 1e-9 else str(vv)

    # 1) карта ID материала -> название материала (из технических строк в корзине)
    material_by_id: dict[int, str] = {}
    for it in cart or []:
        if not _is_material_placeholder(it):
            continue
        mid = it.get('Материал_ID') or it.get('ID')
        try:
            mid_int = int(float(mid))
        except Exception:
            mid_int = None

        mname = str(it.get('Материал') or '').strip()
        if not mname:
            raw = str(it.get('Название') or '').strip()
            mname = raw.split(':', 1)[-1].strip() if ':' in raw else raw
        mname = _short_material_name(mname)

        if mid_int is not None and mname:
            material_by_id[mid_int] = mname

    # 2) собираем позиции (с учётом промо-части у трусиков)
    expanded_items: list[dict] = []
    for it in cart or []:
        if _is_material_placeholder(it):
            continue

        qty = int(it.get('quantity', 1) or 1)
        is_panties = bool(it.get('is_panties'))

        promo_applied = 0
        try:
            promo_applied = int(it.get('promo_applied') or 0)
        except Exception:
            promo_applied = 0

        if is_panties and promo_active and promo_applied > 0 and qty > promo_applied:
            it_promo = dict(it)
            it_promo['quantity'] = promo_applied
            it_promo['_promo_part'] = True
            expanded_items.append(it_promo)

            it_reg = dict(it)
            it_reg['quantity'] = qty - promo_applied
            it_reg['_promo_part'] = False
            expanded_items.append(it_reg)
        else:
            it_one = dict(it)
            if is_panties and promo_active and promo_applied > 0:
                it_one['_promo_part'] = True
            expanded_items.append(it_one)

    # 3) группируем одинаковые позиции (name+material+color+price_tag)
    grouped: dict[tuple[str, str, str, str, int, str, int], int] = {}
    for it in expanded_items:
        name = str(it.get('Название') or it.get('Модель') or f"ID {it.get('ID')}").strip()
        color = str(it.get('Цвет') or '').strip() or '—'

        material = str(it.get('Материал') or '').strip()
        # UI-улучшение для "Другие аксессуары":
        # у них часто нет материала/цвета, поэтому показываем МОДЕЛЬ отдельной строкой
        _t = str(it.get('Тип') or '').strip().lower()
        name_l = name.lower()
        is_accessory_like = ('другие аксессуары' in _t) or ('пояс для чулок' in _t) or bool(it.get('is_stock_belt'))
        is_bust = ('бюст' in _t) or ('бюст' in name_l) or ('bust' in name_l)
        if not material:
            mid = it.get('Материал_ID')
            try:
                mid_int = int(float(mid)) if mid is not None else None
            except Exception:
                mid_int = None
            if mid_int is not None and material_by_id.get(mid_int):
                material = material_by_id[mid_int]
        material = _short_material_name(material) if material else '—'

        qty = int(it.get('quantity', 1) or 1)

        is_panties = bool(it.get('is_panties'))
        is_promo_part = bool(it.get('_promo_part')) and is_panties

        if is_panties and is_promo_part and it.get('promo_unit_price') is not None:
            unit_price = safe_convert_price(it.get('promo_unit_price'))
        elif is_panties:
            unit_price = safe_convert_price(it.get('original_price') if it.get('original_price') is not None else it.get('Цена', 0))
        else:
            unit_price = safe_convert_price(it.get('Цена', 0))

        unit_price_int = int(round(unit_price)) if unit_price is not None else 0

        price_tag = 'акция' if is_promo_part else 'обычная'
        display_mode = 'acc' if is_accessory_like else ('both' if (is_panties or is_bust) else 'mat')
        model_line = (str(it.get('Модель') or '').strip() or '—') if display_mode in ('acc','both') else ''
        key = (name, material, color, price_tag, unit_price_int, model_line, display_mode)
        grouped[key] = grouped.get(key, 0) + qty

    # 4) рендер
    lines: list[str] = []
    i = 1
    for (name, material, color, price_tag, unit_price_int, model_line, display_mode), qty in grouped.items():
        lines.append(f"{i}. {escape_markdown(name)}")
        if display_mode in ('acc','both'):
            _m = model_line or '—'
            lines.append(f"   Модель: {escape_markdown(_m) if _m != '—' else '—'}")
            if display_mode == 'both':
                lines.append(f"   Материал: {escape_markdown(material) if material != '—' else '—'}")
        else:
            lines.append(f"   Материал: {escape_markdown(material) if material != '—' else '—'}")

        if price_tag == 'акция':
            lines.append(f"   Цена: {_human_price(unit_price_int)} ₽ (акция)")
        else:
            lines.append(f"   Цена: {_human_price(unit_price_int)} ₽")

        if qty > 1:
            lines.append(f"   Кол-во: {qty}")
            lines.append(f"   Сумма: {_human_price(unit_price_int * qty)} ₽")

        lines.append("")
        i += 1

    return "\n".join(lines).rstrip()



@retry_on_network_error()
async def show_confirmation(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id
    cart = user_carts.get(user_id)

    # --- ОТПРАВКА ФОТО ИЗ ЗАКАЗА (как в корзине) ---
    try:
        # Загружаем все данные из таблицы
        all_rows = load_data_from_master_cached(cache_key='all_products_all_rows')
        if not all_rows:
            all_rows = _load_data_from_master_impl()

        print(f"🔍 Загружено строк из таблицы: {len(all_rows)}")

        # Собираем ВСЕ ID из заказа
        cart_model_ids = set()
        cart_material_ids = set()

        for item in cart:
            is_material_item = (
                    str(item.get('Название') or '').strip().startswith('Материал:') or
                    (
                        item.get('Материал') and
                        str(item.get('Модель') or '').strip() in ('', 'Не указана') and
                        any(mat in str(item.get('Материал', '')).lower() for mat in [
                            'материал бюста:', 'материал пояса:'
                        ])
                    )
            )

            if is_material_item:
                if item.get('ID'):
                    try:
                        material_id = int(float(item['ID']))
                        cart_material_ids.add(material_id)
                        print(f"🔍 Материал добавлен в material_ids: {material_id} - {item.get('Материал')}")
                    except (ValueError, TypeError):
                        pass
            else:
                if item.get('ID'):
                    try:
                        model_id = int(float(item['ID']))
                        cart_model_ids.add(model_id)
                        print(f"🔍 Модель добавлена в model_ids: {model_id} - {item.get('Название')}")
                    except (ValueError, TypeError):
                        pass

            if item.get('Материал_ID'):
                try:
                    material_id_from_field = int(float(item['Материал_ID']))
                    cart_material_ids.add(material_id_from_field)
                    print(f"🔍 Material_ID добавлен в material_ids: {material_id_from_field}")
                except (ValueError, TypeError):
                    pass

        print(f"🔍 ID моделей в заказе: {cart_model_ids}")
        print(f"🔍 ID материалов в заказе: {cart_material_ids}")

        # Ищем фото для каждого ID в заказе
        images_ordered = []
        seen_images = set()

        for row in all_rows:
            # Проверяем основной ID (для моделей)
            row_id = None
            try:
                if row.get('ID'):
                    row_id = int(float(row['ID']))
            except (ValueError, TypeError):
                continue

            # Если этот ID есть в заказе моделей - ищем фото модели
            if row_id and row_id in cart_model_ids:
                model_image = row.get('Изображение модели') or row.get('Изображение')
                if model_image:
                    if isinstance(model_image, str) and model_image.strip():
                        if model_image.startswith(('http://', 'https://')):
                            image_url = model_image
                        elif re.match('^[a-zA-Z0-9_-]{20,200}$', model_image.strip()):
                            image_url = f'https://drive.google.com/uc?export=view&id={model_image.strip()}'
                        else:
                            image_url = None

                        if image_url and image_url not in seen_images:
                            images_ordered.append(image_url)
                            seen_images.add(image_url)
                            print(f"✅ Найдено фото МОДЕЛИ для ID {row_id}: {image_url}")

            # Проверяем ID 2 (для материалов)
            row_id2 = None
            try:
                if row.get('ID 2'):
                    row_id2 = int(float(row['ID 2']))
            except (ValueError, TypeError):
                continue

            # Если этот ID 2 есть в заказе материалов - ищем фото материала
            if row_id2 and row_id2 in cart_material_ids:
                material_image = row.get('Изображение материала') or row.get('Изображение')
                if material_image:
                    if isinstance(material_image, str) and material_image.strip():
                        if material_image.startswith(('http://', 'https://')):
                            image_url = material_image
                        elif re.match('^[a-zA-Z0-9_-]{20,200}$', material_image.strip()):
                            image_url = f'https://drive.google.com/uc?export=view&id={material_image.strip()}'
                        else:
                            image_url = None

                        if image_url and image_url not in seen_images:
                            images_ordered.append(image_url)
                            seen_images.add(image_url)
                            print(f"✅ Найдено фото МАТЕРИАЛА для ID 2 {row_id2}: {image_url}")
                else:
                    print(f"⚠️ Для материала ID 2 {row_id2} не найдено изображение в строке ID {row_id}")

        # ДОПОЛНИТЕЛЬНЫЙ ПОИСК ДЛЯ МАТЕРИАЛОВ
        if cart_material_ids and len(images_ordered) == len(cart_model_ids):
            print(f"🔍 ДОПОЛНИТЕЛЬНЫЙ ПОИСК ДЛЯ МАТЕРИАЛОВ: {cart_material_ids}")
            for material_id in cart_material_ids:
                print(f"🔍 Ищем материал с ID 2 = {material_id}")
                for row in all_rows:
                    row_id2 = None
                    try:
                        if row.get('ID 2'):
                            row_id2 = int(float(row['ID 2']))
                    except (ValueError, TypeError):
                        continue

                    if row_id2 == material_id:
                        material_image = row.get('Изображение материала') or row.get('Изображение')
                        material_name = row.get('Материал', 'Неизвестно')
                        print(
                            f"🔍 Найдена строка для материала {material_id}: ID={row.get('ID')}, Материал='{material_name}', Изображение='{material_image}'"
                        )

                        if material_image:
                            if isinstance(material_image, str) and material_image.strip():
                                if material_image.startswith(('http://', 'https://')):
                                    image_url = material_image
                                elif re.match('^[a-zA-Z0-9_-]{20,200}$', material_image.strip()):
                                    image_url = f'https://drive.google.com/uc?export=view&id={material_image.strip()}'
                                else:
                                    image_url = None

                                if image_url and image_url not in seen_images:
                                    images_ordered.append(image_url)
                                    seen_images.add(image_url)
                                    print(
                                        f"✅ ДОПОЛНИТЕЛЬНО: Найдено фото МАТЕРИАЛА для ID 2 {material_id}: {image_url}"
                                    )
                        break

        print(f"🔍 Всего найдено изображений: {len(images_ordered)}")

        # Сохраняем точный список картинок, чтобы отправить админу ТО ЖЕ САМОЕ
        try:
            await state.update_data(order_images=images_ordered)
        except Exception as _e:
            print(f"⚠️ Не удалось сохранить order_images в state: {_e}")

        # Отправляем изображения
        if images_ordered:
            await message.answer("📸 *Фото товаров из вашего заказа:*", parse_mode=ParseMode.MARKDOWN)

            for i in range(0, len(images_ordered), 10):
                batch = images_ordered[i:i + 10]
                media_group = []

                for j, image_url in enumerate(batch):
                    # Превращаем внешний URL (Drive) в Telegram file_id через канал-кэш
                    try:
                        media_id = await ensure_photo_in_channel(image_url)
                    except Exception:
                        media_id = image_url  # fallback

                    # Подпись только на самой первой фотке первого батча
                    if i == 0 and j == 0:
                        media_group.append(InputMediaPhoto(media=media_id, caption="Ваши товары"))
                    else:
                        media_group.append(InputMediaPhoto(media=media_id))

                try:
                    with timer("tg.send_media_group.user", "-"):
                        await bot.send_media_group(chat_id=user_id, media=media_group)
                    print(f'✅ Успешно отправлено {len(media_group)} фото пользователю {user_id}')
                except Exception as e:
                    print(f'❌ Ошибка отправки media_group: {e}')

        else:
            print("⚠️ Не найдено изображений для товаров в заказе")

    except Exception as e:
        print(f'❌ Ошибка формирования фотоальбома заказа: {e}')
        import traceback
        traceback.print_exc()

    # --- ТЕКСТ ПОДТВЕРЖДЕНИЯ ЗАКАЗА С ОБЪЕДИНЕНИЕМ ---
    order_text = ' *ВАШ ЗАКАЗ ГОТОВ К ОФОРМЛЕНИЮ* \n\n'
    order_text += '*СОСТАВ ЗАКАЗА:*\n'
    total_order_amount = calculate_cart_total(user_id)
    original_total = calculate_original_total(user_id)
    applied_certificate = user_carts.get_applied_certificate(user_id)
    promo_settings = get_promo_settings()
    promo_price = promo_settings.get('PANTIES_PROMO_PRICE', 6500)
    promo_count = promo_settings.get('PANTIES_PROMO_COUNT', 3)

    if applied_certificate and applied_certificate.get('valid'):
        order_text += f"🎫 *Применен сертификат:* {applied_certificate['amount']} руб.\n\n"

    # СОЗДАЕМ СПИСОК ДЛЯ ОТОБРАЖЕНИЯ - ОБЪЕДИНЯЕМ МАТЕРИАЛЫ И МОДЕЛИ (как в корзине)
    display_items = []
    bust_materials = []
    bust_models = []
    stock_belts_materials = []
    stock_belts_models = []

    # Сначала собираем все материалы и модели
    for item in cart:
        # Материалы бюста
        is_bust_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any(mat in str(item.get('Материал', '')).lower() for mat in [
                'материал бюста: хлопковый',
                'материал бюста: кружевной',
                'материал бюста: эластичная сетка',
                'материал бюста: вышивка'
            ]) and
            any(
                term in str(item.get('Тип', '')).lower() or
                term in str(item.get('Категория', '')).lower() or
                term in str(item.get('Название', '')).lower()
                for term in ['бюст', 'материал:']
            )
        )

        # Модели бюста
        is_bust_model = any([
            'бюст' in str(item.get('Тип', '')).lower(),
            'бюст' in str(item.get('Категория', '')).lower(),
            'бюст' in str(item.get('Модель', '')).lower(),
            'бюст' in str(item.get('Название', '')).lower()
        ]) and item.get('Модель')

        # Материалы поясов
        is_stock_belts_material = (
            item.get('Материал') and
            (not item.get('Модель')) and
            any(mat in str(item.get('Материал', '')).lower() for mat in [
                'материал пояса: кружевной',
                'материал пояса: эластичная сетка'
            ]) and
            item.get('Тип') == 'Аксессуары'
        )

        # Модели поясов
        is_stock_belts_model = (
            'пояс' in str(item.get('Модель', '')).lower() and
            'чулок' in str(item.get('Модель', '')).lower() and
            item.get('Тип') == 'Аксессуары'
        )

        if is_bust_material:
            bust_materials.append(item)
        elif is_bust_model:
            bust_models.append(item)
        elif is_stock_belts_material:
            stock_belts_materials.append(item)
        elif is_stock_belts_model:
            stock_belts_models.append(item)
        else:
            # Все остальные товары
            display_items.append(item)

    # ОБЪЕДИНЯЕМ МАТЕРИАЛЫ И МОДЕЛИ БЮСТА
    for model in bust_models:
        model_name = model.get('Модель', '').lower()
        matched_material = None

        # Ищем соответствующий материал бюста
        for material in bust_materials:
            material_id = material.get('ID')
            material_name = material.get('Материал', '').lower()

            # 🔹 Универсальная проверка для вышивки
            is_embroidery_match = (
                'вышивк' in model_name and
                'вышивк' in material_name
            )

            # 🔹 Для эластичной сетки
            is_elastic_match = (
                ('эластичной сетк' in model_name and 'эластичной сетк' in material_name) or
                ('эластичной сетк' in model_name and 'эластичная сетка' in material_name)
            )

            # 🔹 Для хлопкового и кружевного бюста
            is_cotton_match = 'хлопков' in model_name and 'хлопков' in material_name
            is_lace_match = 'кружевн' in model_name and 'кружевн' in material_name

            if is_cotton_match or is_lace_match or is_elastic_match or is_embroidery_match:
                # Материал может использоваться для нескольких моделей (например, два бюста одного материала),
                # поэтому НЕ блокируем повторное использование по material_id.
                matched_material = material
                break

        # Создаем объединенный элемент бюста
        if matched_material:
            combined_item = {
                'Название': model.get('Название', ''),
                'Цена': model.get('Цена', 0),
                'quantity': model.get('quantity', 1),
                'Модель': model.get('Модель', ''),
                'Материал': matched_material.get('Материал', ''),
                'Материал_ID': matched_material.get('ID'),
                'ID': model.get('ID'),
                'Цвет': (model.get('Цвет') or matched_material.get('Цвет') or ''),
                'is_combined_bust': True
            }
            display_items.append(combined_item)
        else:
            # Если материал не найден, показываем модель с предупреждением
            model['missing_material'] = True
            display_items.append(model)

    # ОБЪЕДИНЯЕМ МАТЕРИАЛЫ И МОДЕЛИ ПОЯСОВ
    used_belt_materials = set()
    for model in stock_belts_models:
        model_name = model.get('Модель', '').lower()
        matched_material = None

        # Ищем соответствующий материал пояса
        for material in stock_belts_materials:
            material_id = material.get('ID')
            material_name = material.get('Материал', '').lower()

            # Проверяем соответствие материала и модели пояса
            if (('кружевной' in model_name and 'кружевной' in material_name) or
                    ('эластичной сетк' in model_name and 'эластичной сетк' in material_name) or
                    ('эластичной сетк' in model_name and 'эластичная сетка' in material_name)):

                if material_id not in used_belt_materials:
                    matched_material = material
                    used_belt_materials.add(material_id)
                    break

        # Создаем объединенный элемент пояса
        if matched_material:
            combined_item = {
                'Название': model.get('Название', ''),
                'Цена': model.get('Цена', 0),
                'quantity': model.get('quantity', 1),
                'Модель': model.get('Модель', ''),
                'Материал': matched_material.get('Материал', ''),
                'Материал_ID': matched_material.get('ID'),
                'ID': model.get('ID'),
                'is_combined_belt': True
            }
            display_items.append(combined_item)
        else:
            # Если материал не найден, показываем модель с предупреждением
            model['missing_material'] = True
            display_items.append(model)

    # ОТОБРАЖАЕМ ТОВАРЫ
    item_counter = 1
    for item in display_items:
        price = safe_convert_price(item.get('Цена', 0))
        quantity = item.get('quantity', 1)

        if item.get('is_certificate'):
            display_price = int(float(price)) if price else 0
            order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"
            order_text += f'   Цена: {display_price} ₽\n'
            if item.get('certificate_type') == 'electronic' and item.get('Email'):
                order_text += f"   Email: {item.get('Email')}\n"
            if item.get('certificate_type') == 'paper':
                order_text += '\n'
            order_text += '\n'
            item_counter += 1
            continue

        # ОБЪЕДИНЕННЫЕ БЮСТЫ
        if item.get('is_combined_bust'):
            item_total = price * quantity
            display_price = round(price)
            display_total = round(item_total)

            order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"
            order_text += f'   Цена: {display_price} ₽ x {quantity} = {display_total} ₽\n'
            order_text += f"   Модель: {escape_markdown(item.get('Модель', 'Не указана'))}\n"
            order_text += f"   {escape_markdown(item.get('Материал', ''))}\n"
            if item.get('Цвет'):
                order_text += f"   Цвет: {escape_markdown(item.get('Цвет'))}\n"
            order_text += '\n'
            item_counter += 1
            continue

        # ОБЪЕДИНЕННЫЕ ПОЯСА
        if item.get('is_combined_belt'):
            item_total = price * quantity
            display_price = round(price)
            display_total = round(item_total)

            order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"
            order_text += f'   Цена: {display_price} ₽ x {quantity} = {display_total} ₽\n'
            order_text += f"   Модель: {escape_markdown(item.get('Модель', 'Не указана'))}\n"
            order_text += f"   Материал: {escape_markdown(item.get('Материал', ''))}\n"
            if item.get('Цвет'):
                order_text += f"   Цвет: {escape_markdown(item.get('Цвет'))}\n"
            order_text += '\n'
            item_counter += 1
            continue

        # МОДЕЛИ БЕЗ МАТЕРИАЛА
        if item.get('missing_material'):
            item_total = price * quantity
            display_price = round(price)
            display_total = round(item_total)

            order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"
            order_text += f'   Цена: {display_price} ₽ x {quantity} = {display_total} ₽\n'
            order_text += f"   Модель: {escape_markdown(item.get('Модель', 'Не указана'))}\n"
            order_text += f'   ⚠️ Материал: не выбран\n'
            if item.get('Цвет'):
                order_text += f"   Цвет: {escape_markdown(item.get('Цвет'))}\n"
            order_text += '\n'
            item_counter += 1
            continue

        # ТРУСИКИ С АКЦИЕЙ
        if item.get('is_panties') and 'promo_applied' in item:
            promo_quantity = item['promo_applied']
            regular_quantity = quantity - promo_quantity

            order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"

            if promo_quantity > 0:
                promo_price_per_item = promo_price // promo_count
                promo_total = promo_price_per_item * promo_quantity
                order_text += f'   🖤 Акционная цена: {promo_price_per_item} ₽ x {promo_quantity} = {promo_total} ₽\n'

            if regular_quantity > 0:
                regular_price = item['original_price']
                regular_total = regular_price * regular_quantity
                order_text += f'   💰 Обычная цена: {regular_price} ₽ x {regular_quantity} = {regular_total} ₽\n'

            if item.get('Посадка'):
                order_text += f"   Посадка: {escape_markdown(item.get('Посадка'))}\n"
            if item.get('Материал'):
                order_text += f"   Материал: {escape_markdown(item.get('Материал'))}\n"
            if item.get('Цвет'):
                order_text += f"   Цвет: {escape_markdown(item.get('Цвет'))}\n"
            order_text += '\n'
            item_counter += 1
            continue

        # ВСЕ ОСТАЛЬНЫЕ ТОВАРЫ
        item_total = price * quantity
        display_price = round(price)
        display_total = round(item_total)

        order_text += f"{item_counter}. *{escape_markdown(item.get('Название', ''))}*\n"
        order_text += f'   Цена: {display_price} ₽ x {quantity} = {display_total} ₽\n'

        if item.get('Посадка'):
            order_text += f"   Посадка: {escape_markdown(item.get('Посадка'))}\n"
        if item.get('Модель'):
            order_text += f"   Модель: {escape_markdown(item.get('Модель'))}\n"
        elif item.get('Материал'):
            order_text += f"   Материал: {escape_markdown(item.get('Материал'))}\n"
        if item.get('Цвет'):
            order_text += f"   Цвет: {escape_markdown(item.get('Цвет'))}\n"
        order_text += '\n'
        item_counter += 1

    # ИТОГОВАЯ СУММА
    if applied_certificate and applied_certificate.get('valid'):
        order_text += f'*Исходная сумма:* {original_total} ₽\n'
        order_text += f"*Скидка по сертификату:* -{applied_certificate['amount']} ₽\n"

    order_text += f'*Общая сумма к оплате:* {total_order_amount} ₽\n\n'

    # ДАННЫЕ ДОСТАВКИ И КОНТАКТОВ
    order_text += '*ДАННЫЕ ДЛЯ ДОСТАВКИ:*\n'
    order_text += f"📞 *Телефон:* {escape_markdown(data.get('phone', 'Не указан'))}\n"
    order_text += f"🚚 *Способ доставки:* {escape_markdown(data.get('delivery', 'Не указан'))}\n"

    if data.get('delivery') != 'Электронная доставка':
        address_raw = data.get('address', 'Не указан')
        address_text = escape_markdown(address_raw).replace('\\-', '-')  # убираем экранирование дефиса
        order_text += f"📦 *Адрес:* {address_text}\n"

    # ДАННЫЕ МЕРОК
    order_text += '\n*ВАШИ МЕРКИ:*\n'
    measurements = [
        ('Горизонтальная дуга', 'horizontal_arc'),
        ('Обхват груди', 'bust'),
        ('Обхват под грудью', 'underbust'),
        ('Обхват талии', 'waist'),
        ('Обхват бедер', 'hips')
    ]

    has_measurements = False
    for name, key in measurements:
        if data.get(key):
            order_text += f"📏 *{name}:* {data.get(key)} см\n"
            has_measurements = True

    if not has_measurements:
        order_text += "📏 Мерки не требуются для данного заказа\n"

    # ПОЖЕЛАНИЯ К ЗАКАЗУ
    order_text += f"\n*ПОЖЕЛАНИЯ К ЗАКАЗУ:*\n{escape_markdown(data.get('order_notes', 'Не указаны'))}\n"

    # КНОПКИ ПОДТВЕРЖДЕНИЯ
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='✅ Подтвердить заказ')],
            [KeyboardButton(text='❌ Отменить заказ')]
        ]
    )

    try:
        await message.answer(order_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f'Markdown error in confirmation: {e}')
        plain_text = re.sub('\\*([^*]+)\\*', '\\1', order_text)
        plain_text = re.sub('🎫|💰|🖤|⚠️|📞|🚚|📦|📏|📝|✅|❌', '', plain_text)
        await message.answer(plain_text, reply_markup=kb)

    await state.set_state(Order.Confirmation)


@dp.message(Order.Confirmation, F.text == '❌ Отменить заказ')
@retry_on_network_error()
async def cancel_order_from_confirmation(message: Message, state: FSMContext):
    # Ничего не сохраняем и не отправляем админу, просто возвращаем пользователя в корзину
    await message.answer(
        "❌ Заказ *не* отправлен администратору.\n"
        "Вы можете очистить корзину или добавить что-то еще.",
        parse_mode=ParseMode.MARKDOWN
    )
    await show_cart(message, state)


@dp.message(Order.Confirmation, F.text == '✅ Подтвердить заказ')
@retry_on_network_error()
async def confirm_order(message: Message, state: FSMContext):
    _lock = get_action_lock(message.from_user.id, "confirm_order")
    if _lock.locked():
        try:
            await message.answer('⏳ Уже подтверждаю заказ, секунду...')
        except Exception:
            pass
        return
    await _lock.acquire()
    try:
        data = await state.get_data()
        user_id = message.from_user.id
        cart = user_carts.get(user_id)
        data['user_name'] = f'{message.from_user.full_name} (@{message.from_user.username})' if message.from_user.username else message.from_user.full_name
        data['cart'] = [dict(item) for item in (cart or [])]  # snapshot
        data['total_amount'] = calculate_cart_total(user_id)
        applied_certificate = user_carts.get_applied_certificate(user_id)
        saving_msg = await message.answer('💾 Сохраняем заказ...')
        with timer("order_manager.save_order_to_sheet", "-"):
            success, order_number = await asyncio.to_thread(order_manager.save_order_to_sheet, data)
        if applied_certificate and applied_certificate.get('valid'):
            certificate_applied = certificate_manager.apply_certificate(applied_certificate['number'], user_id, order_number)
            if certificate_applied:
                print(f"✅ Сертификат {applied_certificate['number']} применен к заказу {order_number}")
            else:
                print(f"❌ Ошибка применения сертификата {applied_certificate['number']}")
        # отправку админу делаем в фоне, чтобы не задерживать ответ пользователю
        asyncio.create_task(send_order_to_admin(data, success, order_number, applied_certificate))
        await saving_msg.delete()
        user_carts.clear(user_id)
        if success:
            order_confirmation_text = f"✅ *Ваш заказ №{order_number} принят!*\n\n📦 *Номер заказа:* {order_number}\n💰 *Сумма заказа:* {data['total_amount']} ₽\n\n"
            if applied_certificate and applied_certificate.get('valid'):
                order_confirmation_text += f"🎫 *Применен сертификат:* {applied_certificate['amount']} руб.\n\n"
            order_confirmation_text += '\nСпасибо за доверие к бренду SIA LÌ 🤍 \nМенеджер уже пишет вам 👩🏼‍💻'
            await message.answer(order_confirmation_text, reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Главное меню')]], resize_keyboard=True))
        else:
            await message.answer('✅ *Ваш заказ принят!*\n\n⚠️ *Внимание:* Возникла ошибка при сохранении в базу данных.\nС вами свяжется наш менеджер для уточнения деталей.\n\nСпасибо за заказ! ❤️', reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Главное меню')]], resize_keyboard=True))
        user_stats.save_stats_to_sheet(order_manager)
        if user_stats.should_send_notification():
            await send_stats_to_admin()
            user_stats.mark_notification_sent()
        await state.clear()

    finally:
        if _lock.locked():
            _lock.release()
@retry_on_network_error()
def load_stats_settings():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        try:
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet('Настройки')
            settings_data = worksheet.get_all_records()
            for row in settings_data:
                if row.get('Параметр') == 'STATS_NOTIFICATION_INTERVAL_DAYS':
                    try:
                        value = row.get('Значение', 1)
                        return int(value) if value else 1
                    except (ValueError, TypeError):
                        return 1
        except gspread.WorksheetNotFound:
            return 1
    except Exception as e:
        print(f'Ошибка загрузки настроек статистики: {e}')
        return 1
    return 1

@retry_on_network_error()
def save_stats_settings(interval_days: int):
    try:
        if interval_days is None:
            interval_days = 1
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        try:
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet('Настройки')
        except gspread.WorksheetNotFound:
            worksheet = client.open_by_key(SPREADSHEET_ID).add_worksheet(title='Настройки', rows=100, cols=2)
            worksheet.append_row(['Параметр', 'Значение'])
        settings_data = worksheet.get_all_records()
        found = False
        for i, row in enumerate(settings_data):
            if row.get('Параметр') == 'STATS_NOTIFICATION_INTERVAL_DAYS':
                worksheet.update_cell(i + 2, 2, interval_days)
                found = True
                break
        if not found:
            worksheet.append_row(['STATS_NOTIFICATION_INTERVAL_DAYS', interval_days])
        global STATS_NOTIFICATION_INTERVAL_DAYS
        STATS_NOTIFICATION_INTERVAL_DAYS = interval_days
        data_cache.clear('promotion_settings')
        return True
    except Exception as e:
        print(f'Ошибка сохранения настроек статистики: {e}')
        return False

@dp.message(Command('admin'))
@retry_on_network_error()
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer('❌ У вас нет доступа к админ-панели.')
        return
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text='📊 Статистика бота')],
            [KeyboardButton(text='📢 Сделать рассылку')],
            [KeyboardButton(text='⚙️ Настройки уведомлений')],
            [KeyboardButton(text='🎁 Обновить промо')],
            [KeyboardButton(text='🔄 Обновить каталог')],
            [KeyboardButton(text='🔙 Главное меню')],
        ],
    )
    await message.answer('🛠️ *Админ-панель*', reply_markup=kb)
    await state.set_state(AdminPanel.MainMenu)

@dp.message(AdminPanel.MainMenu, F.text == '📊 Статистика бота')
@retry_on_network_error()
async def show_bot_stats(message: Message):
    stats = user_stats.get_stats()
    stats_text = f"📊 *Статистика бота*\n\n👥 *Всего пользователей:* {stats['total_users']}\n🆕 *Новых за сегодня:* {stats['new_users_today']}\n📈 *Новых за неделю:* {stats['new_users_week']}\n🎯 *Активных за сегодня:* {stats['active_users_today']}\n🔥 *Активных за неделю:* {stats['active_users_week']}\n\n⏰ *Текущий интервал уведомлений:* {STATS_NOTIFICATION_INTERVAL_DAYS} день(дней)\n📅 *Последнее уведомление:* {user_stats._last_notification_sent and datetime.fromtimestamp(user_stats._last_notification_sent).strftime('%Y-%m-%d %H:%M:%S') or 'Никогда'}"
    await message.answer(stats_text)

@dp.message(AdminPanel.MainMenu, F.text == '🎁 Обновить промо')
@retry_on_network_error()
async def refresh_promo_from_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer('❌ У вас нет доступа к админ-панели.')
        return
    await message.answer('⏳ Обновляю настройки промо из Google Sheets...')
    try:
        settings = await asyncio.to_thread(refresh_promo_settings_from_sheets)
        active = settings.get('PANTIES_PROMO_ACTIVE', True)
        price = settings.get('PANTIES_PROMO_PRICE', 6500)
        cnt = settings.get('PANTIES_PROMO_COUNT', 3)
        text_ = settings.get('PANTIES_PROMO_TEXT', '')
        await message.answer(
            f"✅ Промо обновлено.\n\n"
            f"Активно: {active}\n"
            f"Условие: {cnt} за {price} руб\n"
            + (f"Текст: {text_}" if text_ else "")
        )
    except Exception as e:
        await message.answer(f'❌ Не удалось обновить промо: {e}')

@dp.message(AdminPanel.MainMenu, F.text == '🔄 Обновить каталог')
@retry_on_network_error()
async def refresh_catalog_from_admin(message: Message, state: FSMContext):
    # На всякий случай ещё раз проверим, что это админ
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer('❌ У вас нет доступа к этой функции.')
        return

    await message.answer('⏳ Обновляю каталог из Google Sheets, подождите...')

    try:
        # Запускаем тот же скрипт, который ты уже запускал руками:
        # python migrate_from_sheets_to_sqlite.py
        result = subprocess.run(
            [sys.executable, "migrate_from_sheets_to_sqlite.py"],
            capture_output=True,
            text=True,
            encoding="utf-8",  # 🔥 Исправляет ошибку 100%
            errors="ignore",
            timeout=300  # до 5 минут на всякий пожарный
        )

        if result.returncode == 0:
            await message.answer('✅ Каталог успешно обновлён из Google Sheets и записан в SQLite.')

            # ВАЖНО: после обновления SQLite чистим in-memory кэши,
            # чтобы бот сразу начал читать новые данные без перезапуска.
            try:
                data_cache.clear()
            except Exception:
                pass
            try:
                _invalidate_reply_keyboard_cache(message.chat.id)
            except Exception:
                pass

        else:
            # Покажем хоть какую-то диагностическую инфу
            err_text = result.stderr or result.stdout or 'Неизвестная ошибка'
            if len(err_text) > 1500:
                err_text = err_text[:1500] + '...'
            await message.answer(
                f'❌ Ошибка при обновлении каталога.\n\n'
                f'Код возврата: {result.returncode}\n'
                f'Детали:\n<code>{err_text}</code>',
                parse_mode='HTML'
            )

    except subprocess.TimeoutExpired:
        await message.answer('❌ Обновление каталога заняло слишком много времени и было прервано.')
    except Exception as e:
        await message.answer(f'❌ Не удалось запустить обновление каталога:\n<code>{e}</code>', parse_mode='HTML')


@dp.message(AdminPanel.MainMenu, F.text == '⚙️ Настройки уведомлений')
@retry_on_network_error()
async def show_notification_settings(message: Message, state: FSMContext):
    current_interval = STATS_NOTIFICATION_INTERVAL_DAYS
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='1 день'), KeyboardButton(text='2 дня')], [KeyboardButton(text='3 дня'), KeyboardButton(text='7 дней')], [KeyboardButton(text='Другой интервал')], [KeyboardButton(text='🔙 Назад в админ-панель')]])
    await message.answer(f'⚙️ *Настройки уведомлений*\n\nТекущий интервал отправки статистики: *{current_interval} день(дней)*\n\nВыберите новый интервал или введите свой:', reply_markup=kb)
    await state.set_state(AdminPanel.StatsSettings)

@dp.message(AdminPanel.StatsSettings, F.text == '🔙 Назад в админ-панель')
@retry_on_network_error()
async def back_to_admin_panel(message: Message, state: FSMContext):
    await cmd_admin(message, state)

@dp.message(AdminPanel.StatsSettings, F.text.in_(['1 день', '2 дня', '3 дня', '7 дней']))
@retry_on_network_error()
async def set_notification_interval(message: Message):
    interval_map = {'1 день': 1, '2 дня': 2, '3 дня': 3, '7 дней': 7}
    new_interval = interval_map[message.text]
    if save_stats_settings(new_interval):
        await message.answer(f'✅ Интервал уведомлений изменен на *{new_interval} день(дней)*')
        await show_notification_settings(message, FSMContext)
    else:
        await message.answer('❌ Ошибка при сохранении настроек')

@dp.message(AdminPanel.StatsSettings, F.text == 'Другой интервал')
@retry_on_network_error()
async def ask_custom_interval(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад к настройкам')]])
    await message.answer('Введите интервал в днях (от 1 до 30):', reply_markup=kb)
    await state.set_state(AdminPanel.ChangeNotificationInterval)

@dp.message(AdminPanel.ChangeNotificationInterval, F.text == '🔙 Назад к настройкам')
@retry_on_network_error()
async def back_to_settings(message: Message, state: FSMContext):
    await show_notification_settings(message, state)

@dp.message(AdminPanel.ChangeNotificationInterval)
@retry_on_network_error()
async def set_custom_interval(message: Message, state: FSMContext):
    try:
        new_interval = int(message.text)
        if 1 <= new_interval <= 30:
            if save_stats_settings(new_interval):
                await message.answer(f'✅ Интервал уведомлений изменен на *{new_interval} день(дней)*')
                await show_notification_settings(message, state)
            else:
                await message.answer('❌ Ошибка при сохранении настроек')
        else:
            await message.answer('❌ Интервал должен быть от 1 до 30 дней')
    except ValueError:
        await message.answer('❌ Пожалуйста, введите число от 1 до 30')

@dp.message(AdminPanel.MainMenu, F.text == '🔙 Главное меню')
@retry_on_network_error()
async def back_to_main_from_admin(message: Message, state: FSMContext):
    await state.clear()
    await cmd_start(message, state)

class BroadcastManager:

    def __init__(self):
        self.active_broadcasts = {}
        self.progress_messages = {}

    async def send_broadcast_batch(self, user_ids: List[int], broadcast_type: str, content: str, caption: str, progress_msg: Message):
        success_count = 0
        fail_count = 0
        for user_id in user_ids:
            try:
                if broadcast_type == 'text':
                    await RetryManager.send_message(user_id, content)
                elif broadcast_type == 'photo':
                    await RetryManager.send_photo(user_id, content, caption)
                elif broadcast_type == 'video':
                    await RetryManager.send_video(user_id, content, caption)
                elif broadcast_type == 'video_note':
                    await RetryManager.send_video_note(user_id, content)
                success_count += 1
            except TelegramForbiddenError:
                fail_count += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    if broadcast_type == 'text':
                        await RetryManager.send_message(user_id, content)
                    elif broadcast_type == 'photo':
                        await RetryManager.send_photo(user_id, content, caption)
                    elif broadcast_type == 'video':
                        await RetryManager.send_video(user_id, content, caption)
                    elif broadcast_type == 'video_note':
                        await RetryManager.send_video_note(user_id, content)
                    success_count += 1
                except Exception:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logging.error(f'Ошибка отправки пользователю {user_id}: {e}')
        return (success_count, fail_count)

    async def send_broadcast_with_progress(self, broadcast_type: str, content: str, caption: str, message: Message, user_ids: List[int]):
        total_users = len(user_ids)
        success_count = 0
        fail_count = 0
        progress_msg = await message.answer(f'📤 *Начало рассылки...*\n\n👥 Всего пользователей: {total_users}\n✅ Успешно: 0\n❌ Ошибок: 0\n📊 Прогресс: 0%')
        batches = [user_ids[i:i + BROADCAST_BATCH_SIZE] for i in range(0, len(user_ids), BROADCAST_BATCH_SIZE)]
        for i, batch in enumerate(batches):
            batch_success, batch_fail = await self.send_broadcast_batch(batch, broadcast_type, content, caption, progress_msg)
            success_count += batch_success
            fail_count += batch_fail
            progress_percent = int((i + 1) / len(batches) * 100)
            try:
                await progress_msg.edit_text(f'📤 *Рассылка в процессе...*\n\n👥 Всего пользователей: {total_users}\n✅ Успешно: {success_count}\n❌ Ошибок: {fail_count}\n📊 Прогресс: {progress_percent}%\n🎯 Текущая пачка: {i + 1}/{len(batches)}')
            except Exception:
                pass
            if i < len(batches) - 1:
                await asyncio.sleep(BROADCAST_DELAY)
        try:
            await progress_msg.delete()
        except Exception:
            pass
        return (success_count, fail_count)
broadcast_manager = BroadcastManager()

@dp.message(AdminPanel.MainMenu, F.text == '📢 Сделать рассылку')
@retry_on_network_error()
async def start_broadcast(message: Message, state: FSMContext):
    await state.update_data(broadcast_type=None, broadcast_content=None, broadcast_caption='')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='📝 Текстовая рассылка')], [KeyboardButton(text='🖼️ Рассылка с фото')], [KeyboardButton(text='🎥 Рассылка с видео')], [KeyboardButton(text='📹 Рассылка с видеосообщением')], [KeyboardButton(text='🔙 Назад в админ-панель')]])
    stats = user_stats.get_stats()
    await message.answer(f"📢 *Создание рассылки*\n\n👥 Всего пользователей: {stats['total_users']}\n🎯 Активных за неделю: {stats['active_users_week']}\n\nВыберите тип рассылки:", reply_markup=kb)
    await state.set_state(AdminPanel.Broadcast)

@dp.message(AdminPanel.Broadcast, F.text == '📝 Текстовая рассылка')
@retry_on_network_error()
async def broadcast_text(message: Message, state: FSMContext):
    await state.update_data(broadcast_type='text', broadcast_content='', broadcast_caption='')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
    await message.answer('📝 *Текстовая рассылка*\n\nВведите текст сообщения для рассылки:\n\nПоддерживается Markdown разметка:\n*жирный текст*\n_курсив_\n`моноширинный`', reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminPanel.BroadcastText)

@dp.message(AdminPanel.Broadcast, F.text == '🖼️ Рассылка с фото')
@retry_on_network_error()
async def broadcast_photo(message: Message, state: FSMContext):
    await state.update_data(broadcast_type='photo')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
    await message.answer('🖼️ *Рассылка с фото*\n\nОтправьте фото для рассылки:', reply_markup=kb)
    await state.set_state(AdminPanel.BroadcastMedia)

@dp.message(AdminPanel.Broadcast, F.text == '🎥 Рассылка с видео')
@retry_on_network_error()
async def broadcast_video(message: Message, state: FSMContext):
    await state.update_data(broadcast_type='video')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
    await message.answer('🎥 *Рассылка с видео*\n\nОтправьте видео для рассылки:', reply_markup=kb)
    await state.set_state(AdminPanel.BroadcastMedia)

@dp.message(AdminPanel.Broadcast, F.text == '📹 Рассылка с видеосообщением')
@retry_on_network_error()
async def broadcast_video_note(message: Message, state: FSMContext):
    await state.update_data(broadcast_type='video_note')
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
    await message.answer('📹 *Рассылка с видеосообщением*\n\nОтправьте видеосообщение для рассылки:', reply_markup=kb)
    await state.set_state(AdminPanel.BroadcastMedia)

@dp.message(AdminPanel.Broadcast, F.text == '🔙 Назад в админ-панель')
@retry_on_network_error()
async def back_to_admin_from_broadcast(message: Message, state: FSMContext):
    await cmd_admin(message, state)

@dp.message(AdminPanel.BroadcastText)
@retry_on_network_error()
async def process_broadcast_text(message: Message, state: FSMContext):
    if message.text == '🔙 Назад к выбору типа':
        await start_broadcast(message, state)
        return
    elif message.text == '❌ Отменить рассылку':
        await cancel_broadcast(message, state)
        return
    elif message.text == '🚀 Отправить без текста':
        await state.update_data(broadcast_caption='')
        await show_broadcast_preview(message, state)
        return
    text_content = message.text.strip()
    if not text_content:
        await message.answer('❌ Текст не может быть пустым. Введите текст:')
        return
    data = await state.get_data()
    broadcast_type = data.get('broadcast_type')
    if broadcast_type == 'text':
        await state.update_data(broadcast_content=text_content)
    else:
        await state.update_data(broadcast_caption=text_content)
    await show_broadcast_preview(message, state)

@dp.message(AdminPanel.BroadcastMedia, F.photo)
@retry_on_network_error()
async def process_broadcast_photo(message: Message, state: FSMContext):
    if message.text == '🔙 Назад к выбору типа':
        await start_broadcast(message, state)
        return
    elif message.text == '❌ Отменить рассылку':
        await cancel_broadcast(message, state)
        return
    photo_id = message.photo[-1].file_id
    caption = message.caption if message.caption else ''
    await state.update_data(broadcast_content=photo_id, broadcast_caption=caption)
    if caption:
        await show_broadcast_preview(message, state)
    else:
        kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🚀 Отправить без текста')], [KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
        await message.answer("📝 Хотите добавить текст к фото?\n\nВведите текст подписи или нажмите 'Отправить без текста':", reply_markup=kb)
        await state.set_state(AdminPanel.BroadcastText)

@dp.message(AdminPanel.BroadcastMedia, F.video)
@retry_on_network_error()
async def process_broadcast_video(message: Message, state: FSMContext):
    if message.text == '🔙 Назад к выбору типа':
        await start_broadcast(message, state)
        return
    elif message.text == '❌ Отменить рассылку':
        await cancel_broadcast(message, state)
        return
    video_id = message.video.file_id
    caption = message.caption if message.caption else ''
    await state.update_data(broadcast_content=video_id, broadcast_caption=caption)
    if not caption:
        kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🚀 Отправить без текста')], [KeyboardButton(text='🔙 Назад к выбору типа')], [KeyboardButton(text='❌ Отменить рассылку')]])
        await message.answer("📝 Хотите добавить текст к видео?\n\nВведите текст подписи или нажмите 'Отправить без текста':", reply_markup=kb)
        await state.set_state(AdminPanel.BroadcastText)
    else:
        await show_broadcast_preview(message, state)

@dp.message(AdminPanel.BroadcastMedia, F.video_note)
@retry_on_network_error()
async def process_broadcast_video_note(message: Message, state: FSMContext):
    if message.text == '🔙 Назад к выбору типа':
        await start_broadcast(message, state)
        return
    elif message.text == '❌ Отменить рассылку':
        await cancel_broadcast(message, state)
        return
    video_note_id = message.video_note.file_id
    await state.update_data(broadcast_content=video_note_id, broadcast_caption='')
    await show_broadcast_preview(message, state)

@dp.message(AdminPanel.BroadcastText, F.text == '🚀 Отправить без текста')
@retry_on_network_error()
async def send_without_text(message: Message, state: FSMContext):
    await state.update_data(broadcast_caption='')
    await show_broadcast_preview(message, state)

@retry_on_network_error()
async def show_broadcast_preview(message: Message, state: FSMContext):
    data = await state.get_data()
    broadcast_type = data.get('broadcast_type')
    content = data.get('broadcast_content')
    caption = data.get('broadcast_caption', '')
    stats = user_stats.get_stats()
    preview_text = f"📢 *ПРЕДПРОСМОТР РАССЫЛКИ*\n\n👥 Будет отправлено: {stats['total_users']} пользователям\n🎯 Активных за неделю: {stats['active_users_week']}\n\n"
    if broadcast_type == 'text':
        preview_text += f'📝 *Текст:*\n{content}\n\n'
    elif broadcast_type == 'photo':
        preview_text += f'🖼️ *Тип:* Фото с текстом\n'
        preview_text += f"📝 *Текст:* {(caption if caption else 'Без текста')}\n\n"
    elif broadcast_type == 'video':
        preview_text += f'🎥 *Тип:* Видео с текстом\n'
        preview_text += f"📝 *Текст:* {(caption if caption else 'Без текста')}\n\n"
    elif broadcast_type == 'video_note':
        preview_text += f'📹 *Тип:* Видеосообщение\n\n'
    preview_text += '✅ *Подтвердите отправку:*'
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='✅ Запустить рассылку')], [KeyboardButton(text='✏️ Изменить содержимое')], [KeyboardButton(text='❌ Отменить рассылку')]])
    try:
        if broadcast_type == 'text':
            await message.answer(preview_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        elif broadcast_type == 'photo':
            if caption:
                await message.answer_photo(content, caption=f'{caption}\n\n{preview_text}', reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            else:
                await message.answer_photo(content, caption=preview_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        elif broadcast_type == 'video':
            if caption:
                await message.answer_video(content, caption=f'{caption}\n\n{preview_text}', reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            else:
                await message.answer_video(content, caption=preview_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        elif broadcast_type == 'video_note':
            await message.answer_video_note(content)
            await message.answer(preview_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f'Ошибка предпросмотра: {e}')
        error_preview = f'{preview_text}\n\n❌ *Ошибка предпросмотра медиа:* {e}'
        await message.answer(error_preview, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminPanel.BroadcastConfirmation)

@retry_on_network_error()
async def cancel_broadcast(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='🔙 В админ-панель')], [KeyboardButton(text='🔙 Главное меню')]])
    await message.answer('❌ Рассылка отменена', reply_markup=kb)
    await state.set_state(AdminPanel.MainMenu)

@dp.message(AdminPanel.BroadcastConfirmation, F.text == '✅ Запустить рассылку')
@retry_on_network_error()
async def confirm_broadcast(message: Message, state: FSMContext):
    data = await state.get_data()
    broadcast_type = data.get('broadcast_type')
    content = data.get('broadcast_content')
    caption = data.get('broadcast_caption', '')
    if not broadcast_type:
        await message.answer('❌ Ошибка: Тип рассылки не определен')
        await start_broadcast(message, state)
        return
    if broadcast_type == 'text' and (not content or content is None):
        await message.answer('❌ Ошибка: Текст рассылки не может быть пустым')
        await broadcast_text(message, state)
        return
    if broadcast_type in ['photo', 'video'] and (not content or content is None):
        await message.answer('❌ Ошибка: Медиа-файл не найден. Попробуйте заново.')
        await start_broadcast(message, state)
        return
    if broadcast_type in ['photo', 'video'] and content:
        try:
            file_info = await bot.get_file(content)
            if not file_info:
                await message.answer('❌ Ошибка: Неверный идентификатор файла. Попробуйте отправить медиа заново.')
                await start_broadcast(message, state)
                return
        except Exception as e:
            await message.answer(f'❌ Ошибка: Неверный идентификатор файла. Попробуйте отправить медиа заново.\n\nОшибка: {e}')
            await start_broadcast(message, state)
            return
    stats = user_stats.get_stats()
    total_users = stats['total_users']
    user_ids = list(user_stats._users.keys())
    if not user_ids:
        await message.answer('❌ Нет пользователей для рассылки')
        return
    await message.answer(f'🚀 *Запуск улучшенной рассылки...*\n\n👥 Пользователей: {total_users}\n📦 Пачек: {len(user_ids) // BROADCAST_BATCH_SIZE + 1}\n⏱️ Примерное время: {len(user_ids) * BROADCAST_DELAY / 60:.1f} минут\n\n⏳ Начинаем отправку...', reply_markup=ReplyKeyboardRemove())
    success_count, fail_count = await broadcast_manager.send_broadcast_with_progress(broadcast_type, content, caption, message, user_ids)
    report_text = f'📊 *ОТЧЕТ О РАССЫЛКЕ*\n\n✅ Успешно отправлено: {success_count}\n❌ Не удалось отправить: {fail_count}\n'
    if success_count + fail_count > 0:
        effectiveness = success_count / (success_count + fail_count) * 100
        report_text += f'📈 Эффективность: {effectiveness:.1f}%\n\n'
    report_text += f"🕒 Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text='📢 Новая рассылка')], [KeyboardButton(text='🔙 В админ-панель')], [KeyboardButton(text='🔙 Главное меню')]])
    await message.answer(report_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AdminPanel.MainMenu)

@dp.message(AdminPanel.BroadcastConfirmation, F.text == '✏️ Изменить содержимое')
@retry_on_network_error()
async def edit_broadcast_content(message: Message, state: FSMContext):
    data = await state.get_data()
    broadcast_type = data.get('broadcast_type')
    if broadcast_type == 'text':
        await broadcast_text(message, state)
    else:
        await start_broadcast(message, state)

@dp.message(AdminPanel.BroadcastConfirmation, F.text == '❌ Отменить рассылку')
@retry_on_network_error()
async def cancel_broadcast_confirmation(message: Message, state: FSMContext):
    await cancel_broadcast(message, state)

@dp.message(AdminPanel.MainMenu, F.text == '📢 Новая рассылка')
@retry_on_network_error()
async def new_broadcast_from_menu(message: Message, state: FSMContext):
    await start_broadcast(message, state)


@retry_on_network_error()
async def send_order_to_admin(
    data: dict,
    save_success: bool = True,
    order_number: str = "",
    applied_certificate: dict = None
):
    """
    Отправляет администратору структурированное сообщение о заказе:
    - шапка (клиент, контакты, статус сохранения)
    - состав заказа с объединением бюстов и поясов
    - информация о сертификате
    - итоги по сумме
    """
    try:
        # --- ШАПКА ЗАКАЗА ---
        timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
        user = escape_markdown(data.get("user_name", "Неизвестный пользователь"))
        phone = escape_markdown(data.get("phone", "Не указан"))
        delivery = escape_markdown(data.get("delivery", "Не указан"))

        promo_settings = get_promo_settings()
        promo_price = promo_settings.get("PANTIES_PROMO_PRICE", 6500)
        promo_count = promo_settings.get("PANTIES_PROMO_COUNT", 3)

        admin_message_lines: list[str] = []

        admin_message_lines.append(f"📦 *НОВЫЙ ЗАКАЗ* #{order_number}")
        admin_message_lines.append(f"🕒 *Время:* {timestamp}")
        admin_message_lines.append("")
        admin_message_lines.append(f"👤 *Клиент:* {user}")
        admin_message_lines.append(f"📞 *Телефон:* {phone}")
        admin_message_lines.append(f"🚚 *Способ получения:* {delivery}")

        if applied_certificate and applied_certificate.get("valid"):
            admin_message_lines.append(
                f"🎫 *Применен сертификат:* {applied_certificate['amount']} руб. (№{applied_certificate['number']})"
            )

        admin_message_lines.append(
            "💾 *Статус:* " + ("✅ Сохранен в таблицу" if save_success else "❌ Ошибка сохранения")
        )

        # --- МЕРКИ ---
        measurements_map = [
            ("bust", "Обхват груди"),
            ("horizontal_arc", "Горизонтальная дуга"),
            ("underbust", "Обхват под грудью"),
            ("waist", "Обхват талии"),
            ("hips", "Обхват бедер"),
        ]

        has_measurements = False
        for key, label in measurements_map:
            val = data.get(key)
            if val:
                if not has_measurements:
                    admin_message_lines.append("📏 *Мерки:*")
                    has_measurements = True
                admin_message_lines.append(f"   {label}: {val} см")

        # --- ПОЖЕЛАНИЯ + ФОТО / АДРЕС / EMAIL ---
        order_notes = data.get("order_notes", "Не указано")
        admin_message_lines.append(f"💭 *Пожелания:* {escape_markdown(order_notes)}")

        if "photo_id" in data and data["photo_id"]:
            admin_message_lines.append("📸 *Фото клиента:* ✅ Приложено")
        else:
            admin_message_lines.append("📸 *Фото клиента:* ❌ Не приложено")

        delivery_raw = data.get("delivery", "")
        if delivery_raw not in ("Самовывоз", "Электронная доставка"):
            address = escape_markdown(data.get("address", "не указан"))
            admin_message_lines.append(f"📍 *Адрес доставки:* {address}")
        elif delivery_raw == "Электронная доставка":
            email_info = data.get("address", "Email не указан")
            admin_message_lines.append(f"📧 *Email для сертификата:* {escape_markdown(email_info)}")

        admin_message_lines.append("")
        admin_message_lines.append("*Состав заказа:*")

        cart = data.get("cart", []) or []

        # сумма заказа (для админа). В data обычно есть total_amount
        total_order_amount = data.get("total_amount")
        if total_order_amount is None:
            total_order_amount = data.get("total_order_amount")
        if total_order_amount is None:
            total_order_amount = 0
        admin_message_lines.append(build_admin_order_items_text(cart))

        admin_message_lines.append(f"\n💰 *ИТОГОВАЯ СУММА ЗАКАЗА:* {int(total_order_amount)} ₽")
        admin_message_lines.append(f"\n🔢 *НОМЕР ДЛЯ ПОИСКА:* {order_number}")

        admin_message = "\n".join(admin_message_lines)

        # --- Отправка админу ---
        try:
            with timer("tg.send_message.admin", "-"):
                await bot.send_message(ADMIN_CHAT_ID, admin_message, parse_mode=ParseMode.MARKDOWN)

            # Фото клиента (если было)
            if data.get("photo_id"):
                try:
                    with timer("tg.send_photo.admin_client", "-"):
                        await bot.send_photo(
                            ADMIN_CHAT_ID,
                            data["photo_id"],
                            caption=f"Фото клиента — заказ #{order_number}",
                        )
                except Exception as e:
                    print(f"Ошибка отправки фото клиента админу: {e}")
                    await bot.send_message(
                        ADMIN_CHAT_ID,
                        f"❌ Не удалось отправить фото клиента для заказа {order_number}",
                    )

            # Фото товаров заказа — ОДИН раз (после текста и фото клиента)
            # ВАЖНО: отправляем админу ТОЧНО те же картинки, что показывали пользователю на подтверждении
            order_images = data.get("order_images") or []

            if order_images:
                try:
                    # Telegram позволяет отправлять медиа-группы до 10 элементов
                    from aiogram.types import InputMediaPhoto

                    chunk_size = 10
                    for start in range(0, len(order_images), chunk_size):
                        chunk = order_images[start:start + chunk_size]
                        media = [InputMediaPhoto(media=url) for url in chunk]
                        await bot.send_media_group(ADMIN_CHAT_ID, media)
                except Exception as e:
                    print(f"Ошибка отправки order_images админу: {e}")
                    # fallback
                    cart_for_photos = [dict(i) for i in (data.get("cart") or [])]
                    if cart_for_photos:
                        try:
                            await send_cart_photos_to(ADMIN_CHAT_ID, cart_for_photos)
                        except Exception as e2:
                            print(f"Ошибка отправки фото товаров админу: {e2}")
                            await bot.send_message(
                                ADMIN_CHAT_ID,
                                f"❌ Не удалось отправить фото товаров для заказа {order_number}",
                            )
            else:
                cart_for_photos = [dict(i) for i in (data.get("cart") or [])]
                if cart_for_photos:
                    try:
                        await send_cart_photos_to(ADMIN_CHAT_ID, cart_for_photos)
                    except Exception as e:
                        print(f"Ошибка отправки фото товаров админу: {e}")
                        await bot.send_message(
                            ADMIN_CHAT_ID,
                            f"❌ Не удалось отправить фото товаров для заказа {order_number}",
                        )

        except Exception as e:
            print(f"Markdown error при отправке админу: {e}")
            plain_text = re.sub(r"\*([^*]+)\*", r"\1", admin_message)
            plain_text = re.sub(
                "📦|👤|📞|🚚|📍|💰|📏|📸|💭|🎉|📧|✅|❌|🆔|👕|🕒|💾|🔢|🎫|👙|🩲|📝|🖤",
                "",
                plain_text,
            )
            await bot.send_message(ADMIN_CHAT_ID, plain_text)


    except Exception as e:
        print(f"Error sending order to admin: {e}")
        try:
            # Без parse_mode, чтобы не ломаться на символах в тексте ошибки
            await bot.send_message(
                ADMIN_CHAT_ID,
                f"❌ Ошибка при отправке заказа: {e}",
                parse_mode=None
            )
        except Exception as inner_e:
            print(f"Failed to send error message: {inner_e}")


@retry_on_network_error()

def _normalize_photo_ref(photo_ref: str) -> str:
    """Normalize photo reference for Telegram API.
    Supports:
      - Telegram file_id (returned as-is)
      - HTTP(S) URL (returned as-is)
      - Google Drive file id or share link -> converted to direct view URL
    """
    if not photo_ref:
        return ""
    raw = str(photo_ref).strip().strip('"').strip("'")
    if not raw:
        return ""
    # If it's already a URL or an attach:// reference, keep it
    if raw.startswith("http://") or raw.startswith("https://") or raw.startswith("attach://"):
        return raw

    # Google Drive: raw might be just the file id
    # Typical file id length is 20-80 and contains letters, digits, '-' and '_'
    import re
    if re.fullmatch(r"[A-Za-z0-9_-]{20,100}", raw):
        return f"https://drive.google.com/uc?export=view&id={raw}"

    # Google Drive share link variants
    m = re.search(r"(?:id=|/d/)([A-Za-z0-9_-]{20,100})", raw)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=view&id={file_id}"

    # Otherwise assume it's a Telegram file_id or something Telegram can handle
    return raw


async def show_measurement_guide(chat_id: int, bot=None) -> bool:
    # Поддержка старого вызова: show_measurement_guide(chat_id)
    # Если bot не передан, берём глобальный экземпляр (как в остальном core.py)
    if bot is None:
        bot = globals().get('bot')
    if bot is None:
        raise RuntimeError("Bot instance is not available for show_measurement_guide")

    """Показывает картинку 'Замеры' перед вводом мерок.

    Ищем строку в БД (products) по type/category='Замеры' (и иногда по SKU M_%).
    Картинка может лежать в разных полях в зависимости от миграций — поэтому берём из нескольких источников.
    """
    try:
        rows = query_products(product_type="Замеры")
    except Exception as e:
        print(f"❌ Ошибка при поиске 'Замеры' в БД: {e}")
        await bot.send_message(chat_id, "❌ Не удалось найти картинку 'Замеры' (ошибка БД).")
        return False

    if not rows:
        await bot.send_message(chat_id, "❌ Не нашлась строка 'Замеры' в каталоге.")
        return False

    row = rows[0]

    def _get_photo_ref(r: dict) -> str | None:
        # 1) прямые поля (в разных версиях схемы)
        for k in (
            "Изображение",
            "Изображение модели",
            "Изображение материала",
            "model_photo_id",
            "model_image",
            "image",
            "photo_id",
            "photo",
            "ModelPhotoId",
            "ModelPhotoID",
        ):
            v = r.get(k)
            if v:
                return v

        # 2) иногда исходная строка лежит внутри raw_json
        raw = r.get("raw_json")
        if isinstance(raw, str):
            try:
                import json  # локально, чтобы не зависеть от верхних импортов
                raw = json.loads(raw)
            except Exception:
                raw = None

        if isinstance(raw, dict):
            for k in (
                "ModelPhotoId",
                "ModelPhotoID",
                "ModelPhotoId ",
                "Image",
                "Изображение",
                "Фото",
                "model_photo_id",
                "model_image",
                "image",
            ):
                v = raw.get(k)
                if v:
                    return v
        return None

    photo_ref = _get_photo_ref(row)

    if not photo_ref:
        keys = ", ".join(sorted(row.keys()))
        print(f"⚠️ Найдена строка 'Замеры', но поле картинки пустое. keys={keys}")
        await bot.send_message(
            chat_id,
            "⚠️ Найдена строка 'Замеры', но поле картинки пустое. "
            "Проверь, что в строке 'Замеры' заполнен столбец ModelPhotoId (или Image/Изображение), "
            "и что после изменения ты нажал 'Обновить каталог'.",
        )
        return False

    photo_to_send = _normalize_photo_ref(photo_ref)
    try:
        await bot.send_photo(chat_id, photo_to_send, caption="📏 Замеры (как правильно снять мерки)")
        return True
    except Exception as e:
        print(f"❌ Ошибка при отправке картинки 'Замеры': {e}")
        await bot.send_message(chat_id, "❌ Не удалось отправить картинку с мерками.")
        return False

async def send_stats_to_admin():
    try:
        stats = user_stats.get_stats()
        stats_text = f"📊 *Статистика пользователей*\n\n👥 *Всего пользователей:* {stats['total_users']}\n🆕 *Новых за сегодня:* {stats['new_users_today']}\n📈 *Новых за неделю:* {stats['new_users_week']}\n🎯 *Активных за сегодня:* {stats['active_users_today']}\n🔥 *Активных за неделю:* {stats['active_users_week']}\n\n⏰ *Отчет за:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        await bot.send_message(ADMIN_CHAT_ID, stats_text)
        user_stats.save_stats_to_sheet(order_manager)
    except Exception as e:
        print(f'Ошибка отправки статистики администратору: {e}')

class BotMonitor:

    def __init__(self):
        self.start_time = None
        self.error_count = 0
        self.last_error_time = None

    async def start_monitoring(self):
        self.start_time = datetime.now()
        print(f"🤖 Бот запущен в {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        asyncio.create_task(self.periodic_health_check())

    async def periodic_health_check(self):
        while True:
            try:
                me = await bot.get_me()
                print(f'✅ Соединение с Telegram стабильно. Бот: @{me.username}')
                worksheet = order_manager._get_worksheet()
                if worksheet:
                    print('✅ Соединение с Google Sheets стабильно')
                else:
                    print('⚠️ Проблемы с соединением Google Sheets')
            except Exception as e:
                self.error_count += 1
                self.last_error_time = datetime.now()
                print(f'❌ Ошибка проверки здоровья: {e}')
            await asyncio.sleep(300)

    def get_status(self):
        return {'uptime': str(datetime.now() - self.start_time) if self.start_time else 'Не запущен', 'error_count': self.error_count, 'last_error': self.last_error_time.strftime('%Y-%m-%d %H:%M:%S') if self.last_error_time else 'Нет ошибок', 'users_count': len(user_stats._users), 'active_broadcasts': len(broadcast_manager.active_broadcasts)}
bot_monitor = BotMonitor()

@retry_on_network_error()
async def main(extra_tasks=None):
    """
    Главная точка входа бота.

    extra_tasks: список функций вида async def task(bot),
    которые нужно запустить как фоновые задачи (например,
    catalog_health_notifier_loop из bot.py).
    """
    if extra_tasks is None:
        extra_tasks = []

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )

    # Инициализируем локальную БД каталога
    init_db()
    # Инициализируем кэш file_id для фото (ускоряет слайдеры после 1-го прогрева)
    try:
        await photo_cache_service.init()
        logging.info("photo_cache_service initialized")
    except Exception as e:
        logging.warning("photo_cache_service init failed: %s", e)

    # Загружаем настройки промо один раз при старте (дальше обновление вручную в админ-панели)
    try:
        await asyncio.to_thread(refresh_promo_settings_from_sheets)
        logging.info('promo_settings loaded')
    except Exception as e:
        logging.warning('promo_settings load failed: %s', e)

    print('🤖 Бот запускается...')
    print('🔧 Проверяем подключение к Google Таблицам...')

    global STATS_NOTIFICATION_INTERVAL_DAYS
    loaded_interval = load_stats_settings()
    STATS_NOTIFICATION_INTERVAL_DAYS = loaded_interval if loaded_interval is not None else 1
    print(f'✅ Интервал уведомлений статистики: {STATS_NOTIFICATION_INTERVAL_DAYS} дней')

    # Проверка Google-таблиц
    try:
        worksheet = order_manager._get_worksheet()
        if worksheet:
            print('✅ Подключение к Google Таблицам успешно установлено')
        else:
            print('❌ Ошибка подключения к Google Таблицам')
    except Exception as e:
        print(f'❌ Ошибка при проверке подключения к Google Таблицам: {e}')

    # Проверка таблицы сертификатов
    try:
        cert_worksheet = certificate_manager._get_worksheet()
        if cert_worksheet:
            print('✅ Подключение к таблице сертификатов успешно установлено')
        else:
            print('❌ Ошибка подключения к таблице сертификатов')
    except Exception as e:
        print(f'❌ Ошибка при проверке подключения к таблице сертификатов: {e}')

    # Мониторинг бота
    await bot_monitor.start_monitoring()
    print('🚀 Бот готов к работе!')

    # Стартовое уведомление админу
    try:
        await bot.send_message(ADMIN_CHAT_ID, '🤖 Бот запущен и готов к работе!')
        await send_stats_to_admin()
        user_stats.mark_notification_sent()
    except Exception as e:
        print(f'Ошибка отправки стартового уведомления: {e}')

    # 🔹 ЗАПУСК ФОНОВЫХ ЗАДАЧ (например, напоминание про каталог)
    for task_func in extra_tasks:
        try:
            asyncio.create_task(task_func(bot))
            # можно добавить отладочный print, если хочешь:
            # print(f'✅ Фоновая задача {task_func.__name__} запущена')
        except Exception as e:
            print(f'❌ Не удалось запустить фоновую задачу {getattr(task_func, "__name__", task_func)}: {e}')

    # Поллинг
    try:
        print('🔄 Запуск поллинга...')
        await dp.start_polling(bot)
    except Exception as e:
        print(f'Критическая ошибка при запуске бота: {e}')
        print('🔄 Попытка перезапуска через 30 секунд...')
        await asyncio.sleep(30)
        # важно передать extra_tasks дальше, чтобы фоновые задачи не потерялись
        await main(extra_tasks=extra_tasks)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n🛑 Бот остановлен пользователем')
    except Exception as e:
        print(f'❌ Непредвиденная ошибка: {e}')