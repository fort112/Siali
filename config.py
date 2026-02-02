import os
import logging
from dotenv import load_dotenv
load_dotenv()

# Конфигурация бота.
# Токен и ID администратора берутся из переменных окружения:
#
#   BOT_TOKEN=123456:ABCDEF...
#   ADMIN_CHAT_ID=352103221
#
# Не храните реальные токены в коде/репозитории.

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID_RAW = os.getenv("ADMIN_CHAT_ID")

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в переменных окружения")

if ADMIN_CHAT_ID_RAW is None:
    ADMIN_CHAT_ID = None
    logging.warning("ADMIN_CHAT_ID не задан. Функции, зависящие от ID админа, могут работать некорректно.")
else:
    try:
        ADMIN_CHAT_ID = int(ADMIN_CHAT_ID_RAW)
    except ValueError:
        raise ValueError("ADMIN_CHAT_ID должно быть целым числом")

# Список админов (для удобства рассылок и т.п.)
if ADMIN_CHAT_ID is not None:
    ADMIN_IDS = [ADMIN_CHAT_ID]
else:
    ADMIN_IDS = []
