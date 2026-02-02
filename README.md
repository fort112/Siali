# Полноценный модульный проект Telegram-бота

Этот проект собран из твоего исходного большого файла и организован по модулям.

## Структура

- `bot.py` — точка входа. Запускает `main()` из `app.core`.
- `config.py` — конфигурация токена и ID администратора (через переменные окружения).
- `requirements.txt` — зависимости.
- `app/`
  - `core.py` — основной код бота (вся логика, как в исходном файле, без захардкоженных токенов).
  - `handlers/` — обёртка над хендлерами (`from app.core import *`), задел на дальнейшее разнесение.
  - `services/`
    - `sheets.py` — реэкспорт классов для работы с Google Sheets (`CertificateManager`, `GoogleSheetsOrderManager`), если они есть.
  - `domain/`
    - `cart.py` — реэкспорт классов и функций корзины (если есть в core).
  - `keyboards/`
    - `common.py` — реэкспорт общих клавиатур (если есть в core).
  - `utils/`
    - `retry.py` — реэкспорт утилит для повторов запросов (если есть в core).

Вся реальная логика сейчас физически находится в `app/core.py`, но вокруг неё уже есть модульная обвязка,
которую можно постепенно использовать для более глубокого рефакторинга:
переносить конкретные куски кода из `core.py` в соответствующие модули.

## Настройка и запуск

1. Установи зависимости:

   ```bash
   pip install -r requirements.txt
   ```

2. Задай переменные окружения:

   **Linux / macOS / WSL:**
   ```bash
   export BOT_TOKEN="твой_токен_бота"
   export ADMIN_CHAT_ID="твой_telegram_id"
   ```

   **Windows (PowerShell):**
   ```powershell
   $env:BOT_TOKEN = "твой_токен_бота"
   $env:ADMIN_CHAT_ID = "твой_telegram_id"
   ```

   При желании можешь использовать `.env` и `python-dotenv`.

3. Убедись, что у тебя есть `credentials.json` для Google Sheets и корректно настроены таблицы,
   как это было в исходном проекте.

4. Запусти бота:

   ```bash
   python bot.py
   ```

Если позже захочешь углубить рефакторинг — можно постепенно выносить
из `app/core.py` реальные реализации в:
- `app/handlers/*.py` — по разделам (каталог, корзина, сертификаты, админка и т.д.);
- `app/services/*.py` — работу с Google Sheets, сертификатами, промо и т.п.;
- `app/domain/*.py` — чистую доменную логику (корзина, товары, акции);
- `app/keyboards/*.py` — все функции, создающие клавиатуры.


## Deploy to bothost.ru (PRO)

1. Push this repository to GitHub (do **not** commit `.env`, `credentials.json`, `*.db`).
2. In bothost panel: create Python project, connect GitHub repo (branch `main`).
3. Set start command: `python bot.py`
4. Add environment variables:
   - `BOT_TOKEN`
   - `ADMIN_CHAT_ID`
   - `DATABASE_PATH=/data/bot_data.db`
   - `PHOTO_CACHE_PATH=/data/photo_cache.json`
   - `GOOGLE_CREDENTIALS_PATH=/data/credentials.json`
5. Upload `credentials.json` into `/data/credentials.json` via bothost file manager.

`/data` is persistent storage, so database and caches will survive redeploys.
