import asyncio
import logging

from app.core import main
from catalog_health import get_catalog_health
from config import ADMIN_IDS


async def notify_admins_catalog_health(bot):
    """Отправляет админу health-уведомление (если нужно). Не роняет процесс."""
    try:
        health = get_catalog_health()
        # health может быть dict
        msg = None
        if isinstance(health, dict):
            msg = health.get("message_to_admin")
        else:
            msg = getattr(health, "message_to_admin", None)

        if not msg:
            return

        if not ADMIN_CHAT_ID:
            logging.warning("ADMIN_CHAT_ID не задан — уведомление о здоровье каталога пропущено.")
            return

        await bot.send_message(ADMIN_CHAT_ID, msg)
    except Exception as e:
        logging.exception(f"Ошибка notify_admins_catalog_health: {e}")


    if not health.message_to_admin:
        print(f"✅ Каталог OK (age_days={health.age_days}, status={health.status})")
        return

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, health.message_to_admin)
            print(f"📨 Отправлено предупреждение админу {admin_id}")
        except Exception as e:
            print(f"❌ Ошибка отправки админу {admin_id}: {e}")


async def catalog_health_notifier_loop():
    """Фоновая проверка каталога. Никогда не падает наружу."""
    while True:
        try:
            await asyncio.sleep(60 * 30)  # каждые 30 минут
            await notify_admins_catalog_health(bot)
        except Exception as e:
            logging.exception(f"Ошибка catalog_health_notifier_loop: {e}")
            await asyncio.sleep(60)


    while True:
        await notify_admins_catalog_health(bot)
        await asyncio.sleep(24 * 60 * 60)  # 24 часа


if __name__ == "__main__":
    try:
        asyncio.run(main(extra_tasks=[catalog_health_notifier_loop]))
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.exception("❌ Ошибка запуска бота: %s", e)
