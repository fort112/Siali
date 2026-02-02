import asyncio
import logging

from app.core import main
from catalog_health import get_catalog_health
from config import ADMIN_IDS


async def notify_admins_catalog_health(bot):
    """
    Разовая проверка состояния каталога и отправка уведомления администраторам,
    если нужно (после 5 и 10 дней).
    """
    try:
        health = get_catalog_health()
    except Exception as e:
        print(f"❌ Ошибка проверки состояния каталога: {e}")
        return

    if not health.message_to_admin:
        print(f"✅ Каталог OK (age_days={health.age_days}, status={health.status})")
        return

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, health.message_to_admin)
            print(f"📨 Отправлено предупреждение админу {admin_id}")
        except Exception as e:
            print(f"❌ Ошибка отправки админу {admin_id}: {e}")


async def catalog_health_notifier_loop(bot):
    """
    Раз в сутки проверяет состояние каталога и напоминает админу.
    """
    await asyncio.sleep(10)   # задержка после старта

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
