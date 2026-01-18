"""
Головний файл: запуск бота та циклу парсингу.
"""
import asyncio
import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramConflictError
import db
import scraper
import utils
import bot

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
os.makedirs("logs", exist_ok=True)

class JSONFormatter(logging.Formatter):
    """JSON форматтер для логов."""
    def format(self, record):
        log_data = {
            'time': datetime.now(timezone.utc).isoformat(),
            'level': record.levelname,
            'module': record.module,
            'msg': record.getMessage(),
        }
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_data, ensure_ascii=False)


# Настройка логгера
logger = logging.getLogger()
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))

# Консольный handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(JSONFormatter())
logger.addHandler(console_handler)

# Файловый handler
file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
file_handler.setFormatter(JSONFormatter())
logger.addHandler(file_handler)

# Конфигурация
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не задан в переменных окружения!")

POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '180'))
MAX_SEND_PER_MINUTE = int(os.getenv('MAX_SEND_PER_MINUTE', '1'))
# Нормализация таймзоны: Europe/Uzhgorod -> Europe/Kyiv
tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
TIMEZONE = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env

logger.info("Запуск бота...")

def cleanup_tmp_dir(max_age_seconds: int = 86400) -> None:
    """
    Удалить временные PNG из tmp/ старше 1 дня (или другого значения).
    """
    tmp_dir = Path("tmp")
    if not tmp_dir.exists():
        return
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_seconds
    removed = 0
    for p in tmp_dir.glob("schedule_*.png"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
                removed += 1
        except Exception as e:
            logger.warning(f"Не вдалося видалити тимчасовий файл {p}: {e}")
    if removed:
        logger.info(f"Очищено tmp/: видалено {removed} старих файлів")


async def scrape_loop_task(bot_instance: Bot):
    """Основной цикл парсинга сайта."""
    logger.info(f"Запуск цикла парсинга (интервал: {POLL_INTERVAL_SECONDS} сек)")
    
    while True:
        try:
            logger.info("Начало парсинга сайта...")
            
            # Парсинг в отдельном потоке (Selenium блокирующий)
            snapshot = await asyncio.to_thread(scraper.parse_schedule_snapshot, TIMEZONE)
            
            if not snapshot:
                logger.warning("Парсинг не удался, пропускаем итерацию")
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            today_snapshot = snapshot.get("today")
            tomorrow_snapshot = snapshot.get("tomorrow")

            # -----------------------
            # TODAY
            # -----------------------
            if today_snapshot:
                schedule_date = today_snapshot["schedule_date"]
                groups_data = today_snapshot["groups"]

                for group_code, intervals in groups_data.items():
                    off_intervals = intervals.get("off", [])
                    on_intervals = intervals.get("on", [])
                    maybe_intervals = intervals.get("maybe", [])

                    new_hash = utils.compute_group_hash(
                        schedule_date, off_intervals, on_intervals, maybe_intervals
                    )

                    old_state = db.get_group_state(group_code)
                    if old_state and old_state["hash"] == new_hash:
                        continue

                    logger.info(f"Обнаружено изменение для группы {group_code} (сьогодні)")

                    data_json = json.dumps(
                        {"off": off_intervals, "on": on_intervals, "maybe": maybe_intervals},
                        ensure_ascii=False,
                    )
                    db.save_group_state(group_code, schedule_date, new_hash, data_json)

                    subscribers = db.get_subscribed_users_for_group(group_code)
                    sent_count = 0
                    for subscriber in subscribers:
                        user_id = subscriber["tg_user_id"]
                        chat_id = subscriber["tg_chat_id"]

                        success = await bot.send_schedule_updated_package(
                            bot_instance,
                            chat_id,
                            user_id,
                            group_code,
                            schedule_date,
                            on_intervals,
                            off_intervals,
                            maybe_intervals,
                            TIMEZONE,
                            MAX_SEND_PER_MINUTE,
                        )
                        if success:
                            sent_count += 1
                        await asyncio.sleep(0.1)

                    logger.info(f"Отправлено {sent_count} уведомлений для группы {group_code} (сьогодні)")

            # -----------------------
            # TOMORROW
            # -----------------------
            if tomorrow_snapshot:
                schedule_date = tomorrow_snapshot["schedule_date"]
                groups_data = tomorrow_snapshot["groups"]

                for group_code, intervals in groups_data.items():
                    off_intervals = intervals.get("off", [])
                    on_intervals = intervals.get("on", [])
                    maybe_intervals = intervals.get("maybe", [])

                    new_hash = utils.compute_group_hash(
                        schedule_date, off_intervals, on_intervals, maybe_intervals
                    )

                    old_state = db.get_group_state_tomorrow(group_code)
                    if old_state and old_state["hash"] == new_hash:
                        continue

                    is_first_for_this_date = (not old_state) or (old_state.get("schedule_date") != schedule_date)
                    logger.info(
                        f"Обнаружено изменение для группы {group_code} (завтра, first={is_first_for_this_date})"
                    )

                    data_json = json.dumps(
                        {"off": off_intervals, "on": on_intervals, "maybe": maybe_intervals},
                        ensure_ascii=False,
                    )
                    db.save_group_state_tomorrow(group_code, schedule_date, new_hash, data_json)

                    subscribers = db.get_subscribed_users_for_group(group_code)
                    sent_count = 0
                    for subscriber in subscribers:
                        user_id = subscriber["tg_user_id"]
                        chat_id = subscriber["tg_chat_id"]

                        success = await bot.send_schedule_tomorrow_updated_package(
                            bot_instance,
                            chat_id,
                            user_id,
                            group_code,
                            schedule_date,
                            on_intervals,
                            off_intervals,
                            maybe_intervals,
                            TIMEZONE,
                            MAX_SEND_PER_MINUTE,
                            is_first_for_this_date,
                        )
                        if success:
                            sent_count += 1
                        await asyncio.sleep(0.1)

                    logger.info(f"Отправлено {sent_count} уведомлений для группы {group_code} (завтра)")
            
            logger.info("Парсинг завершен успешно")
            
        except Exception as e:
            logger.error(f"Ошибка в цикле парсинга: {e}", exc_info=True)
        
        # Ожидание перед следующей итерацией
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def bot_task():
    """Задача запуска бота."""
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(bot.router)
    
    bot_instance = Bot(token=TELEGRAM_BOT_TOKEN)
    
    logger.info("Бот запущен и готов к работе")
    
    try:
        await dp.start_polling(bot_instance)
    finally:
        await bot_instance.session.close()


async def main():
    """Главная функция."""
    cleanup_tmp_dir()
    # Инициализация БД
    db.init_db()
    
    # Создаем экземпляр бота для передачи в scrape_loop
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(bot.router)
    bot_instance = Bot(token=TELEGRAM_BOT_TOKEN)
    
    # Запускаем обе задачи
    await asyncio.gather(
        dp.start_polling(bot_instance),
        scrape_loop_task(bot_instance)
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка бота...")
    except TelegramConflictError as e:
        logger.error(f"Конфликт Telegram: {e}. Возможно, бот уже запущен в другом процессе.")
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)

