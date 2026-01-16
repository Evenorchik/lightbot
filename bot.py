"""
Telegram бот на aiogram v3 для уведомлений о графике отключений.
"""
import logging
import json
import os
from aiogram import Bot, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import db
import utils
import render

logger = logging.getLogger(__name__)

router = Router()


class GroupSelection(StatesGroup):
    waiting_for_group = State()


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Створити нижнє меню з двома кнопками."""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Обрати групу"), KeyboardButton(text="Показати графік")],
            [KeyboardButton(text="Що робити?")],
        ],
        resize_keyboard=True,
        is_persistent=True,
        one_time_keyboard=False
    )
    return keyboard


def create_group_keyboard() -> InlineKeyboardMarkup:
    """Создать inline-клавиатуру с кнопками выбора группы."""
    buttons = []
    groups = ["1.1", "1.2", "2.1", "2.2", "3.1", "3.2", 
              "4.1", "4.2", "5.1", "5.2", "6.1", "6.2"]
    
    # Размещаем по 2 кнопки в ряд
    for i in range(0, len(groups), 2):
        row = []
        row.append(InlineKeyboardButton(text=groups[i], callback_data=f"set_group:{groups[i]}"))
        if i + 1 < len(groups):
            row.append(InlineKeyboardButton(text=groups[i+1], callback_data=f"set_group:{groups[i+1]}"))
        buttons.append(row)
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def send_schedule_for_group(bot: Bot, chat_id: int, group_code: str, timezone: str = "Europe/Kyiv") -> bool:
    """
    Відправити поточний графік для групи (текст + картинка).
    
    Args:
        bot: екземпляр бота
        chat_id: ID чату
        group_code: код групи
        timezone: таймзона для маркера "зараз"
    
    Returns:
        True якщо графік відправлено, False якщо даних немає
    """
    group_state = db.get_group_state(group_code)
    
    if not group_state:
        await bot.send_message(
            chat_id,
            "Дані ще не завантажено, спробуйте через хвилину.",
            reply_markup=main_menu_keyboard()
        )
        return False
    
    image_path = None
    try:
        data = json.loads(group_state['data_json'])
        off = data.get('off', [])
        on = data.get('on', [])
        maybe = data.get('maybe', [])
        
        # Отправляем текстовый график
        message_text = utils.format_schedule_message(
            group_state['schedule_date'],
            group_code,
            off,
            on,
            maybe
        )
        await bot.send_message(
            chat_id,
            message_text,
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
        
        # Генерируем и отправляем картинку
        try:
            # Нормализация таймзоны: Europe/Uzhgorod -> Europe/Kyiv
            normalized_tz = "Europe/Kyiv" if timezone == "Europe/Uzhgorod" else timezone
            now_dt = utils.get_now_in_tz(normalized_tz)
            image_path = render.render_schedule_image(
                schedule_date=group_state['schedule_date'],
                group_code=group_code,
                on_intervals=on,
                off_intervals=off,
                now_dt=now_dt,
                tz_name=normalized_tz
            )
            
            caption = f"Група {group_code} • {group_state['schedule_date']}"
            await bot.send_photo(
                chat_id,
                FSInputFile(image_path),
                caption=caption,
                reply_markup=main_menu_keyboard()
            )
        except Exception as e:
            logger.error(f"render_image_failed: {e}", exc_info=True)
            # Продолжаем без картинки
        
        return True
    except Exception as e:
        logger.error(f"Помилка при відправці графіку: {e}")
        await bot.send_message(
            chat_id,
            "Помилка при отриманні графіку.",
            reply_markup=main_menu_keyboard()
        )
        return False
    finally:
        # Удаляем временный файл
        if image_path and os.path.exists(image_path):
            try:
                os.remove(image_path)
            except Exception as e:
                logger.warning(f"Не вдалося видалити тимчасовий файл {image_path}: {e}")


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start."""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Создаем или обновляем пользователя
    db.create_or_update_user(user_id, chat_id)
    user = db.get_user(user_id)
    
    if user and user.get('group_code'):
        text = f"Поточна група: {user['group_code']}"
    else:
        text = (
            "Привіт! 👋\n\n"
            "Цей бот надсилає сповіщення про зміни графіку відключень електроенергії у Львові!.\n\n"
            "Оберіть групу через кнопку «Обрати групу»."
        )
    
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(F.text == "Обрати групу")
async def handle_choose_group(message: Message):
    """Обробник кнопки 'Обрати групу'."""
    await message.answer(
        "Оберіть вашу групу:",
        reply_markup=create_group_keyboard()
    )


@router.message(F.text == "Показати графік")
async def handle_show_schedule(message: Message):
    """Обробник кнопки 'Показати графік'."""
    user_id = message.from_user.id
    user = db.get_user(user_id)
    
    if not user or not user.get('group_code'):
        await message.answer(
            "Спочатку оберіть групу (кнопка «Обрати групу»).",
            reply_markup=main_menu_keyboard()
        )
        return
    
    group_code = user['group_code']
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
    timezone = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env
    await send_schedule_for_group(message.bot, message.chat.id, group_code, timezone)


@router.message(F.text == "Що робити?")
async def handle_what_to_do(message: Message):
    text = (
        "🛑 Прямо зараз\n\n"
        "Нічого не вирішуй сьогодні. Втома ≠ правда.\n\n"
        "Випий води. Серйозно. Часто це 30% проблем.\n\n"
        "Відʼєбись від себе. Ти не зобовʼязаний бути продуктивним.\n\n"
        "🧠 Для голови\n\n"
        "Запиши все, що давить. Не красиво. Не логічно. Просто злити.\n\n"
        "Скороти світ. Один день. Одна задача. Один крок.\n\n"
        "Не роби великих висновків уночі. Мозок — мудак після 22:00.\n\n"
        "🧍‍♂️ Для тіла\n\n"
        "Ляж. Навіть якщо не спиш.\n\n"
        "Пройдися 10–15 хвилин. Без музики. Без цілі.\n\n"
        "Поїж нормально. Не “що було”, а щось тепле.\n\n"
        "🔕 Межі\n\n"
        "Тимчасово забий. На людей, чати, новини, очікування.\n\n"
        "Скажи “мені зараз важко”. Одній людині. Цього досить.\n\n"
        "Не пояснюй свій стан. Ти не адвокат.\n\n"
        "🔁 Якщо це не перший раз\n\n"
        "Ти не зламався — ти перевантажений.\n\n"
        "Перепочинок — це не нагорода, а умова.\n\n"
        "Можна просити допомогу і не мати плану.\n\n"
        "Все погане рано чи пізно закінчується й життя продовжується."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(Command("group"))
async def cmd_group(message: Message, state: FSMContext):
    """Обработчик команды /group."""
    text = (
        "Оберіть вашу групу:\n"
        "Як дізнатись групу - https://poweron.loe.lviv.ua/shedule-off\n"
    )
    await message.answer(
        text,
        reply_markup=main_menu_keyboard()
    )
    await message.answer(
        "Оберіть вашу групу:\n"
        "Як дізнатись групу - https://poweron.loe.lviv.ua/shedule-off\n",
        reply_markup=create_group_keyboard()
    )


@router.message(Command("status"))
async def cmd_status(message: Message):
    """Обработчик команды /status."""
    user_id = message.from_user.id
    user = db.get_user(user_id)
    
    if not user or not user.get('group_code'):
        await message.answer(
            "Ви ще не обрали групу. Використовуйте кнопку «Обрати групу» для вибору.",
            reply_markup=main_menu_keyboard()
        )
        return
    
    group_code = user['group_code']
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
    timezone = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env
    await send_schedule_for_group(message.bot, message.chat.id, group_code, timezone)


@router.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    """Обработчик команды /unsubscribe."""
    user_id = message.from_user.id
    db.set_subscription(user_id, False)
    await message.answer(
        "Підписку відключено. Ви більше не будете отримувати сповіщення.\n\n"
        "Використовуйте кнопку «Обрати групу» для повторної активації.",
        reply_markup=main_menu_keyboard()
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help."""
    text = (
        "📋 Список команд:\n\n"
        "/start - Початок роботи та вибір групи\n"
        "/group - Змінити групу\n"
        "/status - Показати поточний графік для вашої групи\n"
        "/unsubscribe - Відключити підписку на сповіщення\n"
        "/help - Показати цю справку\n\n"
        "Бот автоматично надсилає сповіщення при зміні графіку для вашої групи.\n\n"
        "Також використовуйте нижнє меню для швидкого доступу."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.callback_query(F.data.startswith("set_group:"))
async def process_group_selection(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора группы через inline-кнопку."""
    group_code = callback.data.replace("set_group:", "")
    
    if not utils.validate_group(group_code):
        await callback.answer("Невірний код групи!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    
    # Сохраняем группу и активируем подписку
    db.create_or_update_user(user_id, chat_id, group_code)
    db.set_subscription(user_id, True)
    
    await callback.answer(f"Група {group_code} встановлена!")
    
    # Отправляем подтверждение
    await callback.message.edit_text(
        f"Група встановлена: {group_code}",
        reply_markup=None
    )
    
    # Сразу отправляем график
    import os
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
    timezone = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env
    await send_schedule_for_group(callback.bot, chat_id, group_code, timezone)
    
    await state.clear()


@router.callback_query(F.data.startswith("group_"))
async def process_group_selection_old(callback: CallbackQuery, state: FSMContext):
    """Обработчик старого формата callback (для обратной совместимости)."""
    group_code = callback.data.replace("group_", "")
    
    if not utils.validate_group(group_code):
        await callback.answer("Невірний код групи!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    
    db.create_or_update_user(user_id, chat_id, group_code)
    db.set_subscription(user_id, True)
    
    await callback.answer(f"Група {group_code} обрана!")
    await callback.message.edit_text(
        f"✅ Група {group_code} успішно обрана!\n\n"
        f"Ви будете отримувати сповіщення про зміни графіку для цієї групи."
    )
    
    # Отправляем график
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
    timezone = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env
    await send_schedule_for_group(callback.bot, chat_id, group_code, timezone)
    
    await state.clear()


@router.message(GroupSelection.waiting_for_group)
async def process_group_text(message: Message, state: FSMContext):
    """Обработчик текстового ввода группы."""
    group_code = message.text.strip()
    
    if not utils.validate_group(group_code):
        await message.answer(
            "Невірний код групи! Будь ласка, оберіть групу з кнопок або введіть "
            "правильний код (наприклад, 1.1, 2.2 тощо).",
            reply_markup=main_menu_keyboard()
        )
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    db.create_or_update_user(user_id, chat_id, group_code)
    db.set_subscription(user_id, True)
    
    await message.answer(
        f"✅ Група {group_code} успішно обрана!",
        reply_markup=main_menu_keyboard()
    )
    
    # Отправляем график
    import os
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    tz_env = os.getenv('TIMEZONE', 'Europe/Kyiv')
    timezone = "Europe/Kyiv" if tz_env == "Europe/Uzhgorod" else tz_env
    await send_schedule_for_group(message.bot, chat_id, group_code, timezone)
    
    await state.clear()


async def send_notification(bot: Bot, chat_id: int, message_text: str, user_id: int, max_per_minute: int = 1) -> bool:
    """Отправить уведомление пользователю с проверкой антиспама."""
    if not db.can_send_message(user_id, max_per_minute):
        logger.info(f"Пропуск сообщения для пользователя {user_id} (антиспам)")
        return False
    
    try:
        await bot.send_message(
            chat_id,
            message_text,
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
        db.update_last_sent_at(user_id)
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")
        return False
