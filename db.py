"""
SQLite база данных для бота уведомлений о графике отключений.
"""
import sqlite3
import json
import logging
import os
from typing import Optional, Dict, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DB_FILE = "bot.db"


def get_connection():
    """Создать соединение с БД."""
    # В Docker bind-mount может создать директорию, если файла не было на хосте.
    # Тогда SQLite не сможет открыть "файл" и упадёт с unable to open database file.
    if os.path.isdir(DB_FILE):
        raise sqlite3.OperationalError(
            f"unable to open database file: '{DB_FILE}' is a directory. "
            f"Fix: remove directory and create file (e.g. `rm -rf bot.db && touch bot.db`)."
        )
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Инициализировать схему БД."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_user_id INTEGER PRIMARY KEY,
            tg_chat_id INTEGER NOT NULL,
            group_code TEXT NULL,
            is_subscribed INTEGER NOT NULL DEFAULT 1,
            last_sent_at TEXT NULL
        )
    """)
    
    # Таблица состояния групп
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS group_state (
            group_code TEXT PRIMARY KEY,
            schedule_date TEXT NOT NULL,
            hash TEXT NOT NULL,
            data_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")


def get_user(tg_user_id: int) -> Optional[Dict]:
    """Получить пользователя по tg_user_id."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM users WHERE tg_user_id = ?",
        (tg_user_id,)
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def create_or_update_user(tg_user_id: int, tg_chat_id: int, group_code: Optional[str] = None):
    """Создать или обновить пользователя."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (tg_user_id, tg_chat_id, group_code, is_subscribed)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(tg_user_id) DO UPDATE SET
            tg_chat_id = excluded.tg_chat_id,
            group_code = excluded.group_code
    """, (tg_user_id, tg_chat_id, group_code))
    conn.commit()
    conn.close()


def update_user_group(tg_user_id: int, group_code: str):
    """Обновить группу пользователя."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET group_code = ? WHERE tg_user_id = ?",
        (group_code, tg_user_id)
    )
    conn.commit()
    conn.close()


def set_subscription(tg_user_id: int, is_subscribed: bool):
    """Установить статус подписки."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET is_subscribed = ? WHERE tg_user_id = ?",
        (1 if is_subscribed else 0, tg_user_id)
    )
    conn.commit()
    conn.close()


def update_last_sent_at(tg_user_id: int):
    """Обновить время последней отправки."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute(
        "UPDATE users SET last_sent_at = ? WHERE tg_user_id = ?",
        (now, tg_user_id)
    )
    conn.commit()
    conn.close()


def can_send_message(tg_user_id: int, max_per_minute: int = 1) -> bool:
    """Проверить, можно ли отправить сообщение (антиспам)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT last_sent_at FROM users WHERE tg_user_id = ?",
        (tg_user_id,)
    )
    row = cursor.fetchone()
    conn.close()
    
    if not row or not row[0]:
        return True
    
    try:
        last_sent = datetime.fromisoformat(row[0])
        now = datetime.now(timezone.utc)
        diff_seconds = (now - last_sent).total_seconds()
        min_interval = 60.0 / max_per_minute
        return diff_seconds >= min_interval
    except Exception as e:
        logger.warning(f"Ошибка при проверке антиспама: {e}")
        return True


def get_subscribed_users_for_group(group_code: str) -> List[Dict]:
    """Получить всех подписанных пользователей для группы."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM users
        WHERE group_code = ? AND is_subscribed = 1
    """, (group_code,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_all_users() -> List[Dict]:
    """Получить всех пользователей (для broadcast)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_group_state(group_code: str) -> Optional[Dict]:
    """Получить последнее состояние группы."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM group_state WHERE group_code = ?",
        (group_code,)
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def save_group_state(group_code: str, schedule_date: str, hash_value: str, data_json: str):
    """Сохранить состояние группы."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO group_state (group_code, schedule_date, hash, data_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_code) DO UPDATE SET
            schedule_date = excluded.schedule_date,
            hash = excluded.hash,
            data_json = excluded.data_json,
            updated_at = excluded.updated_at
    """, (group_code, schedule_date, hash_value, data_json, now))
    conn.commit()
    conn.close()

