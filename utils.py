"""
Утилиты: хеширование, diff, форматирование сообщений, работа со временем.
"""
import hashlib
import json
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timezone as tz_utc
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

VALID_GROUPS = {"1.1", "1.2", "2.1", "2.2", "3.1", "3.2", 
                "4.1", "4.2", "5.1", "5.2", "6.1", "6.2"}


def validate_group(group_code: str) -> bool:
    """Проверить валидность кода группы."""
    return group_code in VALID_GROUPS


def time_to_minutes(time_str: str) -> int:
    """Преобразовать HH:MM в минуты с начала дня (0-1439)."""
    h, m = map(int, time_str.split(':'))
    if h == 24 and m == 0:
        return 1440
    return h * 60 + m


def minutes_to_time(minutes: int) -> str:
    """Преобразовать минуты обратно в HH:MM. 1440 -> "24:00"."""
    if minutes >= 1440:
        return "24:00"
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def parse_interval(interval_str: str) -> Tuple[int, int]:
    """
    Парсить интервал "HH:MM–HH:MM" в кортеж (start_minutes, end_minutes).
    
    Args:
        interval_str: строка вида "HH:MM–HH:MM" или "HH:MM-HH:MM"
    
    Returns:
        (start_minutes, end_minutes) где значения 0..1440
    """
    interval_str = interval_str.strip()
    sep = '–' if '–' in interval_str else '-'
    parts = interval_str.split(sep, 1)
    
    if len(parts) != 2:
        raise ValueError(f"Неверный формат интервала: {interval_str}")
    
    start_str = parts[0].strip()
    end_str = parts[1].strip()
    
    start_min = time_to_minutes(start_str)
    end_min = time_to_minutes(end_str)
    
    return (start_min, end_min)


def parse_date_ddmmyyyy(date_str: str) -> date:
    """
    Парсить дату из формата DD.MM.YYYY.
    
    Args:
        date_str: строка вида "DD.MM.YYYY"
    
    Returns:
        объект date
    """
    parts = date_str.split('.')
    if len(parts) != 3:
        raise ValueError(f"Неверный формат даты: {date_str}")
    
    day, month, year = map(int, parts)
    return date(year, month, day)


def get_now_in_tz(tz_name: str) -> datetime:
    """
    Получить текущее время в указанной таймзоне.
    
    Args:
        tz_name: название таймзоны (например, "Europe/Uzhgorod" или "Europe/Kyiv")
    
    Returns:
        datetime с timezone
    
    Примечание:
        - Europe/Uzhgorod автоматически заменяется на Europe/Kyiv
        - При ошибке ZoneInfo используется fallback на Europe/Kyiv
        - Если и это не работает - используется UTC
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Нормализация: Europe/Uzhgorod -> Europe/Kyiv
    if tz_name == "Europe/Uzhgorod":
        tz_name = "Europe/Kyiv"
    
    # Пытаемся создать ZoneInfo
    try:
        tz = ZoneInfo(tz_name)
        return datetime.now(tz)
    except ZoneInfoNotFoundError:
        logger.warning(f"Таймзона {tz_name} не найдена, используется Europe/Kyiv")
        try:
            tz = ZoneInfo("Europe/Kyiv")
            return datetime.now(tz)
        except (ZoneInfoNotFoundError, Exception) as e:
            logger.warning(f"Не удалось использовать Europe/Kyiv, используется UTC: {e}")
            return datetime.now(tz_utc.utc)


def merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """
    Объединить пересекающиеся и стыкующиеся интервалы.
    
    Args:
        intervals: список кортежей (start_minutes, end_minutes)
    
    Returns:
        отсортированный список объединенных интервалов
    """
    if not intervals:
        return []
    
    # Сортируем по началу
    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    
    merged = []
    for start, end in sorted_intervals:
        if merged and merged[-1][1] >= start:
            # Пересекаются или соприкасаются - склеиваем
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    
    return merged


def invert_intervals(off_intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """
    Вычислить ON интервалы как дополнение к OFF на [0, 1440).
    
    Args:
        off_intervals: список OFF интервалов в минутах
    
    Returns:
        список ON интервалов в минутах
    """
    if not off_intervals:
        # Если OFF пустой, ON = весь день
        return [(0, 1440)]
    
    # Объединяем OFF интервалы
    merged_off = merge_intervals(off_intervals)
    
    # Вычисляем дополнение
    on_intervals = []
    current = 0
    
    for off_start, off_end in merged_off:
        if current < off_start:
            # Есть промежуток до OFF
            on_intervals.append((current, off_start))
        current = max(current, off_end)
    
    # Если остался промежуток до конца суток
    if current < 1440:
        on_intervals.append((current, 1440))
    
    return on_intervals


def intervals_to_strings(intervals: List[Tuple[int, int]]) -> List[str]:
    """Преобразовать интервалы из минут в строки "HH:MM–HH:MM"."""
    result = []
    for start, end in intervals:
        start_str = minutes_to_time(start)
        end_str = minutes_to_time(end)
        result.append(f"{start_str}–{end_str}")
    return result


def normalize_intervals(intervals: List[str]) -> List[str]:
    """Нормализовать и отсортировать интервалы времени."""
    if not intervals:
        return []
    
    # Парсим интервалы в минуты
    parsed = []
    for interval in intervals:
        interval = interval.strip()
        if not interval:
            continue
            
        if '–' in interval or '-' in interval:
            # Это интервал вида "HH:MM–HH:MM"
            sep = '–' if '–' in interval else '-'
            parts = interval.split(sep, 1)
            if len(parts) == 2:
                start_str, end_str = parts[0].strip(), parts[1].strip()
                try:
                    start_min = time_to_minutes(start_str)
                    end_min = time_to_minutes(end_str)
                    parsed.append((start_min, end_min))
                except:
                    continue
    
    if not parsed:
        return []
    
    # Объединяем и преобразуем обратно
    merged = merge_intervals(parsed)
    return intervals_to_strings(merged)


def compute_group_hash(schedule_date: str, off_intervals: List[str], 
                      on_intervals: List[str], maybe_intervals: List[str]) -> str:
    """Вычислить SHA256 хеш для группы."""
    off_sorted = normalize_intervals(off_intervals)
    on_sorted = normalize_intervals(on_intervals)
    maybe_sorted = normalize_intervals(maybe_intervals)
    
    canonical = f"{schedule_date}|OFF:{','.join(off_sorted)};ON:{','.join(on_sorted)};MAYBE:{','.join(maybe_sorted)}"
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def compute_diff(old_intervals: List[str], new_intervals: List[str]) -> Tuple[List[str], List[str]]:
    """Вычислить разницу между старым и новым списком интервалов.
    
    Returns:
        (added, removed) - списки добавленных и удаленных интервалов
    """
    old_set = set(normalize_intervals(old_intervals))
    new_set = set(normalize_intervals(new_intervals))
    
    added = sorted(new_set - old_set)
    removed = sorted(old_set - new_set)
    
    return added, removed


def format_schedule_message(schedule_date: str, group_code: str,
                            off_intervals: List[str], on_intervals: List[str],
                            maybe_intervals: Optional[List[str]] = None,
                            old_data: Optional[Dict] = None) -> str:
    """Форматировать сообщение с графиком."""
    maybe_intervals = maybe_intervals or []
    
    # Нормализуем интервалы
    off_norm = normalize_intervals(off_intervals)
    on_norm = normalize_intervals(on_intervals)
    maybe_norm = normalize_intervals(maybe_intervals)
    
    lines = [
        f"Графік ({schedule_date}), група {group_code}",
        "",
        f"OFF: {', '.join(off_norm) if off_norm else 'немає'}",
        f"ON : {', '.join(on_norm) if on_norm else 'немає'}"
    ]
    
    if maybe_norm:
        lines.append(f"MAYBE: {', '.join(maybe_norm)}")
    
    # Вычисляем изменения, если есть старая версия
    if old_data:
        try:
            old_json = json.loads(old_data.get('data_json', '{}'))
            old_off = old_json.get('off', [])
            old_on = old_json.get('on', [])
            old_maybe = old_json.get('maybe', [])
            
            off_added, off_removed = compute_diff(old_off, off_intervals)
            on_added, on_removed = compute_diff(old_on, on_intervals)
            maybe_added, maybe_removed = compute_diff(old_maybe, maybe_intervals)
            
            changes = []
            if off_added:
                changes.extend([f"+OFF {iv}" for iv in off_added])
            if off_removed:
                changes.extend([f"-OFF {iv}" for iv in off_removed])
            if on_added:
                changes.extend([f"+ON {iv}" for iv in on_added])
            if on_removed:
                changes.extend([f"-ON {iv}" for iv in on_removed])
            if maybe_added:
                changes.extend([f"+MAYBE {iv}" for iv in maybe_added])
            if maybe_removed:
                changes.extend([f"-MAYBE {iv}" for iv in maybe_removed])
            
            if changes:
                lines.append("")
                lines.append("Зміни:")
                lines.extend(changes)
        except Exception as e:
            # Если не удалось распарсить старые данные - просто не показываем изменения
            pass
    
    message = "\n".join(lines)
    return f"```\n{message}\n```"
