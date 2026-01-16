"""
Парсер сайта poweron.loe.lviv.ua через Selenium.
Парсит текстовый блок div.power-off__text с помощью regex.
"""
import logging
import os
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import undetected_chromedriver as uc
import utils

logger = logging.getLogger(__name__)


def ensure_debug_dir():
    """Создать директорию debug если её нет."""
    os.makedirs("debug", exist_ok=True)


def save_debug_artifacts(driver, html_content: str):
    """Сохранить артефакты для отладки."""
    ensure_debug_dir()
    try:
        # Сохранить HTML
        with open("debug/last.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        
        # Сохранить скриншот
        driver.save_screenshot("debug/last.png")
        logger.info("Артефакты сохранены в debug/")
    except Exception as e:
        logger.error(f"Ошибка при сохранении артефактов: {e}")


def extract_schedule_date(lines: List[str], timezone: str = "Europe/Kyiv") -> str:
    """
    Извлечь дату графика из текста.
    
    Ищет строку "Графік погодинних відключень на DD.MM.YYYY"
    Если не найдена - использует сегодняшнюю дату в указанной таймзоне.
    """
    date_pattern = re.compile(r"Графік погодинних відключень на\s+(\d{2}\.\d{2}\.\d{4})")
    
    for line in lines:
        match = date_pattern.search(line)
        if match:
            return match.group(1)
    
    # Fallback: сегодняшняя дата в указанной таймзоне
    tz = ZoneInfo(timezone)
    today = datetime.now(tz)
    schedule_date = today.strftime("%d.%m.%Y")
    logger.warning(f"Дата графика не найдена на странице, используется сегодняшняя: {schedule_date}")
    return schedule_date


def extract_group_off_intervals(line: str) -> List[Tuple[int, int]]:
    """
    Извлечь OFF интервалы из строки группы.
    
    Ищет паттерн "з HH:MM до HH:MM" и возвращает список кортежей (start_minutes, end_minutes).
    """
    # Regex для интервалов: "з HH:MM до HH:MM"
    interval_pattern = re.compile(r"з\s+(\d{2}:\d{2})\s+до\s+(\d{2}:\d{2})")
    
    intervals = []
    for match in interval_pattern.finditer(line):
        start_str = match.group(1)
        end_str = match.group(2)
        try:
            start_min = utils.time_to_minutes(start_str)
            end_min = utils.time_to_minutes(end_str)
            intervals.append((start_min, end_min))
        except Exception as e:
            logger.debug(f"Ошибка при парсинге интервала {start_str}-{end_str}: {e}")
            continue
    
    return intervals


def parse_schedule_text(lines: List[str], timezone: str = "Europe/Kyiv") -> Optional[Dict]:
    """
    Парсить график из текстовых строк.
    
    Args:
        lines: список строк из div.power-off__text p
        timezone: таймзона для fallback даты
    
    Returns:
        Dict с ключами:
            - schedule_date: str
            - groups: Dict[str, Dict] где ключ - код группы, значение - {
                'off': List[str],
                'on': List[str],
                'maybe': List[str]
            }
        None если парсинг не удался
    """
    # Извлекаем дату
    schedule_date = extract_schedule_date(lines, timezone)
    
    # Regex для определения строки группы: "Група X.Y."
    group_pattern = re.compile(r"^Група\s+(\d\.\d)\.")
    
    groups_data = {}
    valid_groups = utils.VALID_GROUPS
    
    # Парсим строки
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Проверяем, начинается ли строка с "Група"
        match = group_pattern.match(line)
        if not match:
            continue
        
        group_code = match.group(1)
        if group_code not in valid_groups:
            logger.warning(f"Найдена неизвестная группа: {group_code}")
            continue
        
        # Извлекаем OFF интервалы
        off_intervals_minutes = extract_group_off_intervals(line)
        
        if not off_intervals_minutes:
            logger.warning(f"Не найдено интервалов для группы {group_code}")
            # Не считаем это критической ошибкой, просто пропускаем группу
            continue
        
        # Нормализуем OFF интервалы (объединяем пересекающиеся)
        off_merged = utils.merge_intervals(off_intervals_minutes)
        
        # Вычисляем ON интервалы как дополнение
        on_intervals_minutes = utils.invert_intervals(off_merged)
        
        # Преобразуем в строки
        off_strings = utils.intervals_to_strings(off_merged)
        on_strings = utils.intervals_to_strings(on_intervals_minutes)
        
        groups_data[group_code] = {
            'off': off_strings,
            'on': on_strings,
            'maybe': []  # MAYBE отсутствует в текущем формате
        }
    
    # Валидация: должно быть найдено ровно 12 групп
    found_groups = set(groups_data.keys())
    expected_groups = valid_groups
    
    if len(found_groups) != 12:
        missing = expected_groups - found_groups
        logger.error(f"Недостаточно данных: найдено {len(found_groups)} групп с данными. Отсутствуют: {missing}")
        return None
    
    if found_groups != expected_groups:
        missing = expected_groups - found_groups
        logger.error(f"Найдены не все группы. Отсутствуют: {missing}")
        return None
    
    result = {
        'schedule_date': schedule_date,
        'groups': groups_data
    }
    
    logger.info(f"Успешно распарсено: дата {schedule_date}, найдено {len(found_groups)} групп")
    return result


def parse_schedule_snapshot(timezone: str = "Europe/Kyiv") -> Optional[Dict]:
    """
    Парсить график отключений с сайта.
    
    Args:
        timezone: таймзона для fallback даты
    
    Returns:
        Dict с ключами:
            - schedule_date: str
            - groups: Dict[str, Dict] где ключ - код группы, значение - {
                'off': List[str],
                'on': List[str],
                'maybe': List[str]
            }
        None если парсинг не удался
    """
    driver = None
    try:
        # Настройка драйвера
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--window-size=1365,768')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Поддержка CHROME_BINARY из переменной окружения
        chrome_binary = os.getenv('CHROME_BINARY')
        if chrome_binary:
            options.binary_location = chrome_binary
            logger.info(f"Используется Chrome из CHROME_BINARY: {chrome_binary}")
        
        driver = uc.Chrome(options=options)
        driver.set_window_size(1365, 768)
        
        logger.info("Открываю страницу poweron.loe.lviv.ua")
        driver.get("https://poweron.loe.lviv.ua/")
        
        # Ждем загрузки контента
        wait = WebDriverWait(driver, 30)
        
        # Ждем присутствия div.power-off__text
        try:
            container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.power-off__text")))
        except TimeoutException:
            logger.error("Таймаут при загрузке контейнера div.power-off__text")
            if driver:
                save_debug_artifacts(driver, driver.page_source)
            return None
        
        # Ждем минимум 5 <p> внутри контейнера
        try:
            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "div.power-off__text p")) >= 5)
        except TimeoutException:
            logger.error("Таймаут: не найдено минимум 5 элементов <p> в div.power-off__text")
            if driver:
                save_debug_artifacts(driver, driver.page_source)
            return None
        
        # Извлекаем все строки текста
        paragraphs = driver.find_elements(By.CSS_SELECTOR, "div.power-off__text p")
        lines = [p.text.strip() for p in paragraphs if p.text.strip()]
        
        if len(lines) < 5:
            logger.error(f"Недостаточно строк для парсинга: найдено {len(lines)}")
            if driver:
                save_debug_artifacts(driver, driver.page_source)
            return None
        
        html_content = driver.page_source
        
        # Парсим текст
        result = parse_schedule_text(lines, timezone)
        
        if not result:
            # Невалидный snapshot - сохраняем артефакты
            if driver:
                save_debug_artifacts(driver, html_content)
            return None
        
        return result
        
    except WebDriverException as e:
        logger.error(f"Ошибка WebDriver: {e}")
        if driver:
            try:
                save_debug_artifacts(driver, driver.page_source if 'page_source' in dir(driver) else "")
            except:
                pass
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при парсинге: {e}", exc_info=True)
        if driver:
            try:
                save_debug_artifacts(driver, driver.page_source if 'page_source' in dir(driver) else "")
            except:
                pass
        return None
    finally:
        # Гарантированный cleanup
        if driver is not None:
            try:
                driver.quit()
            except OSError as e:
                logger.warning(f"Ошибка при закрытии драйвера (OSError): {e}")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии драйвера: {e}")
