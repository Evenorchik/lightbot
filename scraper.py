"""
Парсер сайта poweron.loe.lviv.ua через Selenium.
Парсит текстовый блок div.power-off__text с помощью regex.
"""
import logging
import os
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import threading
import atexit
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import undetected_chromedriver as uc
import utils

logger = logging.getLogger(__name__)

_DRIVER_LOCK = threading.Lock()
_SHARED_DRIVER: Optional[uc.Chrome] = None


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


def _try_accept_consent_in_current_context(driver, timeout_seconds: int = 3) -> bool:
    """
    Попытаться нажать кнопку согласия в текущем контексте (default content или iframe).
    Возвращает True если клик выполнен.
    """
    wait_short = WebDriverWait(driver, timeout_seconds)
    candidates = [
        "//button[normalize-space()='Consent']",
        "//button[contains(normalize-space(), 'Consent')]",
        "//a[normalize-space()='Consent']",
        "//input[@type='button' and contains(@value,'Consent')]",
        # украинские варианты (на случай локализации)
        "//button[contains(normalize-space(), 'Прийня')]",
        "//button[contains(normalize-space(), 'Погодж')]",
        "//button[contains(normalize-space(), 'Згоден')]",
    ]
    for xp in candidates:
        try:
            el = wait_short.until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            return True
        except Exception:
            continue
    return False


def accept_consent_if_present(driver) -> bool:
    """
    Принять cookie/consent попап, если он появился.
    Часто CMP находится в iframe — пытаемся и там.
    """
    clicked = False

    # 1) основной документ
    try:
        clicked = _try_accept_consent_in_current_context(driver, timeout_seconds=3) or clicked
    except Exception:
        pass

    # 2) iframes
    try:
        for fr in driver.find_elements(By.CSS_SELECTOR, "iframe"):
            try:
                driver.switch_to.frame(fr)
                if _try_accept_consent_in_current_context(driver, timeout_seconds=2):
                    clicked = True
                    break
            except Exception:
                continue
            finally:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
    except Exception:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

    if clicked:
        logger.info("Consent popup прийнято (кнопка Accept/Consent натиснута)")
        # ждем, пока кнопка исчезнет (без sleep)
        try:
            WebDriverWait(driver, 10).until_not(
                EC.presence_of_element_located((By.XPATH, "//button[contains(normalize-space(), 'Consent')]"))
            )
        except Exception:
            pass

    return clicked


def _dispose_shared_driver(reason: str) -> None:
    global _SHARED_DRIVER
    with _DRIVER_LOCK:
        d = _SHARED_DRIVER
        _SHARED_DRIVER = None
    if d is not None:
        try:
            d.quit()
        except OSError as e:
            logger.warning(f"Ошибка при закрытии драйвера (OSError) [{reason}]: {e}")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии драйвера [{reason}]: {e}")


def _get_or_create_shared_driver(options: uc.ChromeOptions) -> uc.Chrome:
    """
    Создать undetected-chromedriver один раз и переиспользовать.
    Это уменьшает шанс утечек/Errno 24 (Too many open files) на VPS из-за повторных
    операций patcher.auto() внутри undetected-chromedriver.
    """
    global _SHARED_DRIVER
    with _DRIVER_LOCK:
        if _SHARED_DRIVER is not None:
            return _SHARED_DRIVER
        d = uc.Chrome(options=options)
        d.set_window_size(1365, 768)
        _SHARED_DRIVER = d
        return d


@atexit.register
def _cleanup_driver_on_exit():
    _dispose_shared_driver("atexit")


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


def split_lines_into_sections(lines: List[str]) -> Dict[str, List[str]]:
    """
    Разбить lines на секции по заголовку:
    "Графік погодинних відключень на DD.MM.YYYY"
    Возвращает dict: {schedule_date_str: section_lines}
    """
    header_re = re.compile(r"Графік погодинних відключень на\s+(\d{2}\.\d{2}\.\d{4})")
    sections: Dict[str, List[str]] = {}
    current_date: Optional[str] = None

    for line in lines:
        t = line.strip()
        if not t:
            continue
        m = header_re.search(t)
        if m:
            current_date = m.group(1)
            sections.setdefault(current_date, [])
            # сохраняем заголовок тоже (не обязательно, но полезно для extract_schedule_date fallback)
            sections[current_date].append(t)
            continue
        if current_date is not None:
            sections[current_date].append(t)

    return sections


def parse_groups_from_section_lines(section_lines: List[str]) -> Optional[Dict[str, Dict]]:
    """
    Парсит только строки групп из section_lines, возвращает groups_data (12 групп) либо None.
    """
    group_pattern = re.compile(r"^Група\s+(\d\.\d)\.")
    groups_data: Dict[str, Dict] = {}
    valid_groups = utils.VALID_GROUPS

    for line in section_lines:
        line = line.strip()
        if not line:
            continue
        match = group_pattern.match(line)
        if not match:
            continue

        group_code = match.group(1)
        if group_code not in valid_groups:
            continue

        off_intervals_minutes = extract_group_off_intervals(line)
        if not off_intervals_minutes:
            logger.warning(f"Не найдено интервалов для группы {group_code}")
            continue

        off_merged = utils.merge_intervals(off_intervals_minutes)
        on_intervals_minutes = utils.invert_intervals(off_merged)

        groups_data[group_code] = {
            "off": utils.intervals_to_strings(off_merged),
            "on": utils.intervals_to_strings(on_intervals_minutes),
            "maybe": [],
        }

    if len(groups_data.keys()) != 12:
        missing = valid_groups - set(groups_data.keys())
        logger.error(f"Недостаточно данных: найдено {len(groups_data.keys())} групп с данными. Отсутствуют: {missing}")
        return None

    return groups_data


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

    Возвращает:
        {
          "today": {"schedule_date": "...", "groups": {...}},
          "tomorrow": {"schedule_date": "...", "groups": {...}} | None
        }
    """
    # Даты "сегодня/завтра" в указанной таймзоне
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("Europe/Kyiv")
    today_dt = datetime.now(tz).date()
    today_str = today_dt.strftime("%d.%m.%Y")
    tomorrow_dt = today_dt + timedelta(days=1)
    tomorrow_str = tomorrow_dt.strftime("%d.%m.%Y")

    sections = split_lines_into_sections(lines)
    if not sections:
        # fallback: один блок как раньше
        schedule_date = extract_schedule_date(lines, timezone)
        groups = parse_groups_from_section_lines(lines)
        if not groups:
            return None
        return {"today": {"schedule_date": schedule_date, "groups": groups}, "tomorrow": None}

    today_section_lines = sections.get(today_str)
    tomorrow_section_lines = sections.get(tomorrow_str)

    # fallback selection если exact match не найден
    if today_section_lines is None:
        # берем минимальную дату из секций как "сегодня"
        try:
            parsed_dates = sorted(
                (utils.parse_date_ddmmyyyy(d), d) for d in sections.keys()
            )
            if parsed_dates:
                today_section_lines = sections[parsed_dates[0][1]]
                today_str = parsed_dates[0][1]
        except Exception:
            pass

    if tomorrow_section_lines is None:
        # берем максимальную дату из секций (если секций > 1)
        try:
            parsed_dates = sorted(
                (utils.parse_date_ddmmyyyy(d), d) for d in sections.keys()
            )
            if len(parsed_dates) >= 2:
                tomorrow_section_lines = sections[parsed_dates[-1][1]]
                tomorrow_str = parsed_dates[-1][1]
        except Exception:
            pass

    if not today_section_lines:
        logger.error("Не удалось выбрать секцию на сегодня")
        return None

    today_groups = parse_groups_from_section_lines(today_section_lines)
    if not today_groups:
        return None

    tomorrow_snapshot = None
    if tomorrow_section_lines and tomorrow_str != today_str:
        tomorrow_groups = parse_groups_from_section_lines(tomorrow_section_lines)
        if tomorrow_groups:
            tomorrow_snapshot = {"schedule_date": tomorrow_str, "groups": tomorrow_groups}

    logger.info(
        f"Успешно распарсено: сегодня {today_str} (12 групп), завтра {tomorrow_str if tomorrow_snapshot else 'нет'}"
    )
    return {"today": {"schedule_date": today_str, "groups": today_groups}, "tomorrow": tomorrow_snapshot}


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
        
        driver = _get_or_create_shared_driver(options)
        
        logger.info("Открываю страницу poweron.loe.lviv.ua")
        driver.get("https://poweron.loe.lviv.ua/")
        
        # Ждем загрузки контента
        wait = WebDriverWait(driver, 30)

        # На VPS часто показывается consent-попап (cookie). Он блокирует контент — принимаем.
        try:
            accept_consent_if_present(driver)
        except Exception as e:
            logger.warning(f"Не удалось обработать consent popup: {e}")
        
        # Ждем присутствия div.power-off__text
        try:
            container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.power-off__text")))
        except TimeoutException:
            logger.error("Таймаут при загрузке контейнера div.power-off__text")
            if driver:
                save_debug_artifacts(driver, driver.page_source)
            return None
        
        # Ждем минимум 5 <p> внутри контейнера и чтобы в них был непустой текст
        try:
            wait.until(
                lambda d: sum(
                    1
                    for p in d.find_elements(By.CSS_SELECTOR, "div.power-off__text p")
                    if (p.get_attribute("innerText") or "").strip()
                )
                >= 5
            )
        except TimeoutException:
            logger.error("Таймаут: не найдено минимум 5 непустых <p> в div.power-off__text")
            if driver:
                save_debug_artifacts(driver, driver.page_source)
            return None
        
        # Извлекаем все строки текста (innerText устойчивее, чем .text в headless/при скрытии)
        paragraphs = driver.find_elements(By.CSS_SELECTOR, "div.power-off__text p")
        lines = []
        for p in paragraphs:
            t = (p.get_attribute("innerText") or "").strip()
            if t:
                lines.append(t)
        
        if len(lines) < 5:
            try:
                title = driver.title
                url = driver.current_url
            except Exception:
                title = ""
                url = ""
            logger.error(
                f"Недостаточно строк для парсинга: найдено {len(lines)} (всего <p>: {len(paragraphs)}). "
                f"title='{title}' url='{url}'"
            )
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
        _dispose_shared_driver("WebDriverException")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при парсинге: {e}", exc_info=True)
        if driver:
            try:
                save_debug_artifacts(driver, driver.page_source if 'page_source' in dir(driver) else "")
            except:
                pass
        _dispose_shared_driver("Exception")
        return None
    finally:
        # Драйвер переиспользуется (см. _get_or_create_shared_driver).
        # Закрываем его только при ошибках через _dispose_shared_driver() или при завершении процесса (atexit).
        pass
