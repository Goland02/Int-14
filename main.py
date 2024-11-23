import sqlite3
import sys
import urllib.request
import urllib.parse
import re
import logging
from typing import Set
from urllib.error import URLError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Функция создания sqlite базы данных
def create_database(db_name: str) -> None:
    logging.info("Создание базы данных %s", db_name)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY, url TEXT UNIQUE)")
    conn.commit()
    conn.close()

# Функция сохранения ссылки в БД
def store_link(db_name: str, url: str) -> None:
    url = urllib.parse.unquote(url)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO links (url) VALUES (?)", (url,))
        logging.debug("Ссылка сохранена: %s", url)
    except Exception as e:
        logging.error("Непредвиденная ошибка при сохранении ссылки %s: %s", url, e)
    conn.commit()
    conn.close()

# Функция для извлечения языка из ссылки, которую мы даем программе как изначальную
def extract_lang(url: str) -> str:
    pattern = re.compile(r'https://(.*?)\.wikipedia')
    lang = pattern.search(url)
    if lang:
        return lang.group(1)
    return ""

# Функция для извлечения ссылок из html кода во множество
def extract_links(html: str, language: str) -> Set[str]:
    logging.debug("Извлечение ссылок из HTML-контента")
    html = urllib.parse.unquote(html)
    pattern = re.compile(
        r'href=\"/wiki/(?!Category:|File:|Template:|Special:|User:|Wikipedia:|Image:|MediaWiki:|'
        r'Help:|Portal:|Draft:|Module:|TimedText:|MOS:|Talk:|Категория:|Шаблон:|Файл:|Служебная:|'
        r'Википедия:|Участник:|Справка:|Портал:|Обсуждение:)([^"#]*)'
    )
    matches = pattern.findall(html)
    base_url = f"https://{language}.wikipedia.org/wiki/"
    return {base_url + urllib.parse.quote(match) for match in matches}

# Функция для извлечения html кода по URL адресу
def fetch_html(url: str) -> str:
    for attempt in range(3):
        try:
            logging.debug("Получение HTML по URL: %s", url)
            with urllib.request.urlopen(url, timeout=7) as response:
                return response.read().decode("utf-8")
        except URLError as e:
            logging.error("Ошибка при загрузке %s: %s", url, e)
            if attempt == 2:
                logging.error("Все попытки исчерпаны. Не удалось загрузить %s", url)
                return ""  # Возвращаем пустую строку при неудаче

# Основная функция, в которой происходит работа программы
def crawl_links(db_name: str, start_url: str, depth: int) -> None:
    create_database(db_name)
    current_depth: int = 0
    to_crawl = {start_url}
    language = extract_lang(start_url)
    crawled = set()

# Сохранение начальной ссылки первой в БД
    for link in to_crawl:
        store_link(db_name, link)

    while current_depth < depth and to_crawl:
        logging.info("Текущая глубина: %d, осталось обойти: %d ссылок", current_depth, len(to_crawl))
        next_to_crawl = set()

        for url in to_crawl:
            if url not in crawled:
                try:
                    html = fetch_html(url)
                    if html:  # Проверяем, что HTML не пуст
                        links = extract_links(html, language)
                        for link in links:
                            store_link(db_name, link)
                        next_to_crawl.update(links)
                except Exception as e:
                    logging.error("Ошибка при обработке %s: %s", url, e)
                crawled.add(url)
        to_crawl = next_to_crawl
        current_depth += 1


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Использование: python main.py <Wikipedia URL> <Database Name>")
        sys.exit(1)

    start_url = sys.argv[1]
    db_name = sys.argv[2]
    crawl_links(db_name, start_url, depth=6)