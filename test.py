import pytest
import sqlite3
import urllib.error
import os
from unittest.mock import patch, MagicMock
from main import create_database, store_link, extract_lang, extract_links, fetch_html, crawl_links


@pytest.fixture
def html_sample():
    return """
    <html>
        <body>
            <a href="/wiki/Chipmunk">Chipmunk</a>
            <a href="/wiki/Category:Rodents">Category:Rodents</a>
            <a href="/wiki/Squirrel#Details">Squirrel</a>
            <a href="/wiki/%D0%92%D0%B8%D0%BA%D0%B8%D0%BF%D0%B5%D0%B4%D0%B8%D1%8F:Example">Википедия:Example</a>
        </body>
    </html>
    """

# Тест для функции создания базы данных
def test_create_database():
    db_name = "test_temp.db"  # Временный файл базы данных
    try:
        create_database(db_name)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='links'")
        assert cursor.fetchone() is not None, "Таблица links должна быть создана"
        conn.close()
    finally:
        if os.path.exists(db_name):
            os.remove(db_name)  # Удаляем временный файл после теста

# Тест для сохранения ссылки
def test_store_link():
    db_name = "test_temp.db"
    try:
        create_database(db_name)
        store_link(db_name, "https://en.wikipedia.org/wiki/Chipmunk")
        conn = sqlite3.connect(db_name)  # Проверяем содержимое базы
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM links WHERE url = 'https://en.wikipedia.org/wiki/Chipmunk'")
        assert cursor.fetchone() is not None, "Ссылка должна быть сохранена в таблице links"
        conn.close()
    finally:
        if os.path.exists(db_name):
            os.remove(db_name)

# Тест для извлечения языка из URL
def test_extract_lang():
    assert extract_lang("https://en.wikipedia.org/wiki/Chipmunk") == "en", "Должен быть извлечён язык en"
    assert extract_lang("https://ru.wikipedia.org/wiki/Белка") == "ru", "Должен быть извлечён язык ru"
    assert extract_lang("https://fr.wikipedia.org/wiki/Ecureuil") == "fr", "Должен быть извлечён язык fr"
    assert extract_lang("https://wikipedia.org/wiki/Main_Page") == "", "Должен быть возвращён пустой язык"

# Тест для извлечения ссылок
def test_extract_links(html_sample):
    links = extract_links(html_sample, "en")
    assert "https://en.wikipedia.org/wiki/Chipmunk" in links, "Ссылка на Chipmunk должна быть извлечена"
    assert "https://en.wikipedia.org/wiki/Category:Rodents" not in links, "Ссылки с префиксом Category не должны извлекаться"
    assert "https://en.wikipedia.org/wiki/Squirrel" in links, "Ссылка на Squirrel должна быть извлечена без якоря"
    assert "https://en.wikipedia.org/wiki/%D0%92%D0%B8%D0%BA%D0%B8%D0%BF%D0%B5%D0%B4%D0%B8%D1%8F:Example" not in links, "Ссылки с префиксом Википедия не должны извлекаться"


# Тест для функции fetch_html
@patch("urllib.request.urlopen")
def test_fetch_html(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = "<html><body>Test HTML</body></html>".encode('utf-8')
    mock_response.__enter__.return_value = mock_response  # Для использования with
    mock_urlopen.return_value = mock_response

    html = fetch_html("https://example.com")
    assert "Test HTML" in html, "Должен быть получен HTML-контент"

@patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError("https://example.com", 404, "Not Found", {}, None))
def test_fetch_html_error():
    html = fetch_html("https://example.com")
    assert html == "", "Должна быть возвращена пустая строка при ошибке загрузки"

# Интеграционный тест для crawl_links
@patch("main.fetch_html")
def test_crawl_links(mock_fetch_html, html_sample):
    mock_fetch_html.return_value = html_sample
    db_name = "test_temp.db"
    start_url = "https://en.wikipedia.org/wiki/Chipmunk"
    try:
        crawl_links(db_name, start_url, depth=1)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM links")
        results = cursor.fetchall()
        conn.close()

        assert len(results) > 1, "Ссылки должны быть сохранены в базе данных"
        assert any("Chipmunk" in row[0] for row in results), "Ссылка на Chipmunk должна быть сохранена"

    finally:
        if os.path.exists(db_name):
            os.remove(db_name)
