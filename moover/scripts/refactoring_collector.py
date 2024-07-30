# import asyncio
import asyncpg
from pydantic import BaseModel, HttpUrl, Field
from enum import Enum
from datetime import datetime
from loguru import logger
from .db import DATABASE_FROM, DATABASE_TO


class CategoryEnum(str, Enum):
    """
    Class to check if it belongs to the used categories
    Класс для проверки на принадлежность используемым категориям
    """
    economy = 'economy'
    science = 'science'
    sports = 'sports'
    technology = 'technology'
    entertainment = 'entertainment'
    society = 'society'
    other = 'other'
    culture = 'culture'


class AntismiFields(BaseModel):
    """
    Field validation class for writing to the antiSMI database
    Класс валидации полей для записи в базу данных antiSMI
    """
    url: HttpUrl
    date: datetime
    title: str
    resume: str
    category: CategoryEnum
    # clear the empty columns
    # убираем пустые столбцы
    news: str = Field(..., min_length=1)  # "'...' - required
    links: list[HttpUrl] | None
    agency: str


class AntismiModel(BaseModel):
    """
    Class for validating the list of news to be written to the antiSMI database
    Класс для валидации списка новостей, который будет записан в базу данных antiSMI
    """
    dicts_list: list[AntismiFields]


def validate_news(news_data: list[dict]):
    """
    Basic function for validating and writing news to antiSMI database
    Основная функция для валидации и записи новостей в базу данных antiSMI
    """
    record_news_list = []
    for news in news_data:
        try:
            validator_fields = AntismiFields(**news)
            record_news_list.append(news)
        except ValueError:
            logger.error(news)
    return record_news_list


def convert_url_string_to_list(url_string):
    # Если строка пустая, возвращаем пустой список
    if not url_string:
        return []

    # Разделяем строку по запятым и удаляем пробелы вокруг каждого URL
    url_list = [url.strip() for url in url_string.split(',')]

    # Удаляем пустые строки, которые могли возникнуть из-за лишних запятых
    url_list = [url for url in url_list if url]

    return url_list


# Асинхронная функция для получения данных из базы данных PostgreSQL
async def fetch_data():
    conn = await asyncpg.connect(DATABASE_FROM)

    # Выполнение запроса к базе данных
    query = 'SELECT * FROM final'
    records = await conn.fetch(query)

    # Преобразование записей в список словарей
    result = [dict(record) for record in records]

    await conn.close()

    return result


async def insert_data(records):
    conn = await asyncpg.connect(DATABASE_TO)

    # Формируем SQL-запрос для вставки данных
    query = '''
        INSERT INTO news_view(
        url, date, news, agency, links, title, resume, category, embedding
        ) 
        VALUES(
        $1, $2, $3, $4, $5, $6, $7, $8, NULL
        )
    '''

    # Асинхронная запись данных в таблицу
    await conn.executemany(query, [(record['url'], record['date'], record['news'], record['agency'], record['links'],
                                    record['title'], record['resume'], record['category']) for record in records])

    await conn.close()

