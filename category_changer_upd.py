from transformers import pipeline
import torch
import asyncio
import asyncpg
from typing import List, Tuple
import numpy as np
import json
import os

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# Используйте предобученную модель из Hugging Face Hub
classifier = pipeline("text-classification", model="data-silence/rus-news-classifier", device=device, truncation=True)

DB_CONFIG = {
    "host": "localhost",
    "database": "your_database",
    "user": "your_username",
    "password": "your_password"
}

BATCH_SIZE = 1000
CHECKPOINT_FILE = "classification_checkpoint.json"


def predict_category(text):
    common_result = classifier(text)
    result = [int(res['label'].split('_')[-1]) for res in common_result]
    return result


async def fetch_news_batch(conn: asyncpg.Connection, offset: int) -> List[Tuple[str, str]]:
    query = """
    SELECT url, news
    FROM news
    WHERE date::date < '2024-09-01'
    ORDER BY date
    LIMIT $1 OFFSET $2
    """
    return await conn.fetch(query, BATCH_SIZE, offset)


async def insert_classifications(conn: asyncpg.Connection, classifications: List[Tuple[str, int]]):
    query = """
    INSERT INTO news_reclassification (url, new_category_id)
    VALUES ($1, $2)
    """
    await conn.executemany(query, classifications)


async def update_news_categories(conn: asyncpg.Connection):
    query = """
    UPDATE news n
    SET category_id = nr.new_category_id
    FROM news_reclassification nr
    WHERE n.url = nr.url
    """
    await conn.execute(query)


async def process_batch(conn: asyncpg.Connection, batch: List[Tuple[str, str]]) -> List[Tuple[str, int]]:
    news_texts = [news for _, news in batch]
    predictions = predict_category(news_texts)
    return [(url, int(pred)) for (url, _), pred in zip(batch, predictions)]


def save_checkpoint(offset: int, total_processed: int):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"offset": offset, "total_processed": total_processed}, f)


def load_checkpoint() -> Tuple[int, int]:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
            return data["offset"], data["total_processed"]
    return 0, 0


async def classify_news():
    offset, total_processed = load_checkpoint()
    print(f"Starting from offset {offset}, total processed: {total_processed}")

    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            while True:
                batch = await fetch_news_batch(conn, offset)
                if not batch:
                    break

                classifications = await process_batch(conn, batch)
                await insert_classifications(conn, classifications)

                total_processed += len(batch)
                offset += BATCH_SIZE
                print(f"Processed {total_processed} news articles")

                save_checkpoint(offset, total_processed)

            print("Классификация завершена. Обновление категорий в таблице news...")
            # await update_news_categories(conn)
            # print("Обновление категорий завершено.")

    # Удаление файла чекпоинта после успешного завершения
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


if __name__ == '__main__':
    asyncio.run(classify_news())
