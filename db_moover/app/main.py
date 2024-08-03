from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

from scripts.api_async_client import NewsAPIClient
from scripts.moover_services import fetch_data, insert_data, convert_url_string_to_list, validate_news


async def move_news_to_new_db():
    logger.info('Получаем новости')
    data = await fetch_data()
    logger.info(f'Собрано из старой базы {len(data)} новостей')

    for record in data:
        record['links'] = convert_url_string_to_list(record['links'])
    record_news_list = validate_news(news_data=data)
    logger.info(f'Отобрано для записи в новую базу {len(record_news_list)} валидных новостей')

    await insert_data(records=record_news_list)
    logger.info(f'Новости успешно записаны в новую базу')


async def upsert_embs():
    api_client = NewsAPIClient("http://127.0.0.1:8000")
    logger.info('Идёт получение новостей без эмбеддингов')
    news = await api_client.fetch_news_without_embeddings()
    logger.info(f'Получено {len(news)} новостей')
    news_list = [n['news'] for n in news]
    logger.info('Начинается процесс генерации эмбеддингов для новостей')
    embs = await api_client.make_embs(news_list=news_list)
    update_list = [{"url": item["url"], "embedding": emb} for item, emb in zip(news, embs)]
    logger.info('Начинается процесс вставки эмбеддингов')
    update_batch_result = await api_client.update_news_batch(update_list)
    logger.info(f'{update_batch_result}')


async def main():
    try:
        # await move_news_to_new_db()
        await upsert_embs()

        # scheduler = AsyncIOScheduler()
        # scheduler.configure(timezone='Europe/Moscow')

        # scheduler.add_job(move_news_to_new_db, 'cron', hour='9-21/4, 23', minute=53, id='moving',
        #                   max_instances=10, misfire_grace_time=600)
        # scheduler.add_job(upsert_embs, 'cron', hour='22', id='embedder',
        #                   max_instances=10, misfire_grace_time=600)

        # scheduler.start()

        # while True:
        #     await asyncio.sleep(1)

    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    asyncio.run(main())
