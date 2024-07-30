from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

from scripts.refactoring_collector import fetch_data, insert_data, convert_url_string_to_list, validate_news


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


async def main():
    try:
        # await move_news_to_new_db()
        scheduler = AsyncIOScheduler()
        scheduler.configure(timezone='Europe/Moscow')
        scheduler.add_job(move_news_to_new_db, 'cron', hour='9-21/4, 23', minute=53, id='moving',
                          max_instances=10, misfire_grace_time=600)
        scheduler.start()

        while True:
            await asyncio.sleep(1)

    except Exception as e:
        logger.exception(e)

if __name__ == '__main__':
    asyncio.run(main())
