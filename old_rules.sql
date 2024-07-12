-- news_view_insert
INSERT INTO news (url, title, resume, news, date, agency_id, category_id, links)
  VALUES (new.url, new.title, new.resume, new.news, ((new.date AT TIME ZONE 'Europe/Moscow'::text) AT TIME ZONE 'UTC'::text), ( SELECT agencies.id
           FROM agencies
          WHERE (agencies.telegram = new.agency)), ( SELECT categories.id
           FROM categories
          WHERE (categories.category = new.category)), new.links)

-- news_view_update
UPDATE news SET title = new.title, resume = new.resume, news = new.news, date = ((new.date AT TIME ZONE 'Europe/Moscow'::text) AT TIME ZONE 'UTC'::text), agency_id = ( SELECT agencies.id
           FROM agencies
          WHERE (agencies.telegram = new.agency)), category_id = ( SELECT categories.id
           FROM categories
          WHERE (categories.category = new.category)), links = new.links
  WHERE (news.url = new.url)

-- news_view_delete
DELETE FROM news
  WHERE (news.url = old.url)


-- РАЗДЕЛ II: ПРЕДСТАВЛЕНИЯ, ПРАВИЛА, ТРИГГЕРЫ ДЛЯ СОВМЕСТИМОСТИ ПРЕЖНЕЙ РАБОТЫ С ТЕКУЩЕЙ СТРУКТУРОЙ

-- Создаем представление news_view, которое возвращает время в UTC
CREATE VIEW news_view AS
SELECT
    n.url,
    n.title,
    n.resume,
    n.news,
    n.date AS date, -- Возвращаем время в UTC, так как оно уже хранится в UTC в таблице news
    a.name AS agency,
    c.name AS category,
    array_to_string(n.links, ', ') AS links -- Преобразуем массив обратно в строку
FROM
    news n
JOIN
    agencies a ON n.agency_id = a.id
JOIN
    categories c ON n.category_id = c.id;


-- Правило для вставки в представление news_view
CREATE OR REPLACE RULE news_view_insert AS
ON INSERT TO news_view
DO INSTEAD
INSERT INTO news (url, title, resume, news, date, agency_id, category_id, links)
VALUES (
    NEW.url,
    NEW.title,
    NEW.resume,
    NEW.news,
    NEW.date AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC', -- Преобразование даты в UTC
    (SELECT id FROM agencies WHERE telegram = NEW.agency),
    (SELECT id FROM categories WHERE category = NEW.category),
  	NEW.links
	-- COALESCE(string_to_array(NEW.links, ', '), NEW.links::text[]) -- Преобразование строкового списка в массив
);

-- Правило для обновления
CREATE OR REPLACE RULE news_view_update AS
ON UPDATE TO news_view
DO INSTEAD
UPDATE news
SET
    title = NEW.title,
    resume = NEW.resume,
    news = NEW.news,
    date = NEW.date AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC', -- Преобразование даты в UTC
    agency_id = (SELECT id FROM agencies WHERE telegram = NEW.agency),
    category_id = (SELECT id FROM categories WHERE category = NEW.category),
    links = NEW.links -- Преобразование строкового списка в массив
WHERE url = NEW.url;

-- Правило для удаления
CREATE OR REPLACE RULE news_view_delete AS
ON DELETE TO news_view
DO INSTEAD
DELETE FROM news
WHERE url = OLD.url;


-- ПРИМЕРЫ ДЛЯ ТЕСТИРОВАНИЯ ПРАВИЛЬНОСТИ РАБОТЫ ПРЕДСТАВЛЕНИЯ

-- Вставка тестовых данных
INSERT INTO news_view (url, title, resume, news, date, agency, category, links)
VALUES (
    'https://example.com/news/1', -- URL
    'Test Title', -- Заголовок
    'Test Resume', -- Краткое содержание
    'Test News Content', -- Основное содержание новости
    '2024-07-06 12:00:00', -- Дата в московском времени
    'Lenta', -- Агентство (telegram идентификатор)
    'society', -- Категория
    '{https://example.com/source1, https://example.com/source2}' -- Ссылки на первоисточники через запятую
);


-- Обновление тестовых данных
UPDATE news_view
SET
    title = 'Updated Test Title',
    resume = 'Updated Test Resume',
    news = 'Updated Test News Content',
    date = '2024-07-07 12:00:00', -- Новая дата в московском времени
    agency = 'Lenta', -- Новое агентство
    category = 'sports', -- Новая категория
    links = '{https://example.com/source3, https://example.com/source4}' -- Новые ссылки на первоисточники
WHERE url = 'https://example.com/news/1';

-- Удаление тестовых данных
DELETE FROM news_view
WHERE url = 'https://example.com/news/1';