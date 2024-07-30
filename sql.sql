-- РАЗДЕЛ I: НОРМАЛИЗАЦИЯ ТАБЛИЦ

-- Шаг 1: Создание временной таблицы с уникальными строками (исключение дублей)
CREATE TEMP TABLE temp_table AS
SELECT DISTINCT ON (url) *
FROM news;

-- Шаг 2: Удаление всех строк из оригинальной таблицы
TRUNCATE TABLE news;

-- Шаг 3: Вставка уникальных строк обратно в оригинальную таблицу
INSERT INTO news
SELECT *
FROM temp_table;

-- Удаление временной таблицы
DROP TABLE temp_table;


-- Преобразование типа ссылок из списка в массив
ALTER TABLE news
    ADD COLUMN links_array TEXT[];

UPDATE news
SET links_array = string_to_array(links, ', ');


ALTER TABLE news
    DROP COLUMN links;


ALTER TABLE news
    RENAME COLUMN links_array TO links;


-- Переименование прежней таблицы и создание новой с необходимой структурой данных

ALTER TABLE news
    RENAME TO old_news_table;


CREATE TABLE news
(
    id          SERIAL PRIMARY KEY,
    url         TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    resume      TEXT,
    news        TEXT,
    date        TIMESTAMP WITHOUT TIME ZONE,
    agency_id   INTEGER NOT NULL REFERENCES agencies (id),
    category_id INTEGER NOT NULL REFERENCES categories (id),
    links       TEXT[]
);



-- ПРИВЕДЕНИЕ ОСТАЛЬНЫХ ТАБЛИЦ К НЕОБХОДИМОМУ ВИДУ


-- Шаг 1: Проверка уникальности значений в столбце id
SELECT id, COUNT(*)
FROM agencies
GROUP BY id
HAVING COUNT(*) > 1;

-- Шаг 2: Установка столбца id в качестве первичного ключа
ALTER TABLE agencies
    ADD PRIMARY KEY (id);

-- Шаг 3: Настройка последовательности для автоматической генерации уникальных значений
CREATE SEQUENCE agencies_id_seq START WITH (SELECT MAX(id) + 1 FROM categories);
ALTER TABLE agencies
    ALTER COLUMN id SET DEFAULT nextval('agencies_id_seq');

-- Выполнение запроса для получения имени столбца первичного ключа

-- 1. Добавить новый столбец id с типом SERIAL
ALTER TABLE categories
    ADD COLUMN id SERIAL;

-- 2. Обновить таблицу, назначив новый столбец id в качестве первичного ключа
ALTER TABLE categories
    ADD PRIMARY KEY (id);

-- 3. Добавить уникальное ограничение на столбец category
ALTER TABLE categories
    ADD CONSTRAINT unique_category UNIQUE (category);
SET date = date AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC';


-- ПЕРЕНОС ДАННЫХ В НОВУЮ ТАБЛИЦУ ИЗ СТАРОЙ, УДАЛЕНИЕ СТАРОЙ ТАБЛИЦЫ

INSERT INTO news (url, title, resume, news, date, agency_id, category_id, links)
SELECT url,
       title,
       resume,
       news,
       old_news_table.date AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC', -- Преобразование времени в UTC
       (SELECT id FROM agencies WHERE telegram = old_news_table.agency),
       (SELECT id FROM categories WHERE category = old_news_table.category),
       links
FROM old_news_table;

DROP TABLE old_news_table;


-- РАЗДЕЛ II: ТРИГГЕРЫ ДЛЯ СОВМЕСТИМОСТИ ПРЕЖНЕЙ РАБОТЫ С ТЕКУЩЕЙ СТРУКТУРОЙ

-- !!! Меняем правила на триггеры, чтобы добавить работу с эмбеддингами в представлении news_view

-- -- Шаг 0: Обновляем представление news_view, которое дополнительно отображает эмбеддинги
CREATE OR REPLACE VIEW news_view AS
SELECT n.url,
       n.title,
       n.resume,
       n.news,
       n.date,
       a.telegram AS agency,
       c.category,
       n.links,
       e.embedding -- добавляем поле embedding из таблицы embs
FROM news n
         JOIN
     agencies a ON n.agency_id = a.id
         JOIN
     categories c ON n.category_id = c.id
         LEFT JOIN
     embs e ON n.url = e.news_url;
-- связываем с таблицей embs по полю url


-- Шаг 1: Создание функций-триггеров
-- 1. Функция для вставки

CREATE OR REPLACE FUNCTION trg_insert_news_view() RETURNS trigger AS
$$
BEGIN
    -- Вставка данных в таблицу news
    INSERT INTO news (url, title, resume, news, date, agency_id, category_id, links)
    VALUES (NEW.url,
            NEW.title,
            NEW.resume,
            NEW.news,
            (NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
            (SELECT id FROM agencies WHERE telegram = NEW.agency),
            (SELECT id FROM categories WHERE category = NEW.category),
            NEW.links);

    -- Вставка данных в таблицу embs, если передан embedding
    IF NEW.embedding IS NOT NULL THEN
        INSERT INTO embs (date, url, embedding)
        VALUES ((NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
                NEW.url,
                NEW.embedding);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Функция для обновления
CREATE OR REPLACE FUNCTION trg_update_news_view() RETURNS trigger AS
$$
BEGIN
    -- Обновление данных в таблице news
    UPDATE news
    SET title       = NEW.title,
        resume      = NEW.resume,
        news        = NEW.news,
        date        = (NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
        agency_id   = (SELECT id FROM agencies WHERE telegram = NEW.agency),
        category_id = (SELECT id FROM categories WHERE category = NEW.category),
        links       = NEW.links
    WHERE url = OLD.url
      AND date = OLD.date;

    -- Обновление данных в таблице embs, если передан embedding
    IF NEW.embedding IS NOT NULL THEN
        INSERT INTO embs (date, url, embedding)
        VALUES ((NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
                NEW.url,
                NEW.embedding)
        ON CONFLICT (date, url) DO UPDATE
            SET embedding = EXCLUDED.embedding;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 3. Функция для удаления
CREATE OR REPLACE FUNCTION trg_delete_news_view() RETURNS trigger AS
$$
BEGIN
    -- Удаление данных из таблицы news
    DELETE
    FROM news
    WHERE url = OLD.url
      AND date = OLD.date;

    -- Удаление данных из таблицы embs
    DELETE
    FROM embs
    WHERE url = OLD.url
      AND date = OLD.date;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- В таком случае, необходимо немного изменить триггеры, чтобы учитывать тот факт, что поле links уже является массивом text[]. Удалим ненужные преобразования и обновим функции триггеров.
-- Шаг 1: Создание функций-триггеров

--     Функция для вставки

CREATE OR REPLACE FUNCTION trg_insert_news_view() RETURNS trigger AS
$$
BEGIN
    -- Вставка данных в таблицу news
    INSERT INTO news (url, title, resume, news, date, agency_id, category_id, links)
    VALUES (NEW.url,
            NEW.title,
            NEW.resume,
            NEW.news,
            (NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
            (SELECT id FROM agencies WHERE telegram = NEW.agency),
            (SELECT id FROM categories WHERE category = NEW.category),
            NEW.links);

    -- Вставка данных в таблицу embs, если передан embedding
    IF NEW.embedding IS NOT NULL THEN
        INSERT INTO embs (date, url, embedding)
        VALUES ((NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
                NEW.url,
                NEW.embedding);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Функция для обновления

CREATE OR REPLACE FUNCTION trg_update_news_view() RETURNS trigger AS
$$
BEGIN
    -- Обновление данных в таблице news
    UPDATE news
    SET title       = NEW.title,
        resume      = NEW.resume,
        news        = NEW.news,
        date        = (NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
        agency_id   = (SELECT id FROM agencies WHERE telegram = NEW.agency),
        category_id = (SELECT id FROM categories WHERE category = NEW.category),
        links       = NEW.links
    WHERE url = OLD.url
      AND date = OLD.date;

    -- Обновление данных в таблице embs, если передан embedding
    IF NEW.embedding IS NOT NULL THEN
        INSERT INTO embs (date, url, embedding)
        VALUES ((NEW.date AT TIME ZONE 'Europe/Moscow') AT TIME ZONE 'UTC',
                NEW.url,
                NEW.embedding)
        ON CONFLICT (date, url) DO UPDATE
            SET embedding = EXCLUDED.embedding;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Функция для удаления

CREATE OR REPLACE FUNCTION trg_delete_news_view() RETURNS trigger AS
$$
BEGIN
    -- Удаление данных из таблицы news
    DELETE
    FROM news
    WHERE url = OLD.url
      AND date = OLD.date;

    -- Удаление данных из таблицы embs
    DELETE
    FROM embs
    WHERE url = OLD.url
      AND date = OLD.date;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Шаг 2: Создание триггеров
-- 1.Триггер на вставку
CREATE TRIGGER insert_news_view_trigger
    INSTEAD OF INSERT
    ON news_view
    FOR EACH ROW
EXECUTE FUNCTION trg_insert_news_view();

-- 2. Триггер на обновление
CREATE TRIGGER update_news_view_trigger
    INSTEAD OF UPDATE
    ON news_view
    FOR EACH ROW
EXECUTE FUNCTION trg_update_news_view();


-- 3. Триггер на удаление
CREATE TRIGGER delete_news_view_trigger
    INSTEAD OF DELETE
    ON news_view
    FOR EACH ROW
EXECUTE FUNCTION trg_delete_news_view();

-- Пример вставки, обновления и удаления данных через представление news_view
-- 1. Вставка данных
INSERT INTO news_view (url, title, resume, news, date, agency, category, links, embedding)
VALUES ('http://example.com/news1',
        'Example Title',
        'Example Resume',
        'Example News Content',
        '2023-06-01 12:00:00+03',
        'Example Agency',
        'Example Category',
        ARRAY ['https://example.com/source1', 'https://example.com/source2'],
        ARRAY [0.1, 0.2, 0.3]::float[]);

-- 2. Обновление данных
UPDATE news_view
SET title     = 'Updated Title',
    resume    = 'Updated Resume',
    news      = 'Updated News Content',
    date      = '2023-06-01 14:00:00+03',
    agency    = 'Updated Agency',
    category  = 'Updated Category',
    links     = ARRAY ['https://example.com/source3', 'https://example.com/source4'],
    embedding = ARRAY [0.4, 0.5, 0.6]::float[]
WHERE url = 'http://example.com/news1'
  AND date = '2023-06-01 12:00:00+03';


-- 3. Удаление данных
DELETE
FROM news_view
WHERE url = 'http://example.com/news1'
  AND date = '2023-06-01 12:00:00+03';



-- РАЗДЕЛ III: ПАРТИЦИРОВАНИЕ


-- Замена первичного ключа на составной первичный ключ
ALTER TABLE news
    DROP CONSTRAINT news_pkey;
ALTER TABLE news
    ADD PRIMARY KEY (url, date);

-- Создаём партицированную пустую таблицу с необходимой структурой
CREATE TABLE news_partitioned
(
    url         TEXT,
    title       TEXT,
    resume      TEXT,
    news        TEXT    NOT NULL,
    date        TIMESTAMP WITHOUT TIME ZONE,
    agency_id   INTEGER NOT NULL REFERENCES agencies (id),
    category_id INTEGER NOT NULL REFERENCES categories (id),
    links       TEXT[],
    PRIMARY KEY (url, date)
) PARTITION BY RANGE (date);

-- Создаем партиции для каждого года, начиная с 1999 года и заканчивая 2030 годом

-- 1. 1999 год
CREATE TABLE news_y1999 PARTITION OF news_partitioned
    FOR VALUES FROM ('1999-01-01') TO ('2000-01-01');

-- 2. 2000 год
CREATE TABLE news_y2000 PARTITION OF news_partitioned
    FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
--

-- 5. 2029 год
CREATE TABLE news_y2029 PARTITION OF news_partitioned
    FOR VALUES FROM ('2029-01-01') TO ('2030-01-01');


-- Переносим данные:
INSERT INTO news_partitioned
SELECT *
FROM news;

-- Заменяем старую таблицу:
DROP TABLE news;
ALTER TABLE news_partitioned
    RENAME TO news;