# Database Optimization and Embeddings Generation

This repository contains work to optimise the database used within the antiSMI project and is of no value to external contributors.

This repository contains SQL scripts and Python code for optimizing and accelerating a PostgreSQL database by normalizing tables and adding necessary indexes. Additionally, it includes a script for generating embeddings for news articles and storing them in the database.

## Table of Contents
- [Introduction](#introduction)
- [Database Normalization](#database-normalization)

- [Embeddings Generation](#embeddings-generation)
- [Usage](#usage)
  - [Insert Data Example](#insert-data-example)
  - [Update Data Example](#update-data-example)
  - [Delete Data Example](#delete-data-example)
- [License](#license)

## Introduction

The purpose of this repository is to demonstrate how to normalize a large PostgreSQL database to improve performance, and how to generate and store embeddings for text data. This is particularly useful for handling large datasets such as millions of news articles.

## Database Normalization

You should use the script `sql.sql` to nomalise the db data.

It involves the implementation of the steps necessary for this purpose: 

  - Step 1: Create Normalized Tables
  - Step 2: Migrate Data to Normalized Tables
  - Step 3: Create user-friendly views
  - Step 3: Create Rules and Triggers to ensure compatibility between views and the new data structure 

## Embeddings Generation

The new data structure requires recreating the news embeddings, for which the `cook_db_embs.py` script is used.
The project uses LaBSE embeddings, which tend to equally arrange vectors of similar words in different languages in vector space.

## Usage

Here are examples of how to interact with the new data structure.

### Insert Data Example
```sql
-- Insert News
INSERT INTO news_view (url, title, resume, news, date, agency, category, links)
VALUES ('https://example.com', 'Example Title', 'Example Resume', 'Example News',
        '2023-01-01 12:00:00+00', 'example_agency', 'example_category', 'https://source1.com, https://source2.com');
```


### Update Data Example
```sql
-- Update News
UPDATE news_view
SET title = 'Updated Title'
WHERE url = 'https://example.com';
```

### Delete Data Example
```sql
-- Delete News
DELETE FROM news_view
WHERE url = 'https://example.com';
```


## License
This project is licensed under the MIT License.

