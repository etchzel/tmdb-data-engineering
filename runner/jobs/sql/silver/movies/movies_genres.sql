CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.movies_genres (
    movie_id BIGINT,
    genre_id BIGINT,
    genre_name VARCHAR
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/silver/movies_genres',
   max_commit_retry = 4
);

WITH deduped_movies AS (
    SELECT id, genres
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
        FROM bronze.movies
    )
    WHERE rn = 1
),
source AS (
    SELECT id as movie_id,
           genre_id,
           name as genre_name
    FROM deduped_movies AS m,
         UNNEST(genres) AS g(genre_id, name)
)

MERGE INTO silver.movies_genres AS target
USING source
ON target.movie_id = source.movie_id AND 
   target.genre_id = source.genre_id
WHEN NOT MATCHED THEN
    INSERT (movie_id, genre_id, genre_name)
    VALUES (source.movie_id, source.genre_id, source.genre_name);