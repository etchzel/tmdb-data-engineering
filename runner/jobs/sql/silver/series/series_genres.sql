CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.series_genres (
    series_id BIGINT,
    genre_id BIGINT,
    genre_name VARCHAR
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/silver/series_genres',
   max_commit_retry = 4
);

WITH deduped_series AS (
    SELECT id, genres
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
        FROM bronze.series
    )
    WHERE rn = 1
),
source AS (
    SELECT id as series_id,
           genre_id,
           name as genre_name
    FROM deduped_series AS s,
         UNNEST(genres) AS g(genre_id, name)
)
MERGE INTO silver.series_genres AS target
USING source
ON target.series_id = source.series_id AND 
   target.genre_id = source.genre_id
WHEN NOT MATCHED THEN
    INSERT (series_id, genre_id, genre_name)
    VALUES (source.series_id, source.genre_id, source.genre_name);