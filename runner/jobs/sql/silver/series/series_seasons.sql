CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.series_seasons (
    series_id BIGINT,
    season_id BIGINT,
    season_number BIGINT,
    season_name VARCHAR,
    season_air_date DATE,
    season_overview VARCHAR,
    season_episode_count BIGINT
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/silver/series_seasons',
   max_commit_retry = 4
);

MERGE INTO silver.series_seasons AS target
USING (
    WITH deduped_series AS (
        SELECT id, seasons
        FROM (
            SELECT *,
                  ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
            FROM bronze.series
        )
        WHERE rn = 1
    ),
    season_unnest AS (
        SELECT s.id as series_id,
               se.id as season_id,
               se.season_number,
               se.name as season_name,
               DATE(CASE WHEN se.air_date = '' THEN NULL ELSE se.air_date END) as season_air_date,
               se.overview as season_overview,
               se.episode_count as season_episode_count
        FROM deduped_series AS s,
             UNNEST(seasons) AS se(air_date, episode_count, id, name, overview, poster_path, season_number)
    ),
    season_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY series_id, season_id) as rn
        FROM season_unnest
    )
    SELECT series_id,
           season_id,
           season_number,
           season_name,
           season_air_date,
           season_overview,
           season_episode_count
    FROM season_dedup
    WHERE rn = 1
) AS source
ON target.series_id = source.series_id AND 
   target.season_id = source.season_id
WHEN NOT MATCHED THEN
    INSERT (series_id, season_id, season_number, season_name, season_air_date, season_overview, season_episode_count)
    VALUES (source.series_id, source.season_id, source.season_number, source.season_name, source.season_air_date, source.season_overview, source.season_episode_count);