CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.series_curated (
    series_id BIGINT,
    series_original_name VARCHAR,
    localized_name VARCHAR,
    first_air_date DATE,
    last_air_date DATE,
    number_of_seasons BIGINT,
    number_of_episodes BIGINT,
    latest_episode BIGINT,
    next_episode BIGINT,
    series_status VARCHAR,
    series_type VARCHAR,
    in_production BOOLEAN,
    original_language VARCHAR,
    overview VARCHAR,
    series_popularity DOUBLE,
    series_vote_average DOUBLE,
    series_vote_count BIGINT,
    as_of TIMESTAMP
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/silver/series_curated',
   max_commit_retry = 4
);

MERGE INTO silver.series_curated AS target
USING (
    SELECT id as series_id,
           original_name as series_original_name,
           name as localized_name,
           DATE(CASE WHEN first_air_date = '' THEN NULL ELSE first_air_date END) as first_air_date,
           DATE(CASE WHEN last_air_date = '' THEN NULL ELSE last_air_date END) as last_air_date,
           number_of_seasons,
           number_of_episodes,
           last_episode_to_air.episode_number as latest_episode,
           next_episode_to_air.episode_number as next_episode,
           status as series_status,
           type as series_type,
           in_production,
           original_language,
           overview,
           popularity as series_popularity,
           vote_average as series_vote_average,
           vote_count as series_vote_count,
           CAST(FROM_UNIXTIME_NANOS(_ingest_ts * 1000) as TIMESTAMP(6) WITH TIME ZONE) as as_of
    FROM (
        -- dedup based on the latest ingest timestamp
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
        FROM bronze.series
    )
    WHERE rn = 1
) AS source
ON target.series_id = source.series_id
WHEN MATCHED AND target.as_of < source.as_of THEN
    UPDATE SET first_air_date = source.first_air_date,
               last_air_date = source.last_air_date,
               number_of_seasons = source.number_of_seasons,
               number_of_episodes = source.number_of_episodes,
               latest_episode = source.latest_episode,
               next_episode = source.next_episode,
               series_status = source.series_status,
               series_type = source.series_type,
               in_production = source.in_production,
               series_popularity = source.series_popularity,
               series_vote_average = source.series_vote_average,
               series_vote_count = source.series_vote_count,
               as_of = source.as_of
WHEN NOT MATCHED THEN
    INSERT (series_id, series_original_name, localized_name, first_air_date, last_air_date, number_of_seasons, number_of_episodes, latest_episode, next_episode, series_status, series_type, in_production, original_language, overview, series_popularity, series_vote_average, series_vote_count, as_of)
    VALUES (source.series_id, source.series_original_name, source.localized_name, source.first_air_date, source.last_air_date, source.number_of_seasons, source.number_of_episodes, source.latest_episode, source.next_episode, source.series_status, source.series_type, source.in_production, source.original_language, source.overview, source.series_popularity, source.series_vote_average, source.series_vote_count, source.as_of);