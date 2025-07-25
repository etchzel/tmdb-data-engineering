CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.series_details;
CREATE TABLE IF NOT EXISTS gold.series_details
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/gold/series_details',
   max_commit_retry = 4
)
AS
SELECT genre_name,
       series_original_name,
       localized_name,
       first_air_date,
       last_air_date,
       number_of_seasons,
       number_of_episodes,
       latest_episode,
       next_episode,
       series_status,
       series_type,
       in_production,
       original_language,
       season_number,
       season_name,
       season_air_date,
       season_episode_count,
       series_popularity,
       series_vote_average,
       series_vote_count
FROM silver.series_curated AS sc
LEFT JOIN silver.series_genres AS sg ON sc.series_id = sg.series_id
LEFT JOIN silver.series_seasons AS ss ON sc.series_id = ss.series_id;