CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.movies_details;
CREATE TABLE IF NOT EXISTS gold.movies_details
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/gold/movies_details',
   max_commit_retry = 4
)
AS
SELECT genre_name,
       movie_title,
       localized_title,
       movie_original_language,
       movie_release_date,
       movie_budget,
       movie_revenue,
       movie_length,
       movie_status,
       movie_popularity,
       movie_vote_average,
       movie_vote_count
FROM silver.movies_curated AS mg
LEFT JOIN silver.movies_genres AS mc ON mg.movie_id = mc.movie_id;