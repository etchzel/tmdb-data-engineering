CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.movies_curated (
    movie_id BIGINT,
    imdb_id VARCHAR,
    movie_title VARCHAR,
    localized_title VARCHAR,
    movie_original_language VARCHAR,
    movie_release_date DATE,
    movie_tagline VARCHAR,
    movie_overview VARCHAR,
    movie_budget BIGINT,
    movie_revenue BIGINT,
    movie_length BIGINT,
    movie_status VARCHAR,
    movie_popularity DOUBLE,
    movie_vote_average DOUBLE,
    movie_vote_count BIGINT,
    as_of TIMESTAMP
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/silver/movies_curated',
   max_commit_retry = 4
);

MERGE INTO silver.movies_curated AS target
USING (
    SELECT id as movie_id,
           imdb_id,
           original_title as movie_title,
           title as localized_title,
           original_language as movie_original_language,
           DATE(CASE WHEN release_date = '' THEN NULL ELSE release_date END) as movie_release_date,
           tagline as movie_tagline,
           overview as movie_overview,
           budget as movie_budget,
           revenue as movie_revenue,
           runtime as movie_length,
           status as movie_status,
           popularity as movie_popularity,
           vote_average as movie_vote_average,
           vote_count as movie_vote_count,
           CAST(FROM_UNIXTIME_NANOS(_ingest_ts * 1000) as TIMESTAMP(6) WITH TIME ZONE) as as_of
    FROM (
        -- dedup based on the latest ingest timestamp
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
        FROM bronze.movies
    )
    WHERE rn = 1
) AS source
ON target.movie_id = source.movie_id
WHEN MATCHED AND target.as_of < source.as_of THEN
    UPDATE SET movie_release_date = source.movie_release_date,
               movie_revenue = source.movie_revenue,
               movie_status = source.movie_status,
               movie_popularity = source.movie_popularity,
               movie_vote_average = source.movie_vote_average,
               movie_vote_count = source.movie_vote_count,
               as_of = source.as_of
WHEN NOT MATCHED THEN
    INSERT (movie_id, imdb_id, movie_title, localized_title, movie_original_language, movie_release_date, movie_tagline, movie_overview, movie_budget, movie_revenue, movie_length, movie_status, movie_popularity, movie_vote_average, movie_vote_count, as_of)
    VALUES (source.movie_id, source.imdb_id, source.movie_title, source.localized_title, source.movie_original_language, source.movie_release_date, source.movie_tagline, source.movie_overview, source.movie_budget, source.movie_revenue, source.movie_length, source.movie_status, source.movie_popularity, source.movie_vote_average, source.movie_vote_count, source.as_of);
