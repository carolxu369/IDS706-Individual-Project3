-- Databricks notebook source
-- Create Delta table with additional transformations and considerations
CREATE OR REPLACE TABLE prepared_song_data
USING DELTA
AS
SELECT
  artist_id,
  TRIM(artist_name) AS artist_name, -- Trimming whitespace
  CASE 
    WHEN duration < 0 THEN NULL 
    ELSE duration 
  END AS duration, -- Handling negative duration
  release,
  tempo,
  time_signature,
  TRIM(title) AS title, -- Trimming whitespace from title
  year,
  current_timestamp() AS processed_time
FROM
  raw_song_data
WHERE
  year > 0 AND artist_name IS NOT NULL
  AND duration IS NOT NULL;-- Additional data quality check