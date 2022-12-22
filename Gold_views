CREATE TABLE MoAhmedd_homework.gold_allviews
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://ceu-moahmed/datalake/gold_allviews'
) AS SELECT
article,
sum(views) as total_top_view,
min(rank) as top_rank,
count(date) as ranked_days
FROM MoAhmedd_homework.silver_views 
GROUP BY article;
SELECT * FROM MoAhmedd_homework.gold_allviews;
