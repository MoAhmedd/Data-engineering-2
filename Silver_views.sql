DROP TABLE MoAhmedd_homework.silver_views;
CREATE TABLE MoAhmedd_homework.silver_views
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://ceu-moahmed/datalake/views_silver'
) AS SELECT article, views, rank, date
FROM MoAhmedd_homework.bronze_views 
WHERE date IS NOT NULL;

SELECT * FROM MoAhmedd_homework.silver_views;
