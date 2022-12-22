CREATE DATABASE MoAhmedd_homework;

DROP TABLE MoAhmedd_homework.bronze_views;

CREATE EXTERNAL TABLE
MoAhmedd_homework.bronze_views (
  article STRING, 
  views INT, 
  rank INT, 
  date DATE, 
  retrieved_at TIMESTAMP
) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ceu-moahmed/datalake/views/';
