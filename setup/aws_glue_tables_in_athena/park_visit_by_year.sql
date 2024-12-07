CREATE EXTERNAL TABLE IF NOT EXISTS park_visits_by_year (
  ParkName STRING,
  Region STRING,
  State STRING,
  Year STRING,
  RecreationVisits STRING
)
STORED AS PARQUET
LOCATION 's3://demobucketnov2024/output/nationalparkvisitsbyyear'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
