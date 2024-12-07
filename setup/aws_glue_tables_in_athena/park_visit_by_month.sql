CREATE EXTERNAL TABLE IF NOT EXISTS park_visits_by_month (
  ParkName STRING,
  UnitCode STRING,
  ParkType STRING,
  Region STRING,
  State STRING,
  Year STRING,
  Month STRING,
  RecreationVisits STRING
)
STORED AS PARQUET
LOCATION 's3://demobucketnov2024/output/nationalparkvisitsbymonth'
TBLPROPERTIES ("parquet.compression"="SNAPPY");