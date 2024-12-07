CREATE EXTERNAL TABLE IF NOT EXISTS parks_activity_data (
	park_states STRING,
	park_parkCode STRING,
	park_designation STRING,
	park_fullName STRING,
	park_url STRING,
	park_name STRING,
	id STRING,
	name STRING
)

PARTITIONED BY (id STRING)
STORED AS PARQUET
LOCATION 's3://demobucketnov2024/output/nationalparkactivities'
tblproperties ("parquet.compression"="SNAPPY");