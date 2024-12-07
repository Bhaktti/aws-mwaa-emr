CREATE EXTERNAL TABLE IF NOT EXISTS parks_revenue_data (
  Year INT,
  Visitation INT,
  Visitor_Spending double,
  Jobs_Supported INT,
  Local_Jobs INT,
  Total_Output double
)
STORED AS PARQUET
LOCATION 's3://demobucketnov2024/output/nationalparkrevenue'
TBLPROPERTIES ("parquet.compression"="SNAPPY");