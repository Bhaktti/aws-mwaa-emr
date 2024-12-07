
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
import os
import sys

if __name__ == '__main__':
    bucket=sys.argv[1]
    
    spark = SparkSession.builder.appName('Pre Process parks revenue').getOrCreate()
    
    # Read the CSV file
    revenue = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/parks_revenue.csv")
    # Remove commas from numeric values and convert to integers
    df = revenue.withColumn('Visitation', regexp_replace('Visitation', ',', '').cast(IntegerType()))
    df = df.withColumn('Jobs Supported', regexp_replace('Jobs Supported', ',', '').cast(IntegerType()))
    df = df.withColumn('Local Jobs', regexp_replace('Local Jobs', ',', '').cast(IntegerType()))
    
    # Remove dollar signs and commas from monetary values and convert to floats
    df = df.withColumn('Visitor Spending', regexp_replace('Visitor Spending', '[$, billion]', '').cast(FloatType()) * 1e9)
    df = df.withColumn('Total Output', regexp_replace('Total Output', '[$, billion]', '').cast(FloatType()) * 1e9)
    
    # Rename the first column to 'Year'
    revenue_parquet = df.withColumnRenamed(df.columns[0], 'Year')
    # # Write the transformed data back to S3
    # revenue_csv.write.mode("overwrite").csv(f"s3://{bucket}/raw/parks_revenue_2", header=True)
    # # spark = SparkSession.builder.appName('Pre Process parks revenue').getOrCreate()
    # revenue_out = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/parks_revenue_2/*.csv")
    revenue_parquet.write.mode('overwrite').parquet(f"s3://{bucket}/output/nationalparkrevenue/")
