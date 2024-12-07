from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col
import os
import sys

if __name__ == '__main__':


    bucket=sys.argv[1]
    spark = SparkSession.builder.appName('Pre Process park nationalparkvisitsbymonth Dataset').getOrCreate()
    park_visit_by_month = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/US-National-Parks_Use_1979-2023_By-Month.csv")
    park_visit_by_month.write.mode('overwrite').parquet(f"s3://{bucket}/output/nationalparkvisitsbymonth/")