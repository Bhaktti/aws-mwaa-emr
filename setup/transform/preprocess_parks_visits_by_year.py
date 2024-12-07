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
    spark = SparkSession.builder.appName('Pre Process park nationalparkvisitsbyyear Dataset').getOrCreate()
    park_visit_by_year = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/US-National-Parks_RecreationVisits_1979-2023.csv")
    park_visit_by_year.write.mode('overwrite').parquet(f"s3://{bucket}/output/nationalparkvisitsbyyear/")