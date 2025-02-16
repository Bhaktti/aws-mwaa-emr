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
    spark = SparkSession.builder.appName('Pre Process park code Dataset').getOrCreate()
    parks_code_data = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/parks_code_data.csv")
    parks_code_data.write.mode('overwrite').parquet(f"s3://{bucket}/output/nationalparkcode/")