from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

import os
os.environ["HADOOP_HOME"] = "C:/Users/jcuadros/hadoop"
builder = SparkSession.builder \
    .appName("test_delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("✅ SparkSession con Delta Lake funcionando!")
spark.stop()