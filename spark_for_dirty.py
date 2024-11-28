from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, upper, trim

spark = SparkSession.builder \
    .appName("Read_csv_for_clean") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.coalescePartitions", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.join.enabled", "true") \
    .getOrCreate()


df = spark.read.csv("ds_dirty_fin_202410041147.csv", header=True, inferSchema=True)

df_cleaned = df.dropna(subset=["source_cd"])

df_trimmed = df_cleaned.withColumn("client_fio_full", trim(col("client_fio_full")))

df_upper = df_trimmed.select(
    *[upper(col(column)).alias(column) for column in df_cleaned.columns]
)

df_upper.show(3, truncate=False)

spark.stop()
