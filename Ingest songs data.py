# Databricks notebook source
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, year, current_date

# Define variables used in the code below
file_path = "/databricks-datasets/songs/data-001/"
table_name = "raw_song_data"
checkpoint_path = "/tmp/pipeline_get_started/_checkpoint/song_data"

schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)

# Reading the stream
raw_data = (spark.readStream
    .format("cloudFiles")
    .schema(schema)
    .option("cloudFiles.format", "csv")
    .option("sep", "\t")
    .load(file_path))

# Getting the current year
current_year_value = year(current_date())

# Data validation
filtered_data = raw_data.filter(
    col("year").isNotNull() &
    col("artist_name").isNotNull() &
    col("duration").between(0, 3600) &  # Assuming duration in seconds
    col("year").between(1900, current_year_value) &
    col("song_id").isNotNull()  # Assuming song_id is a critical field
)

# Write stream to Delta Lake
(filtered_data.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name))
