from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DateType, FloatType
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .getOrCreate()

schema = StructType([
    StructField("date", DateType(), True),
    StructField("temp", FloatType(), True),
])

print("Initialized")

# Read from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092,localhost:29093,localhost:29094") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "earliest") \
    .load()

print("Stream read successfully")

# Decode the Kafka message value as string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON message and apply schema
parsed_df = kafka_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Transformations: You can apply your normalization/transformation logic here
# E.g., to partition by date:
processed_df = parsed_df.withColumn("date", col("timestamp").cast("date"))

# Write the data to Parquet, partitioned by date
query = processed_df.writeStream \
    .format("parquet") \
    .option("path", "/path/to/output/") \
    .option("checkpointLocation", "/path/to/checkpoint/") \
    .partitionBy("date") \
    .outputMode("append") \
    .start()

query.awaitTermination()
