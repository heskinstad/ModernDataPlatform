from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_date
from pyspark.sql.types import DoubleType
import os
import platform

# Initialize Spark session with local mode (all-in-one process)
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .master("local[*]") \
    .getOrCreate()

print("Initialized Spark session")

# Configuration (can be overridden by environment variables)
# Auto-detect: use container hostnames if running in Docker, localhost if running locally
is_docker = os.environ.get("DOCKER_ENV") == "true"
default_bootstrap = "kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095" if is_docker else "localhost:29092,localhost:29093,localhost:29094,localhost:29095"

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", default_bootstrap)
topic = os.environ.get("KAFKA_TOPIC", "weather-data")

# For local runs, use current directory; for Docker, use the container path
if is_docker:
    output_path = os.environ.get("OUTPUT_PATH", "/opt/consumers_producers/output")
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "/opt/consumers_producers/checkpoint")
else:
    # default local paths inside the repository
    output_path = os.environ.get("OUTPUT_PATH", "./consumers_producers/output")
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "./consumers_producers/checkpoint")

# Streaming termination mode: set STREAM_ONCE=true to process available data and exit
# or leave unset (or false) to run continuously.
stream_once = os.environ.get("STREAM_ONCE", "false").lower() in ("1", "true", "yes")

# Read from Kafka topic as a streaming source
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

print("Kafka stream source created")

# The producer in this repo emits JSON arrays like ["2025-01-01", 1.234]
# We'll extract the array elements using JSON paths: $[0] -> date string, $[1] -> temperature
kafka_text = kafka_df.selectExpr("CAST(value AS STRING) as value_str")

parsed = kafka_text.select(
    get_json_object(col("value_str"), "$[0]").alias("date_str"),
    get_json_object(col("value_str"), "$[1]").alias("temp_str"),
)

# Cast/convert types and normalize column names
processed_df = parsed.select(
    to_date(col("date_str"), "yyyy-MM-dd").alias("date"),
    col("temp_str").cast(DoubleType()).alias("temp"),
)

# Write the data to Parquet, partitioned by date. Ensure output/checkpoint paths are mounted
writer = (
    processed_df.writeStream
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .partitionBy("date")
    .outputMode("append")
)

if stream_once:
    # Process available data once and stop (useful for batch-style micro-batches)
    writer = writer.trigger(once=True)

query = writer.start()

print(f"Streaming to {output_path}, checkpoint {checkpoint_path}, kafka {kafka_bootstrap}, topic {topic}")
query.awaitTermination()
