from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import StructType, StructField, DateType, FloatType
import json, sys, os

# Make sure Spark never uses the external IP
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-data")
DEFAULT_OUTPUT = os.environ.get("SPARK_OUTPUT_PATH", "./data/parquet_output")
DEFAULT_CHECKPOINT = os.environ.get("SPARK_CHECKPOINT_PATH", "./data/checkpoint")
DEFAULT_KAFKA_PACKAGE = os.environ.get("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")

spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .master("local[*]") \
    .config("spark.jars.packages", DEFAULT_KAFKA_PACKAGE) \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Using kafka package: {DEFAULT_KAFKA_PACKAGE}")


def run_once_consume(bootstrap_servers=DEFAULT_BOOTSTRAP, topic=DEFAULT_TOPIC,
                     output_path=DEFAULT_OUTPUT, checkpoint_path=DEFAULT_CHECKPOINT,
                     starting_offsets="earliest"):
    """Run a single micro-batch that reads available Kafka messages, parses
    the JSON-array produced by `kafka_producer.produce_weather_data()` and
    writes the result to Parquet. This uses `trigger(once=True)` so it will
    process available data and then stop (suitable for Airflow scheduled runs).
    """

    print(f"Connecting to Kafka: {bootstrap_servers} topic={topic}")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()

    # messages are JSON arrays like: ["2025-01-02", avgTemp, minTemp, maxTemp, pressure]
    json_col = col("value").cast("string")

    parsed = df.select(
        get_json_object(json_col, "$[0]").alias("date"),
        get_json_object(json_col, "$[1]").cast("double").alias("avgTemp"),
        get_json_object(json_col, "$[2]").cast("double").alias("minTemp"),
        get_json_object(json_col, "$[3]").cast("double").alias("maxTemp"),
        get_json_object(json_col, "$[4]").cast("double").alias("pressure")
    )

    # write a micro-batch (process available data and stop)
    query = parsed.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    print(f"Finished micro-batch write to {output_path} (checkpoint: {checkpoint_path})")


if __name__ == "__main__":
    # Allow simple CLI overrides: python spark_consumer.py <bootstrap> <topic>
    bs = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BOOTSTRAP
    tp = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TOPIC
    out = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_OUTPUT
    cp = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_CHECKPOINT

    run_once_consume(bootstrap_servers=bs, topic=tp, output_path=out, checkpoint_path=cp)


'''
import pandas as pd
import json
import random

# Parameters
num_customers = 500
num_products = 5000
categories = [i for i in range(1, 21)]  # 20 categories

# Customers
customers = [{
    "customer_id": i,
    "first_name": f"FirstName{i}",
    "last_name": f"LastName{i}",
    "email": f"user{i}@example.com",
    "country": random.choice(["USA", "UK", "Germany", "France", "Italy", "Spain", "Canada"])
} for i in range(1, num_customers+1)]

pd.DataFrame(customers).to_csv("customers.csv", index=False)

# Products
with open("products.json", "w") as f:
    for i in range(1, num_products+1):
        product = {
            "product_id": i,
            "name": f"Product{i}",
            "category": random.choice(categories),
            "price": round(random.uniform(5.0, 500.0), 2)
        }
        f.write(json.dumps(product) + "\n")
'''