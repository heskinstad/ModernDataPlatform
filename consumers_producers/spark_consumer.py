from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth


KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "weather.events"
OUTPUT_PATH = "data/warehouse/weather"
CHECKPOINT = "data/checkpoints/weather"
# default package coordinate for Spark-Kafka integration (match Spark/PySpark version)
KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"


def main(bootstrap=KAFKA_BOOTSTRAP, topic=TOPIC, output_path=OUTPUT_PATH, checkpoint=CHECKPOINT, run_once=False):
    # include Kafka connector package so `format("kafka")` is available when
    # running via `python spark_consumer.py` (Spark will attempt to download the package).
    # When using `spark-submit` you can also pass the package with `--packages`.
    spark = (
        SparkSession.builder.appName("WeatherKafkaToParquet")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .getOrCreate()
    )

    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("station_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("temperature_c", DoubleType()),
        StructField("min_c", DoubleType()),
        StructField("max_c", DoubleType()),
        StructField("pressure_hpa", DoubleType()),
        StructField("humidity_pct", DoubleType()),
        StructField("wind_kph", DoubleType()),
    ])

    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
    except Exception as e:
        msg = str(e)
        if "Failed to find data source: kafka" in msg or "Data source kafka" in msg:
            print("ERROR: Spark Kafka data source not found. Ensure the Spark-Kafka connector is available.")
            print("Recommended: run with spark-submit and add --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            print("Or set 'spark.jars.packages' to the same coordinate when creating the SparkSession.")
        # re-raise for visibility after printing guidance
        raise

    # value is bytes, cast to string and parse JSON
    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))
    flattened = parsed.select("data.*")

    # convert timestamp string to timestamp type and add partitions
    with_ts = (
        flattened.withColumn("ts", to_timestamp(col("timestamp")))
        .withColumn("year", year(col("ts")))
        .withColumn("month", month(col("ts")))
        .withColumn("day", dayofmonth(col("ts")))
        .drop("timestamp")
    )

    writer = (
        with_ts.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("year", "month", "day")
        .outputMode("append")
    )

    if run_once:
        # trigger once processes available data and then exits (micro-batch)
        q = writer.trigger(once=True).start()
        print(f"Running micro-batch spark job -> {output_path} (checkpoint: {checkpoint})")
        q.awaitTermination()
    else:
        q = writer.start()
        print(f"Running continuous streaming job -> {output_path} (checkpoint: {checkpoint})")
        q.awaitTermination()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Spark consumer for weather events")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--output", default=OUTPUT_PATH)
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--once", action="store_true", help="Run micro-batch once and exit")
    args = parser.parse_args()
    main(bootstrap=args.bootstrap, topic=args.topic, output_path=args.output, checkpoint=args.checkpoint, run_once=args.once)

