from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

with DAG(
    "spark_microbatch_weather",
    start_date=datetime(2025,1,1),
    schedule="@hourly",
    catchup=False
) as dag:

    spark_etl = SparkSubmitOperator(
        task_id="spark_weather_batch",
        application="/opt/consumers_producers/spark_consumer.py",
        conn_id="spark_default"
    )
