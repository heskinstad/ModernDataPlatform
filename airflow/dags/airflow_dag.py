from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
import datetime

spark_dag = DAG(
    "spark_microbatch_weather",
    start_date=datetime(2025,1,1),
    schedule="@hourly",
    catchup=False
)

Extract = SparkSubmitOperator(
    task_id="spark_weather_batch",
    application="/opt/consumers_producers/spark_consumer.py",
    conn_id="spark_default",
    dag=spark_dag
)

Extract