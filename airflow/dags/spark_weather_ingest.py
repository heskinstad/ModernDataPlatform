from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='spark_weather_ingest',
    default_args=default_args,
    description='Run Spark micro-batch to ingest weather events from Kafka into Parquet',
    schedule_interval='*/5 * * * *',  # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Adjust this command if your spark-submit location or packages differ
SPARK_CMD = (
    'spark-submit '
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 '
    '/opt/airflow/dags/../../consumers_producers/spark_consumer.py '
    '--once '
)

run_spark = BashOperator(
    task_id='run_spark_consumer_once',
    bash_command=SPARK_CMD,
    dag=dag,
)

run_spark
