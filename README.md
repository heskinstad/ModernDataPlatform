# ModernDataPlatform
ModernDataPlatform exercise

## Weather streaming (producer -> Kafka -> Spark -> Parquet)

This repo includes a simple streaming ingestion example that produces synthetic weather sensor events into Kafka and consumes them with Spark Structured Streaming to write partitioned Parquet files locally.

- Schema: `producers/schema.json`

Example record:

```
{
	"timestamp": "2025-11-25T12:00:00Z",
	"station_id": "STATION-A",
	"latitude": 47.61,
	"longitude": -122.33,
	"temperature_c": 16.32,
	"min_c": 14.5,
	"max_c": 18.9,
	"pressure_hpa": 1011.23,
	"humidity_pct": 67.1,
	"wind_kph": 6.2
}
```

Files added:
- `producers/weather_producer.py` — continuous Kafka producer (JSON messages). Uses `station_id` as Kafka key for partitioning.
- `producers/run_producer_once.py` — helper to produce a fixed number of records for testing.
- `consumers_producers/spark_consumer.py` — PySpark Structured Streaming job reading from Kafka and writing Parquet partitioned by `year/month/day`.

Notes and run steps (Windows PowerShell):

1. Start the local Kafka cluster included in `docker-compose.yml`:

```powershell
docker-compose up -d
```

2. (Optional) Install Python deps in a venv:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Produce test data (200 messages) locally:

```powershell
# from repo root
python .\producers\run_producer_once.py
```

To run continuously:

```powershell
python .\producers\weather_producer.py --continuous --interval 1.0
```

4. Run the Spark consumer (requires `spark-submit` / PySpark available):

```powershell
spark-submit .\consumers_producers\spark_consumer.py
```

Output will be written under `data/warehouse/weather/` partitioned by `year=.../month=.../day=...` and checkpoints under `data/checkpoints/weather/`.

If you need the Kafka topic to exist manually, you can create it using the `tools` container in the compose file or the producer will attempt to create the topic on start.

### Partitioning rationale

- **Partition key**: `year/month/day` derived from the event `timestamp`.
- **Why**: Time (date) is typically the most significant analytic dimension for sensor/time-series data — queries commonly filter by date ranges. Partitioning by date gives efficient pruning for reads and makes the on-disk layout clear: `data/warehouse/weather/year=2025/month=11/day=25/...`.

### Airflow orchestration

An Airflow DAG `spark_weather_ingest` is included at `airflow/dags/spark_weather_ingest.py`. It schedules the Spark micro-batch consumer every 5 minutes using `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 ... --once` so each DAG run executes a single micro-batch and exits.

To run Airflow (compose already contains Airflow services):

```powershell
docker-compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

Open the Airflow web UI at `http://localhost:8080` (username/password created by the init job: `admin`/`admin`).

Trigger `spark_weather_ingest` from the UI or let it run on schedule. The task executes `spark-submit` inside the environment where Airflow runs — ensure `spark-submit` is available in that environment (Airflow image in this repo does not include Spark by default). Two options:

- Run Airflow on a host that has Spark installed and accessible (the `spark-submit` in the DAG will then run locally on that host).
- Or modify the DAG to use a DockerOperator that runs Spark in a container image with Spark installed.

If you want, I can add a DockerOperator variant of the DAG so Spark runs inside a container, or add a small helper to create the topic explicitly.

