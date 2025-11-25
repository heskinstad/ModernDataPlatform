import time
import json
import argparse
import random
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "weather.events"


class WeatherProducer:
    def __init__(self, bootstrap_servers=DEFAULT_BOOTSTRAP, topic=DEFAULT_TOPIC):
        self.bootstrap = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            linger_ms=5,
            acks=1,
        )
        try:
            self.admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
        except Exception:
            self.admin = None

        # small set of synthetic stations
        self.stations = [
            {"station_id": "STATION-A", "lat": 47.61, "lon": -122.33},
            {"station_id": "STATION-B", "lat": 40.71, "lon": -74.0},
            {"station_id": "STATION-C", "lat": 34.05, "lon": -118.24},
        ]
        # keep previous temp per station for smoother series
        self.prev_temp = {s["station_id"]: 15.0 + random.random() for s in self.stations}

    def ensure_topic(self, partitions=4, replication_factor=1):
        if not self.admin:
            return
        try:
            topics = [NewTopic(name=self.topic, num_partitions=partitions, replication_factor=replication_factor)]
            self.admin.create_topics(new_topics=topics, validate_only=False)
            print(f"Created topic {self.topic}")
        except Exception as e:
            # topic exists or other error
            print(f"Topic ensure: {e}")

    def _make_record(self, station):
        sid = station["station_id"]
        prev = self.prev_temp.get(sid, 15.0)
        # small random walk
        temperature_c = prev + (random.random() * 2.0 - 1.0)
        min_c = temperature_c - random.random() * 2.0
        max_c = temperature_c + random.random() * 2.0
        pressure_hpa = 1013.0 + (random.random() * 20.0 - 10.0)
        humidity_pct = max(0.0, min(100.0, 50.0 + (random.random() * 40.0 - 20.0)))
        wind_kph = max(0.0, random.random() * 40.0)

        rec = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "station_id": sid,
            "latitude": station["lat"],
            "longitude": station["lon"],
            "temperature_c": round(temperature_c, 2),
            "min_c": round(min_c, 2),
            "max_c": round(max_c, 2),
            "pressure_hpa": round(pressure_hpa, 2),
            "humidity_pct": round(humidity_pct, 1),
            "wind_kph": round(wind_kph, 1),
        }

        # update state
        self.prev_temp[sid] = temperature_c
        return rec

    def run_continuous(self, interval_secs=1.0):
        self.ensure_topic()
        print(f"Starting continuous producer -> topic {self.topic} @ {self.bootstrap}")
        try:
            while True:
                station = random.choice(self.stations)
                rec = self._make_record(station)
                self.producer.send(self.topic, key=rec["station_id"], value=rec)
                # flush occasionally for lower memory and for interactive runs
                self.producer.flush()
                time.sleep(interval_secs)
        except KeyboardInterrupt:
            print("Stopping producer")
        finally:
            self.producer.close()

    def run_count(self, count=100, interval_secs=0.1):
        self.ensure_topic()
        print(f"Producing {count} records to {self.topic}")
        for i in range(count):
            station = random.choice(self.stations)
            rec = self._make_record(station)
            self.producer.send(self.topic, key=rec["station_id"], value=rec)
            if i % 10 == 0:
                self.producer.flush()
            time.sleep(interval_secs)
        self.producer.flush()
        self.producer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Simple weather Kafka producer")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--count", type=int, help="Produce only N messages then exit")
    parser.add_argument("--continuous", action="store_true", help="Run continuously until ctrl-c")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    p = WeatherProducer(bootstrap_servers=args.bootstrap, topic=args.topic)
    if args.continuous:
        p.run_continuous(interval_secs=args.interval)
    elif args.count:
        p.run_count(count=args.count, interval_secs=args.interval)
    else:
        # default: run 100 records then exit
        p.run_count(count=100, interval_secs=args.interval)
