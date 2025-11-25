from confluent_kafka import Producer
import json
import time
from random import random
from datetime import date, timedelta

bootstrap_servers = "localhost:29092,localhost:29093,localhost:29094"
topic = "weather-data"

def produce_weather_data(length):
    start_date = date(2025, 1, 1)
    data = []
    prevTemp = 0.0

    for i in range(1, length):
        current_date = start_date + timedelta(days=i)
        avgTemp = (random() * 6.0) - 3.0 + prevTemp
        minTemp = (random() * 10.0) - 10.0 + avgTemp
        maxTemp = (random() * 10.0) + 10.0 + avgTemp
        pressure = (random() * 30.0) - 15.0 + 1013
        data.append([current_date.isoformat(), avgTemp, minTemp, maxTemp, pressure])
        prevTemp = avgTemp

    return data

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered '{msg.value().decode()}' to partition {msg.partition()} offset {msg.offset()}")

def run_producer(delay=2.0):
    print(f"\n🚀 Starting producer")
    p = Producer({
        'bootstrap.servers': bootstrap_servers,
        'acks': "all"
    })

    weather_data_list = produce_weather_data(100)
    
    for i in range(len(weather_data_list)):
        weather_data = weather_data_list[i]
        
        msg_value = json.dumps(weather_data)
        
        print(f"Producing: {msg_value}")
        p.produce(topic, value=msg_value.encode('utf-8'), callback=delivery_report)
        p.poll(0)
        
        time.sleep(delay)
    
    p.flush()
    print("🏁 Producer finished.\n")

run_producer(delay=5)
