from confluent_kafka import Producer
import json
import time

bootstrap_servers = "localhost:29092,localhost:29093,localhost:29094"
topic = "weather-data"

# Load from JSON
def load_json_data(filename):
    with open(filename, "r") as file:
        data = json.load(file)
    return data

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered '{msg.value().decode()}' to partition {msg.partition()} offset {msg.offset()}")

def run_producer(acks_mode="all", delay=2.0):
    print(f"\n🚀 Starting producer with acks={acks_mode}")
    p = Producer({
        'bootstrap.servers': bootstrap_servers,
        'acks': acks_mode
        # ,'retries': 0,
        # 'delivery.timeout.ms': 1000
    })

    weather_data_list = load_json_data("../data/rdu-weather-history.json")
    
    for i in range(len(weather_data_list)):
        weather_data = weather_data_list[i]
        
        msg_value = json.dumps(weather_data)
        
        print(f"Producing: {msg_value}")
        p.produce(topic, value=msg_value.encode('utf-8'), callback=delivery_report)
        p.poll(0)
        
        # 🔸 Pause to see what happens when manually stopping a broker.
        print(f"⏳ Sleeping {delay}s — you can now stop a broker (e.g., docker stop kafka2)")
        time.sleep(delay)
    
    p.flush()
    print("🏁 Producer finished.\n")

run_producer("1", delay=20)
