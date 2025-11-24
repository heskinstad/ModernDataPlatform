from confluent_kafka import Consumer, KafkaException

bootstrap_servers = "localhost:29092,localhost:29093,localhost:29094,localhost:29095"
topic = "weather-data"

# Configure the consumer
c = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'cap-demo-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the topic (must be a list!)
c.subscribe([topic])

print("🧩 Listening for messages... (Ctrl+C to stop)\n")

try:
    while True:
        msg = c.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(f"📩 Received: {msg.value().decode()} (partition {msg.partition()}, offset {msg.offset()})")
except KeyboardInterrupt:
    pass
finally:
    c.close()
    print("👋 Consumer closed.")

