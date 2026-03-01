import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

regions = ["US", "EU", "ASIA"]

while True:
    data = {
        "order_id": random.randint(1, 1000000),
        "amount": random.randint(50, 500),
        "region": random.choice(regions),
        "event_time": int(time.time() * 1000)
    }

    producer.send("sales", data)
    print("Sent:", data)

    time.sleep(0.05)   # control throughput rate
