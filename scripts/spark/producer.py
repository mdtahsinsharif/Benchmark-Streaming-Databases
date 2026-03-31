import json
import time
from kafka import KafkaProducer
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",

    # serialization
    value_serializer=lambda v: json.dumps(v).encode(),

    # batching
    batch_size=65536,
    linger_ms=5,

    # compression increases throughput
    compression_type="lz4",

    # large buffers
    buffer_memory=67108864
)

regions = ["north", "south", "east", "west"]

# adjust this to control the throughput
messages_per_second = 100  # desired message rate per second
delay = 0.01 / messages_per_second

while True:
    now = int(time.time() * 1000)
    event = {
        "region": random.choice(regions),
        "amount": random.randint(1, 100),
        "event_time": now
    }

    producer.send("sales", event)

    # throttle to avoid Kafka backpressure
    time.sleep(delay)