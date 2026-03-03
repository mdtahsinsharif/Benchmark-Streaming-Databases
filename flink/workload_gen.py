from kafka import KafkaProducer
import json
import random
import time

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize JSON
)

sensor_ids = ["sensor_1", "sensor_2", "sensor_3"]

try:
    while True:
        event = {
            "sensor_id": random.choice(sensor_ids),
            "temperature": round(random.uniform(20.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 60.0), 2),
            "timestamp": int(time.time())
        }
        producer.send("sensor_topic", value=event)
        time.sleep(0.5)  # 2 events/sec
except KeyboardInterrupt:
    producer.close()
