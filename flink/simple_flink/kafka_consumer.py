from kafka import KafkaConsumer
import json

# Kafka config
KAFKA_BROKER = 'localhost:9092'   # replace with your broker IP
TOPIC_NAME = 'sensor_topic'       # replace with your topic
GROUP_ID = 'test-group'

# Create a consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=GROUP_ID,
    auto_offset_reset='earliest',  # start from oldest messages
    value_deserializer=lambda v: v.decode('utf-8')  # decode bytes to string
)

print(f"Listening to Kafka topic: {TOPIC_NAME}...")

# Consume messages
for message in consumer:
    print(f"Received: {message.value}")
