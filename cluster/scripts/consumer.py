from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'flink.output',
    bootstrap_servers='10.1.2.104:9092',
    auto_offset_reset='earliest',
    group_id='flink-output-consumer',
    enable_auto_commit=True
)

print("Listening to 'flink.output'...")

for message in consumer:
    print(message.value.decode('utf-8'))