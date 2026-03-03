from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction

# -------------------------------
# Custom window function
# -------------------------------
class WindowStats(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        total = 0
        count = 0
        for e in elements:
            try:
                total += float(e)
                count += 1
            except ValueError:
                continue
        avg = total / count if count > 0 else 0
        # send result downstream
        out.collect(f"key={key}, count={count}, avg={avg}")

# -------------------------------
# Environment setup
# -------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# -------------------------------
# Kafka consumer
# -------------------------------
kafka_props = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'pyflink_group',
    'auto.offset.reset': 'earliest'
}

consumer = FlinkKafkaConsumer(
    topics='sensor_topic',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

# -------------------------------
# Data stream + processing time window
# -------------------------------
stream = env.add_source(consumer)  # <- no Types.STRING()

stream.key_by(lambda x: "dummy_key") \
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
      .process(WindowStats()) \
      .print()

# -------------------------------
# Execute
# -------------------------------
env.execute("Sensor Kafka Stream Job")
