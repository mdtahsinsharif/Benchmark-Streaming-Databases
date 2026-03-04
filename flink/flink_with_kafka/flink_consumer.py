import json
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ReduceFunction
#import pyflink
#print(pyflink.__version__)


# --------------------------------------------------
# Environment
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

#env.add_jars(
#    "file:///mnt/c/Users/shari/OneDrive/Desktop/tahsin/uoft/ece1724/project/spark_local/flink/flink-connector-kafka_2.12-1.14.6.jar"
#)

env.set_parallelism(1)

# --------------------------------------------------
# Kafka Source (Flink 2.x API)
# --------------------------------------------------

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("localhost:9092")
    .set_topics("sales")
    .set_group_id("flink-benchmark")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Watermark Strategy (event_time is increasing)
watermark_strategy = (
    WatermarkStrategy
    .for_monotonous_timestamps()
)

stream = env.from_source(
    kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="KafkaSource"
)

# --------------------------------------------------
# Parse JSON + Extract Event Time
# --------------------------------------------------

class ParseEvent(MapFunction):
    def map(self, value):
        data = json.loads(value)
        return (
            data["region"],
            data["amount"],
            data["event_time"]
        )

parsed = stream.map(
    ParseEvent(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# --------------------------------------------------
# E2E Latency (Measured at Sink Operator)
# --------------------------------------------------

class ComputeLatency(ProcessFunction):
    def process_element(self, value, ctx):
        region, amount, event_time = value
        sink_time = int(time.time() * 1000)
        latency = sink_time - event_time

        yield (
            region,
            amount,
            latency
        )

latency_stream = parsed.process(
    ComputeLatency(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# --------------------------------------------------
# Throughput (5 second window)
# --------------------------------------------------
'''
throughput = (
    parsed
    .map(lambda x: 1, output_type=Types.INT())
    .window_all(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .sum(0)
)'''

class CountReduce(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2

throughput = (
    parsed
    .map(lambda x: 1, output_type=Types.INT())
    .window_all(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .reduce(CountReduce(), output_type=Types.INT())
)

# --------------------------------------------------
# Output
# --------------------------------------------------

latency_stream.print("E2E_LATENCY")
throughput.print("THROUGHPUT_5S")

env.execute("Flink_2_2_E2E_Latency")
