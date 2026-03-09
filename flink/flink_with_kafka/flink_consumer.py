import json
import time
import numpy as np

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time

from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import WindowFunction
from pyflink.datastream.functions import ReduceFunction

# --------------------------------------------------
# Environment
# --------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()

# parallel pipeline
env.set_parallelism(10)


# --------------------------------------------------
# Kafka Source
# --------------------------------------------------

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("localhost:9092")
    .set_topics("sales")
    .set_group_id("flink-benchmark")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)


# --------------------------------------------------
# Event Parser
# --------------------------------------------------

class ParseEvent(MapFunction):

    def map(self, value):

        data = json.loads(value)

        return (
            data["region"],
            data["amount"],
            data["event_time"]
        )


# --------------------------------------------------
# Watermark Strategy
# --------------------------------------------------

watermark_strategy = (
    WatermarkStrategy
    .for_monotonous_timestamps()
    .with_timestamp_assigner(lambda event, ts: event[2])
)


stream = env.from_source(
    kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="KafkaSource"
)


parsed_stream = stream.map(
    ParseEvent(),
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.INT(),
        Types.LONG()
    ])
)


# --------------------------------------------------
# Latency Calculation (Producer → Sink)
# --------------------------------------------------

class ComputeLatency(MapFunction):

    def map(self, value):

        region, amount, event_time = value

        now = int(time.time() * 1000)

        latency = now - event_time

        return latency


latency_stream = parsed_stream.map(
    ComputeLatency(),
    output_type=Types.LONG()
)


# --------------------------------------------------
# Percentile Latency Window
# --------------------------------------------------

class PercentileWindow(WindowFunction):

    def apply(self, key, window, inputs):

        # inputs is a list of ("lat", latency)
        latencies = [x[1] for x in inputs]  # <-- extract numbers

        if len(latencies) == 0:
            return

        arr = np.array(latencies, dtype=np.int64)

        p50 = int(np.percentile(arr, 50))
        p95 = int(np.percentile(arr, 95))
        p99 = int(np.percentile(arr, 99))

        yield f"count={len(arr)} p50={p50}ms p95={p95}ms p99={p99}ms"


latency_stats = (
    latency_stream
    .map(lambda x: ("lat", x),
         output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
    .key_by(lambda x: x[0])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply(PercentileWindow())
)


# --------------------------------------------------
# Throughput Measurement
# --------------------------------------------------
'''
throughput_stream = (
    parsed_stream
    .map(lambda x: ("count", 1),
         output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    .key_by(lambda x: x[0])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .sum(1)
)'''
class SumReduce(ReduceFunction):
    def reduce(self, value1, value2):
        # value = ("count", 1)
        return (value1[0], value1[1] + value2[1])


throughput_stream = (
    parsed_stream
    .map(lambda x: ("count", 1),
         output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    .key_by(lambda x: x[0])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .reduce(SumReduce())
)


# --------------------------------------------------
# Output
# --------------------------------------------------

latency_stats.print("LATENCY_STATS")

throughput_stream.print("THROUGHPUT_5S")


# --------------------------------------------------
# Execute
# --------------------------------------------------

env.execute("Kafka_Flink_Benchmark")
