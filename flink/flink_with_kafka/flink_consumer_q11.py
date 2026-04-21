import json
import time
import os
import hashlib
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Time, Duration
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.functions import MapFunction, WindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors import FileSink
from pyflink.common.serialization import Encoder


# --------------------------------------------------
# Environment
# --------------------------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(8)


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
# Parse JSON Event
# --------------------------------------------------
class ParseEvent(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            return (
                data.get("region"),
                int(data.get("amount", 0)),
                int(data.get("event_time", int(time.time() * 1000)))
            )
        except:
            return None


# --------------------------------------------------
# Complexity layer 1: CPU-heavy hashing (like Spark UDF)
# --------------------------------------------------
class HeavyCompute(MapFunction):
    def map(self, value):
        if value is None:
            return None

        region, amount, event_time = value

        data = f"{region}-{amount}-{event_time}".encode()

        # 🔥 CPU amplification (tune this knob)
        for _ in range(300):
            data = hashlib.sha256(data).digest()

        return (region, amount, event_time, data.hex())


# --------------------------------------------------
# Compute latency (same definition as Spark)
# --------------------------------------------------
class ComputeLatency(MapFunction):
    def map(self, value):
        if value is None:
            return None

        region, amount, event_time, _ = value

        processing_time = int(time.time() * 1000)
        latency = processing_time - event_time

        return (region, amount, event_time, latency)


# --------------------------------------------------
# Watermark Strategy
# --------------------------------------------------
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(2))
    .with_timestamp_assigner(lambda e, ts: e[2])
)


# --------------------------------------------------
# Build Stream
# --------------------------------------------------
stream = env.from_source(
    kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="KafkaSource"
)

parsed_stream = stream.map(
    ParseEvent(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# 🔥 Added complexity stage (CPU + serialization)
heavy_stream = parsed_stream.map(
    HeavyCompute(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.STRING()])
)

latency_stream = heavy_stream.map(
    ComputeLatency(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.LONG()])
).filter(lambda x: x is not None)


# --------------------------------------------------
# Windowed Throughput + Latency Stats
# --------------------------------------------------
class LatencyWindow(WindowFunction):
    def apply(self, key, window, inputs):

        valid = [x for x in inputs if x is not None]
        latencies = [x[3] for x in valid]
        count = len(latencies)

        if count == 0:
            yield f"window_start={window.start}, window_end={window.end}, records=0"
            return

        latencies.sort()

        avg_latency = sum(latencies) / count
        p50 = latencies[int(0.50 * (count - 1))]
        p95 = latencies[int(0.95 * (count - 1))]
        p99 = latencies[int(0.99 * (count - 1))]

        start_human = datetime.fromtimestamp(window.start / 1000)
        end_human = datetime.fromtimestamp(window.end / 1000)

        yield (
            f"window_start={start_human}, window_end={end_human}, "
            f"records={count}, "
            f"avg_latency_ms={avg_latency:.2f}, "
            f"p50_ms={p50}, p95_ms={p95}, p99_ms={p99}"
        )


windowed_latency_stream = (
    latency_stream
    .key_by(lambda x: x[0])  # region
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply(LatencyWindow(), output_type=Types.STRING())
)


# --------------------------------------------------
# Output (console)
# --------------------------------------------------
windowed_latency_stream.print("WINDOW_STATS")


# --------------------------------------------------
# File sink (for plotting / benchmarking)
# --------------------------------------------------
def get_output_folder():
    timestamp_str = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    folder = f"latency_output/{timestamp_str}"
    os.makedirs(folder, exist_ok=True)
    return folder


folder_name = get_output_folder()

latency_sink = FileSink.for_row_format(
    folder_name,
    Encoder.simple_string_encoder()
).build()

windowed_latency_stream.sink_to(latency_sink)


# --------------------------------------------------
# Execute
# --------------------------------------------------
env.execute("Kafka_Flink_Complexity_Benchmark")