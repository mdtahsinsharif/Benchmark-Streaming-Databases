import json
import time
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, WindowFunction
from pyflink.common import Types, Time, Duration
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction

# ------------------------------
# Environment
# ------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(8)

# ------------------------------
# Enable checkpoints
# ------------------------------
env.enable_checkpointing(5000)  # every 5 sec
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_min_pause_between_checkpoints(1000)
env.get_checkpoint_config().set_checkpoint_timeout(60000)

# Optional: clean old checkpoints to start a clean job
# import shutil
# shutil.rmtree("/tmp/flink-checkpoints", ignore_errors=True)

# ------------------------------
# Kafka Source (input)
# ------------------------------
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("10.1.2.104:9092")
    .set_topics("data.ingestion")
    .set_group_id("flink-benchmark")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# ------------------------------
# Parse JSON Event
# ------------------------------
class ParseEvent(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            region = data.get("region")
            amount = int(data.get("amount", 0))
            event_time = int(data.get("event_time", int(time.time() * 1000)))
            return (region, amount, event_time)
        except:
            return None

# ------------------------------
# Compute latency
# ------------------------------
class ComputeLatency(MapFunction):
    def map(self, value):
        if value is None:
            return None
        region, amount, event_time = value
        processing_time = int(time.time() * 1000)
        latency = max(processing_time - event_time, 0)
        return (region, amount, event_time, latency)

# ------------------------------
# Watermark strategy
# ------------------------------
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(2))
    .with_timestamp_assigner(lambda event, ts: event[2] if event is not None else ts)
)

# ------------------------------
# Build stream
# ------------------------------
stream = env.from_source(
    kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="KafkaSource"
)

parsed_stream = stream.map(
    ParseEvent(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

latency_stream = parsed_stream.map(
    ComputeLatency(),
    output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG(), Types.LONG()])
).filter(lambda x: x is not None and len(x) == 4)

# ------------------------------
# Windowed latency stats
# ------------------------------
class LatencyWindow(WindowFunction):
    def apply(self, key, window, inputs):
        valid_inputs = [x for x in inputs if x is not None and len(x) == 4]
        latencies = [x[3] for x in valid_inputs]
        count = len(latencies)

        if count == 0:
            yield json.dumps({
                "window_start": window.start,
                "window_end": window.end,
                "records": 0
            })
            return

        latencies.sort()
        avg_latency = sum(latencies) / count
        p50 = latencies[int(0.50 * (count - 1))]
        p95 = latencies[int(0.95 * (count - 1))]
        p99 = latencies[int(0.99 * (count - 1))]

        yield json.dumps({
            "window_start": datetime.fromtimestamp(window.start / 1000).isoformat(),
            "window_end": datetime.fromtimestamp(window.end / 1000).isoformat(),
            "records": count,
            "avg_latency_ms": avg_latency,
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99
        })

windowed_latency_stream = (
    latency_stream
    .key_by(lambda x: x[0])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply(LatencyWindow(), output_type=Types.STRING())
)

windowed_latency_stream.print("WINDOW_STATS")

# ------------------------------
# Kafka Sink (output) with transaction timeout
# ------------------------------
kafka_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("10.1.2.104:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("flink.output")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .set_property("transaction.timeout.ms", "900000")  # 15 min
    .build()
)

windowed_latency_stream.sink_to(kafka_sink)

# ------------------------------
# Execute
# ------------------------------
env.execute("Kafka_Flink_Windowed_Latency_Throughput")