from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, unix_timestamp,
    window, count, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time, json

# --------------------------------------------------
# Function to log streaming metrics
# --------------------------------------------------
def log_streaming_metrics(query, log_file="stream_metrics.json"):
    """
    Collect Spark Structured Streaming internal metrics every 5 seconds.
    """
    import threading

    def _logger():
        while query.isActive:
            progress = query.lastProgress
            if progress is not None:
                metrics = {
                    "timestamp": progress["timestamp"],
                    "batchId": progress["batchId"],
                    "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                    "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                    "numInputRows": progress.get("numInputRows"),
                    "batchDurationMs": progress.get("batchDuration"),
                    "schedulerDelayMs": progress.get("durationMs", {}).get("schedulerDelay"),
                    "processingDelayMs": progress.get("durationMs", {}).get("processingDelay"),
                    "totalLatencyEstimateMs": progress.get("durationMs", {}).get("totalDuration"),
                }
                print("\nStreaming Metrics:")
                print(json.dumps(metrics, indent=2))

                with open(log_file, "a") as f:
                    f.write(json.dumps(metrics) + "\n")
            time.sleep(5)

    t = threading.Thread(target=_logger, daemon=True)
    t.start()


# --------------------------------------------------
# Spark Session with parallelism
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("StructuredStreamingBenchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.default.parallelism", 10)
spark.conf.set("spark.sql.shuffle.partitions", 10)


# --------------------------------------------------
# Schema Definition
# --------------------------------------------------
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("region", StringType()),
    StructField("event_time", LongType())  # in ms
])


# --------------------------------------------------
# Kafka Stream Source
# --------------------------------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "latest") \
    .load()


# --------------------------------------------------
# Parse JSON Payload
# --------------------------------------------------
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# --------------------------------------------------
# Compute Processing Time + Per-record Latency
# --------------------------------------------------
processed_df = parsed_df.withColumn(
    "processing_time", (unix_timestamp(current_timestamp()) * 1000).cast("long")
).withColumn(
    "latency_ms", col("processing_time") - col("event_time")
)


# --------------------------------------------------
# Percentile Latency (P50, P95, P99) per 5-second window
# --------------------------------------------------
latency_percentiles_df = processed_df.groupBy(
    window(col("processing_time").cast("timestamp"), "5 seconds")
).agg(
    expr("percentile_approx(latency_ms, 0.5)").alias("p50_latency_ms"),
    expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
    expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency_ms"),
    count("*").alias("records_in_window")
)


# --------------------------------------------------
# Throughput (records processed per 5-second window)
# --------------------------------------------------
throughput_df = processed_df.groupBy(
    window(col("processing_time").cast("timestamp"), "5 seconds")
).agg(
    count("*").alias("records_processed")
)


# --------------------------------------------------
# Output Streams
# --------------------------------------------------
latency_query = latency_percentiles_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

throughput_query = throughput_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()


# --------------------------------------------------
# Start Metrics Logging
# --------------------------------------------------
log_streaming_metrics(latency_query, "metrics/latency_metrics.json")
log_streaming_metrics(throughput_query, "metrics/throughput_metrics.json")


# --------------------------------------------------
# Await Termination
# --------------------------------------------------
latency_query.awaitTermination()
throughput_query.awaitTermination()