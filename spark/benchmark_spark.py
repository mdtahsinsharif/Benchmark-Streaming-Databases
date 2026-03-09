from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    window, count, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time, json


# --------------------------------------------------
# Function to log streaming metrics
# --------------------------------------------------
def log_streaming_metrics(query, log_file="stream_metrics.json"):
    import threading

    def _logger():
        while query.isActive:
            progress = query.lastProgress
            if progress is not None:

                metrics = {
                    "timestamp": progress["timestamp"],
                    "batchId": progress["batchId"],
                    "numInputRows": progress.get("numInputRows"),
                    "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                    "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                    "batchDurationMs": progress.get("batchDuration"),
                    "schedulerDelayMs": progress.get("durationMs", {}).get("schedulerDelay"),
                    "processingDelayMs": progress.get("durationMs", {}).get("processingDelay"),
                    "totalDurationMs": progress.get("durationMs", {}).get("totalDuration")
                }

                print("\nStreaming Metrics:")
                print(json.dumps(metrics, indent=2))

                with open(log_file, "a") as f:
                    f.write(json.dumps(metrics) + "\n")

            time.sleep(5)

    t = threading.Thread(target=_logger, daemon=True)
    t.start()


# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkStreamingBenchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# parallelism similar to Flink job
spark.conf.set("spark.default.parallelism", 10)
spark.conf.set("spark.sql.shuffle.partitions", 10)


# --------------------------------------------------
# Schema
# --------------------------------------------------
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("region", StringType()),
    StructField("event_time", LongType())
])


# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()


# --------------------------------------------------
# Parse JSON
# --------------------------------------------------
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# --------------------------------------------------
# Processing time + latency
# --------------------------------------------------
processed_df = parsed_df.withColumn(
    "processing_time",
    (current_timestamp().cast("double") * 1000).cast("long")
).withColumn(
    "latency_ms",
    col("processing_time") - col("event_time")
)


# --------------------------------------------------
# 5 second window metrics
# --------------------------------------------------
metrics_df = processed_df.groupBy(
    window((col("processing_time")/1000).cast("timestamp"), "5 seconds")
).agg(

    count("*").alias("records"),

    expr("percentile_approx(latency_ms, 0.5)").alias("p50_latency_ms"),
    expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
    expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency_ms"),

    expr("avg(latency_ms)").alias("avg_latency_ms")
).withColumn(
    "throughput_rps",
    col("records") / 5
)


# --------------------------------------------------
# Output Stream
# --------------------------------------------------
query = metrics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="100 milliseconds") \
    .start()


# --------------------------------------------------
# Log internal Spark metrics
# --------------------------------------------------
log_streaming_metrics(query, "metrics/spark_stream_metrics.json")


# --------------------------------------------------
# Await termination
# --------------------------------------------------
query.awaitTermination()