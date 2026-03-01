from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as sum_,
    count,
    from_json,
    unix_timestamp,
    current_timestamp,
    window
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time,json

def log_streaming_metrics(query, log_file="stream_metrics.json"):
    """
    Collect Spark Structured Streaming internal metrics.
    """

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

            # Append metrics to file (for benchmarking research)
            with open(log_file, "a") as f:
                f.write(json.dumps(metrics) + "\n")

        time.sleep(5)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("StructuredStreamingBenchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Schema Definition
# --------------------------------------------------

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("region", StringType()),
    StructField("event_time", LongType())
])

# --------------------------------------------------
# Kafka Stream Source
# --------------------------------------------------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "latest") \
    .load()

# --------------------------------------------------
# Parse JSON Payload
# --------------------------------------------------

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --------------------------------------------------
# Processing Timestamp + Latency Measurement
# --------------------------------------------------

processed_df = parsed_df.withColumn(
    "processing_time",
    (unix_timestamp(current_timestamp()) * 1000).cast("long")
)

processed_df = processed_df.withColumn(
    "latency_ms",
    col("processing_time") - col("event_time")
)

# --------------------------------------------------
# Business Logic
# --------------------------------------------------

# Filter high value orders
filtered_df = processed_df.filter(col("amount") > 150)

# Aggregation + Region Occurrence Count
aggregated_df = filtered_df.groupBy(
    "region"
).agg(
    sum_("amount").alias("total_amount"),
    count("*").alias("region_occurrence_count"),
    # Average latency inside aggregation window
    (sum_("latency_ms") / count("*")).alias("avg_latency_ms")
)

# --------------------------------------------------
# Throughput Measurement (5 second microbatch window)
# --------------------------------------------------

throughput_df = processed_df.groupBy(
    window(col("processing_time").cast("timestamp"), "5 seconds")
).agg(
    count("*").alias("records_processed")
)

# --------------------------------------------------
# Output Sink (Console Benchmark Output)
# --------------------------------------------------

query1 = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = throughput_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

# --------------------------------------------------
# Stream Execution
# --------------------------------------------------
log_streaming_metrics(query1,"metrics/stream_metrics1.json")
#log_streaming_metrics(query2,"metrics/stream_metrics2.json")
query1.awaitTermination()
query2.awaitTermination()
