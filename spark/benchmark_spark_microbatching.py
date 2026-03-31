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
# --------------------------------------------------
# Function to log streaming metrics + window stats
# --------------------------------------------------
def log_streaming_metrics_per_batch(query, spark, log_file="metrics/spark_stream_metrics.json"):
    import threading

    def _logger():
        last_logged_batch = -1
        while query.isActive:
            progress = query.lastProgress
            if progress is not None:
                batch_id = progress["batchId"]
                if batch_id != last_logged_batch:  # only log new batches
                    last_logged_batch = batch_id
                    base_metrics = {
                        "timestamp": progress["timestamp"],
                        "batchId": batch_id,
                        "numInputRows": progress.get("numInputRows"),
                        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                        "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                        "batchDurationMs": progress.get("batchDuration"),
                        "schedulerDelayMs": progress.get("durationMs", {}).get("schedulerDelay"),
                        "processingDelayMs": progress.get("durationMs", {}).get("processingDelay"),
                        "totalDurationMs": progress.get("durationMs", {}).get("totalDuration")
                    }

                    # Windowed metrics from memory sink
                    try:
                        window_metrics = spark.sql("""
                            SELECT *
                            FROM metrics_table
                            ORDER BY window DESC
                            LIMIT 1
                        """).collect()

                        if window_metrics:
                            row = window_metrics[0]
                            base_metrics.update({
                                "records": row.records,
                                "p50_latency_ms": row.p50_latency_ms,
                                "p95_latency_ms": row.p95_latency_ms,
                                "p99_latency_ms": row.p99_latency_ms,
                                "avg_latency_ms": row.avg_latency_ms,
                                "throughput_rps": row.throughput_rps
                            })
                    except Exception as e:
                        base_metrics["window_metrics_error"] = str(e)

                    # Print + write to file
                    #print("\nStreaming Metrics (combined):")
                    #print(json.dumps(base_metrics, indent=2))
                    with open(log_file, "a") as f:
                        f.write(json.dumps(base_metrics) + "\n")

    threading.Thread(target=_logger, daemon=True).start()



# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkStreamingBenchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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
    .format("memory") \
    .queryName("metrics_table") \
    .outputMode("complete") \
    .trigger(processingTime="100 milliseconds") \
    .start()


# --------------------------------------------------
# Log internal Spark metrics
# --------------------------------------------------
log_streaming_metrics_per_batch(query, spark, "metrics/spark_stream_metrics.json")

# --------------------------------------------------
# Await termination
# --------------------------------------------------
query.awaitTermination()