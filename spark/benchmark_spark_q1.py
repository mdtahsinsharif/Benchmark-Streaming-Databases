from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    window, count, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import json, threading

# --------------------------------------------------
# Metrics logger
# --------------------------------------------------
def log_streaming_metrics_per_batch(query, spark, log_file="metrics/spark_stream_metrics.json"):
    def _logger():
        last_logged_batch = -1

        while query.isActive:
            progress = query.lastProgress

            if progress is not None:
                batch_id = progress["batchId"]

                if batch_id != last_logged_batch:
                    last_logged_batch = batch_id

                    base_metrics = {
                        "timestamp": progress.get("timestamp"),
                        "batchId": batch_id,
                        "numInputRows": progress.get("numInputRows"),
                        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                        "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                    }

                    # window metrics from memory sink
                    try:
                        rows = spark.sql("""
                            SELECT * FROM metrics_table
                            ORDER BY window DESC
                            LIMIT 1
                        """).collect()

                        if rows:
                            r = rows[0]
                            base_metrics.update({
                                "records": r.records,
                                "p50_latency_ms": r.p50_latency_ms,
                                "p95_latency_ms": r.p95_latency_ms,
                                "p99_latency_ms": r.p99_latency_ms,
                                "avg_latency_ms": r.avg_latency_ms,
                                "throughput_rps": r.throughput_rps
                            })
                    except Exception as e:
                        base_metrics["window_error"] = str(e)

                    with open(log_file, "a") as f:
                        f.write(json.dumps(base_metrics) + "\n")

    threading.Thread(target=_logger, daemon=True).start()


# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkStreaming_200ms_Benchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.default.parallelism", 3)
spark.conf.set("spark.sql.shuffle.partitions", 3)

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
# 200ms window metrics (THIS IS THE KEY CHANGE)
# --------------------------------------------------
metrics_df = processed_df.groupBy(
    window(
        (col("processing_time") / 1000).cast("timestamp"),
        "200 milliseconds"
    )
).agg(
    count("*").alias("records"),

    expr("percentile_approx(latency_ms, 0.5)").alias("p50_latency_ms"),
    expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
    expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency_ms"),

    expr("avg(latency_ms)").alias("avg_latency_ms")
).withColumn(
    "throughput_rps",
    col("records") / 0.2   # 200ms = 0.2s
)

# --------------------------------------------------
# Output sink (memory for debugging/analysis)
# --------------------------------------------------
query = metrics_df.writeStream \
    .format("memory") \
    .queryName("metrics_table") \
    .outputMode("complete") \
    .trigger(processingTime="100 milliseconds") \
    .start()

# --------------------------------------------------
# Start logger
# --------------------------------------------------
log_streaming_metrics_per_batch(
    query,
    spark,
    "metrics/spark_stream_metrics.json"
)

# --------------------------------------------------
# Run
# --------------------------------------------------
query.awaitTermination()