from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    count,
    expr,
    session_window
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

import json

# --------------------------------------------------
# Logger (FIXED for session windows)
# --------------------------------------------------
def log_streaming_metrics_per_batch(
    query,
    spark,
    log_file="metrics/spark_stream_metrics.json"
):
    import threading

    def _logger():
        last_logged_batch = -1

        while query.isActive:
            progress = query.lastProgress

            if progress is not None:
                batch_id = progress["batchId"]

                if batch_id != last_logged_batch:
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
                        "totalDurationMs": progress.get("durationMs", {}).get("totalDuration"),
                    }

                    try:
                        rows = spark.sql("""
                            SELECT *
                            FROM metrics_table
                            ORDER BY endtime DESC
                            LIMIT 1
                        """).collect()

                        if rows:
                            row = rows[0]

                            base_metrics.update({
                                "bidder": row.bidder,
                                "bid_count": row.bid_count,
                                "starttime": str(row.starttime),
                                "endtime": str(row.endtime),
                                "p50_latency_ms": row.p50_latency_ms,
                                "p95_latency_ms": row.p95_latency_ms,
                                "p99_latency_ms": row.p99_latency_ms,
                                "avg_latency_ms": row.avg_latency_ms,
                                "throughput_rps": row.throughput_rps,
                            })

                    except Exception as e:
                        base_metrics["window_metrics_error"] = str(e)

                    with open(log_file, "a") as f:
                        f.write(json.dumps(base_metrics) + "\n")

    threading.Thread(target=_logger, daemon=True).start()


# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SparkStreaming_Q11_SessionWindows")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.default.parallelism", 8)
spark.conf.set("spark.sql.shuffle.partitions", 8)


# --------------------------------------------------
# Schema
# --------------------------------------------------
schema = StructType([
    StructField("amount", IntegerType()),
    StructField("region", StringType()),  # used as bidder
    StructField("event_time", LongType())  # epoch ms
])


# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100000)
    .load()
)


# --------------------------------------------------
# Parse JSON
# --------------------------------------------------
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)


# --------------------------------------------------
# Add processing time + latency
# --------------------------------------------------
processed_df = (
    parsed_df
    .withColumn(
        "processing_time",
        (current_timestamp().cast("double") * 1000).cast("long")
    )
    .withColumn(
        "latency_ms",
        col("processing_time") - col("event_time")
    )
)


# --------------------------------------------------
# Event-time column for session window
# --------------------------------------------------
processed_df = processed_df.withColumn(
    "event_ts",
    (col("event_time") / 1000).cast("timestamp")
)


# --------------------------------------------------
# Watermark (required)
# --------------------------------------------------
processed_df = processed_df.withWatermark("event_ts", "30 seconds")


# --------------------------------------------------
# Session Window Aggregation (Q11 equivalent)
# --------------------------------------------------
session_df = (
    processed_df
    .groupBy(
        col("region").alias("bidder"),
        session_window(col("event_ts"), "10 seconds")
    )
    .agg(
        count("*").alias("bid_count"),
        expr("percentile_approx(latency_ms, 0.5)").alias("p50_latency_ms"),
        expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
        expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency_ms"),
        expr("avg(latency_ms)").alias("avg_latency_ms"),
    )
)


# --------------------------------------------------
# Compute throughput based on session duration
# --------------------------------------------------
session_df = session_df.select(
    col("bidder"),
    col("bid_count"),
    col("session_window.start").alias("starttime"),
    col("session_window.end").alias("endtime"),
    col("p50_latency_ms"),
    col("p95_latency_ms"),
    col("p99_latency_ms"),
    col("avg_latency_ms"),
    expr("""
        CASE
            WHEN (CAST(session_window.end AS LONG)
                - CAST(session_window.start AS LONG)) > 0
            THEN bid_count /
                (CAST(session_window.end AS LONG)
                - CAST(session_window.start AS LONG))
            ELSE bid_count
        END
    """).alias("throughput_rps")
)


# --------------------------------------------------
# Output Stream
# --------------------------------------------------
query = (
    session_df.writeStream
    .format("memory")
    .queryName("metrics_table")
    .outputMode("complete")
    .trigger(processingTime="100 milliseconds")
    .start()
)


# --------------------------------------------------
# Start logging
# --------------------------------------------------
log_streaming_metrics_per_batch(query, spark)


# --------------------------------------------------
# Await termination
# --------------------------------------------------
query.awaitTermination()