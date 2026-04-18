from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp
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
                    "timestamp": progress.get("timestamp"),
                    "numInputRows": progress.get("numInputRows"),
                    "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                    "processedRowsPerSecond": progress.get("processedRowsPerSecond")
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
    .appName("SparkContinuousStreamingBenchmark") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
#spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.default.parallelism", 10)


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
processed_df = raw_df.selectExpr(
    "CAST(value AS STRING)",
    "timestamp"
).select(
    from_json(col("value"), schema).alias("data"),
    col("timestamp")
).select(
    "data.*",
    (col("timestamp").cast("double") * 1000).cast("long").alias("processing_time")
).withColumn(
    "latency_ms",
    col("processing_time") - col("event_time")
)


# --------------------------------------------------
# Output Stream (continuous mode)
# --------------------------------------------------
query = processed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .trigger(continuous="1 second") \
    .start()

'''query = processed_df.writeStream \
    .format("json") \
    .option("path", "metrics") \
    .option("checkpointLocation", "metrics/spark_checkpoint") \
    .outputMode("append") \
    .trigger(continuous="1 second") \
    .start()
'''

# --------------------------------------------------
# Log internal Spark metrics
# --------------------------------------------------
log_streaming_metrics(query, "metrics/spark_stream_metrics.json")


# --------------------------------------------------
# Await termination
# --------------------------------------------------
query.awaitTermination()