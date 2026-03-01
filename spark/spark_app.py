from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, sum as sum_, from_json, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StructuredStreamingKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Define schema for JSON data
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("region", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .load()

# Parse JSON from Kafka value
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Process stream: filter and aggregate
filtered_df = parsed_df.filter(col("amount") > 150)
aggregated_df = filtered_df.groupBy("region").agg(sum_("amount").alias("total_amount"),count("*").alias("region_occurrence_count"))

# Write to console
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for termination
query.awaitTermination()
