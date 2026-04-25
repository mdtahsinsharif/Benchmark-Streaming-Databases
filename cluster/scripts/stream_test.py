from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp

spark = SparkSession.builder.appName("StreamWindowWatermarkFixed").getOrCreate()

# 1. Read streaming data from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "10.1.2.104") \
    .option("port", 9999) \
    .load()

# 2. Create words
words = lines.select(
    explode(split("value", " ")).alias("word")
)

# 3. IMPORTANT: assign event-time (required for watermark)
# NOTE: socket has no event-time, so we simulate it safely
words_with_time = words.withColumn("event_time", current_timestamp())

# 4. Windowed aggregation with watermark
windowed_counts = words_with_time \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        window("event_time", "10 seconds", "5 seconds"),
        "word"
    ) \
    .count()

# 5. Streaming query
query = windowed_counts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()