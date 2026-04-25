#!/usr/bin/env python3

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ---------------- PATH FIX ----------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_ROOT = os.path.dirname(SCRIPT_DIR)

if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)

from nexmark_spark_generator import build_nexmark_batch, build_nexmark_stream


# ---------------- ARGS ----------------
def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--master", default=None)
    p.add_argument("--rows-per-second", "--tps", dest="rows_per_second", type=int, default=200)
    p.add_argument("--num-partitions", type=int, default=4)

    p.add_argument("--person-proportion", type=int, default=1)
    p.add_argument("--auction-proportion", type=int, default=3)
    p.add_argument("--bid-proportion", type=int, default=46)

    p.add_argument("--max-events", type=int, default=None)

    p.add_argument("--query-name", default="nexmark_q11")
    p.add_argument("--checkpoint-location", default="/srv/nfs/spark/checkpoints")
    p.add_argument("--output-path", default="/srv/nfs/spark/nexmark/normal/q11")

    p.add_argument("--sink-format", choices=("csv", "console", "parquet"), default="csv")

    p.add_argument("--run-seconds", type=int, default=60)
    p.add_argument("--trigger-processing-time", default="5 seconds")

    return p.parse_args()


# ---------------- Q11 (STATELESS VERSION) ----------------
def build_q11_stateless(events_df):
    """
    Q11 approximated as Q1-style projection.
    Removes:
      - session windows
      - watermark
      - aggregations

    This makes it:
      - stateless
      - stable streaming
      - comparable to Q1 latency
    """

    return (
        events_df
        .where(col("event_type") == 2)
        .select(
            col("bid.auction").alias("auction"),
            col("bid.bidder").alias("bidder"),
            col("bid.price").alias("price"),
            col("bid.dateTime").alias("dateTime"),
            col("bid.url").alias("url"),
            col("bid.extra").alias("extra"),
        )
        .withColumn("processed_time", current_timestamp())
    )


# ---------------- MAIN ----------------
def main():

    args = parse_args()

    spark = (
        SparkSession.builder.appName("nexmark_q11_stateless")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.num_partitions))
        .config("spark.default.parallelism", str(args.num_partitions))
        .getOrCreate()
    )

    # ---------------- INPUT ----------------
    if args.max_events is not None:
        df = build_nexmark_batch(spark, args)
    else:
        df = build_nexmark_stream(spark, args)

    result = build_q11_stateless(df)

    checkpoint = f"{args.checkpoint_location}/{args.query_name}"

    writer = (
        result.writeStream
        .queryName(args.query_name)
        .format(args.sink_format)
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("maxRecordsPerFile", 100000)
    )

    # ---------------- OUTPUT CONFIG ----------------
    if args.sink_format == "csv":
        writer = (
            writer.option("path", args.output_path)
            .option("header", "true")
        )

    elif args.sink_format == "parquet":
        writer = writer.option("path", args.output_path)

    elif args.sink_format == "console":
        writer = writer.option("truncate", False).option("numRows", 20)

    query = writer.trigger(
        processingTime=args.trigger_processing_time
    ).start()

    query.awaitTermination(args.run_seconds)

    query.stop()


if __name__ == "__main__":
    main()