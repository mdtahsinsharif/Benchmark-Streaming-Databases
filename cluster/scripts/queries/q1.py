#!/usr/bin/env python3

import argparse
import os
import sys
from argparse import Namespace

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# ---------------- PATH FIX ----------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_ROOT = os.path.dirname(SCRIPT_DIR)
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)

from nexmark_spark_generator import build_nexmark_batch, build_nexmark_stream


# ---------------- ARG PARSER ----------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Nexmark q1 on Spark.")

    parser.add_argument("--master", default=None)

    parser.add_argument(
        "--rows-per-second",
        "--tps",
        dest="rows_per_second",
        type=int,
        default=1000,
    )

    parser.add_argument("--num-partitions", type=int, default=1)

    # 🔥 MUST match q0 (generator behavior depends on this)
    parser.add_argument("--person-proportion", type=int, default=1)
    parser.add_argument("--auction-proportion", type=int, default=3)
    parser.add_argument("--bid-proportion", type=int, default=46)

    parser.add_argument("--max-events", type=int, default=None)

    parser.add_argument("--query-name", default="nexmark_q1")

    parser.add_argument(
        "--checkpoint-location",
        default="/srv/nfs/spark/checkpoints",
    )

    parser.add_argument("--truncate", choices=("true", "false"), default="false")

    parser.add_argument("--trigger-processing-time", default=None)

    parser.add_argument("--run-seconds", type=int, default=None)

    parser.add_argument(
        "--sink-format",
        choices=("noop", "console", "csv", "parquet"),
        default="csv",
    )

    parser.add_argument(
        "--output-path",
        default="/srv/nfs/spark/nexmark/normal/q1",
    )

    return parser.parse_args()


# ---------------- Q1 PROJECTION ----------------
def build_q1_projection(events_df):
    return (
        events_df.where("event_type = 2")
        .select(
            "bid.auction",
            "bid.bidder",
            (col("bid.price") * 0.908).alias("price"),  # ONLY CHANGE vs q0
            "bid.dateTime",
            "bid.extra",
        )
        .withColumn("processed_time", current_timestamp())
    )


# ---------------- MAIN ----------------
def main() -> None:
    args = parse_args()

    builder = SparkSession.builder.appName("nexmark_q1_latency")

    if args.master:
        builder = builder.master(args.master)

    spark = (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.num_partitions))
        .config("spark.default.parallelism", str(args.num_partitions))
        .getOrCreate()
    )

    # ======================================================
    # BATCH MODE (IDENTICAL TO q0)
    # ======================================================
    if args.max_events is not None:
        events_df = build_nexmark_batch(spark, args)
        result_df = build_q1_projection(events_df)
        result_df.show(result_df.count(), truncate=args.truncate == "true")
        return

    # ======================================================
    # STREAMING MODE (IDENTICAL TO q0)
    # ======================================================
    events_df = build_nexmark_stream(spark, args)
    result_df = build_q1_projection(events_df)

    checkpoint = args.checkpoint_location   # EXACTLY SAME AS q0

    spark.conf.set("spark.sql.streaming.fileSink.log.compactInterval", "1000")
    spark.conf.set("spark.sql.streaming.fileSink.log.cleanupDelay", "60000")

    writer = (
        result_df.writeStream
        .queryName(args.query_name)
        .format(args.sink_format)
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("maxRecordsPerFile", 100000)
    )

    if args.sink_format == "console":
        writer = writer.option("truncate", args.truncate).option("numRows", 20)

    elif args.sink_format in {"csv", "parquet"}:
        writer = writer.option("path", args.output_path)

        if args.sink_format == "csv":
            writer = (
                writer.option("header", "true")
                .option("sep", ",")
                .option("quote", "")
                .option("escape", "")
            )

    trigger = args.trigger_processing_time or "5 seconds"
    writer = writer.trigger(processingTime=trigger)

    query = writer.start()

    if args.run_seconds is not None:
        query.awaitTermination(args.run_seconds)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()