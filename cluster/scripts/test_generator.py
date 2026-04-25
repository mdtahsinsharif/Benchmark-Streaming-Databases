#!/usr/bin/env python3

import argparse
from argparse import Namespace

from pyspark.sql import SparkSession

# import your generator function
from nexmark_spark_generator import build_nexmark_stream


def parse_args():
    parser = argparse.ArgumentParser(description="Test Nexmark generator")

    parser.add_argument("--tps", type=int, default=10)
    parser.add_argument("--num-partitions", type=int, default=None)
    parser.add_argument("--trigger-processing-time", default="1 second")

    return parser.parse_args()


def main():
    cli_args = parse_args()

    spark = (
        SparkSession.builder
        .appName("nexmark_generator_test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )

    # ✅ FIX: build proper args object expected by generator
    args = Namespace(
        rows_per_second=cli_args.tps,
        num_partitions=cli_args.num_partitions,
        person_proportion=1,
        auction_proportion=3,
        bid_proportion=46,
        sink_format="console",
        output_path=None,
        checkpoint_location=None,
        query_name="nexmark_test",
        master=None,
        truncate="false",
        trigger_processing_time=cli_args.trigger_processing_time,
        run_seconds=None,
        max_events=None,
    )

    # 🔥 build stream from your generator
    events = build_nexmark_stream(spark, args)

    # OPTIONAL: keep only bids for readability
    events = (
        events.where("event_type = 2")
        .select("bid.*", "dateTime")
    )

    # 🔥 console sink with proper streaming trigger
    query = (
        events.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .outputMode("append")
        .trigger(processingTime=cli_args.trigger_processing_time)
        .start()
    )

    print("Streaming started... Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()