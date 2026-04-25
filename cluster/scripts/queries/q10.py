#!/usr/bin/env python3

import argparse
import os
import sys

from pyspark.sql import functions as F


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_ROOT = os.path.dirname(SCRIPT_DIR)
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)

from nexmark_spark_generator import build_nexmark_stream  # noqa: E402
from datetime import datetime

from query_utils import add_common_streaming_args, create_spark_session, write_partitioned_stream  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Nexmark q10 on Spark.")
    submit_time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    add_common_streaming_args(
        parser,
        "nexmark_q10",
        f"/srv/nfs/spark/nexmark/normal/q10/{submit_time}/bid",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session("nexmark_q10", args.master, args.num_partitions)
    events_df = build_nexmark_stream(spark, args)
    bid_df = events_df.where("event_type = 2").select(
        F.col("bid.auction").alias("auction"),
        F.col("bid.bidder").alias("bidder"),
        F.col("bid.price").alias("price"),
        F.col("bid.dateTime").alias("dateTime"),
        F.col("bid.extra").alias("extra"),
    )

    result_df = bid_df.select(
        F.col("auction"),
        F.col("bidder"),
        F.col("price"),
        F.col("dateTime"),
        F.col("extra"),
        F.date_format(F.col("dateTime"), "yyyy-MM-dd").alias("dt"),
        F.date_format(F.col("dateTime"), "HH:mm").alias("hm"),
        F.current_timestamp().alias("processed_time"),
    )
    write_partitioned_stream(result_df, args, ["dt", "hm"])


if __name__ == "__main__":
    main()
