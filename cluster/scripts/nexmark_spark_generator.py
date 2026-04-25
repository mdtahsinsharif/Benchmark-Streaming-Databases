#!/usr/bin/env python3
"""
Generate Nexmark-shaped streaming data with PySpark Structured Streaming.

This mirrors the Flink Nexmark generator at a practical level:
- event_type 0 = person
- event_type 1 = auction
- event_type 2 = bid
- proportions are configurable
- output schema matches the nested layout used in nexmark/queries/ddl_gen.sql

Examples:
  python scripts/nexmark_spark_generator.py --rows-per-second 100
  python scripts/nexmark_spark_generator.py --sink-format json --output-path /tmp/nexmark-json
  python scripts/nexmark_spark_generator.py --sink-format parquet --output-path /tmp/nexmark-parquet
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PERSON_EVENT_TYPE = 0
AUCTION_EVENT_TYPE = 1
BID_EVENT_TYPE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Nexmark-style streaming data for Spark Structured Streaming."
    )
    parser.add_argument(
        "--rows-per-second",
        "--tps",
        dest="rows_per_second",
        type=int,
        default=1000,
        help="Rows per second for Spark's rate source. Alias: --tps",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=None,
        help="Number of partitions for the rate source. Defaults to Spark default parallelism when omitted.",
    )
    parser.add_argument(
        "--person-proportion",
        type=int,
        default=1,
        help="Relative weight of person events.",
    )
    parser.add_argument(
        "--auction-proportion",
        type=int,
        default=3,
        help="Relative weight of auction events.",
    )
    parser.add_argument(
        "--bid-proportion",
        type=int,
        default=46,
        help="Relative weight of bid events.",
    )
    parser.add_argument(
        "--sink-format",
        choices=("console", "json", "parquet"),
        default="console",
        help="Streaming sink format.",
    )
    parser.add_argument(
        "--output-path",
        help="Required for json/parquet sinks.",
    )
    parser.add_argument(
        "--checkpoint-location",
        default=None,
        help="Checkpoint directory. Defaults under /tmp if omitted.",
    )
    parser.add_argument(
        "--query-name",
        default="nexmark_generator",
        help="Structured Streaming query name.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Spark master URL. When omitted, rely on spark-submit or Spark defaults.",
    )
    parser.add_argument(
        "--truncate",
        choices=("true", "false"),
        default="false",
        help="Console sink truncation mode.",
    )
    parser.add_argument(
        "--trigger-processing-time",
        default=None,
        help="Optional processing time trigger, for example '5 seconds'.",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=None,
        help="Run streaming mode for this many seconds, then stop the query.",
    )
    parser.add_argument(
        "--max-events",
        "--events-num",
        dest="max_events",
        type=int,
        default=None,
        help="Optional bounded generation mode. When set, generate exactly this many events as a batch. Alias: --events-num",
    )
    return parser.parse_args()


def ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def validate_args(args: argparse.Namespace) -> None:
    ensure_positive("rows_per_second", args.rows_per_second)
    ensure_positive("person_proportion", args.person_proportion)
    ensure_positive("auction_proportion", args.auction_proportion)
    ensure_positive("bid_proportion", args.bid_proportion)
    if args.num_partitions is not None:
        ensure_positive("num_partitions", args.num_partitions)
    if args.max_events is not None:
        ensure_positive("max_events", args.max_events)

    if args.sink_format in {"json", "parquet"} and not args.output_path:
        raise ValueError("--output-path is required for json/parquet sinks")


def _null_struct(schema_sql: str) -> F.Column:
    return F.expr(f"CAST(NULL AS {schema_sql})")


def _build_nexmark_events(
    source_df: DataFrame,
    person_proportion: int,
    auction_proportion: int,
    bid_proportion: int,
) -> DataFrame:
    total = (
        person_proportion + auction_proportion + bid_proportion
    )

    mix_index = F.pmod(F.col("sequence_id"), F.lit(total))
    event_type = (
        F.when(mix_index < person_proportion, F.lit(PERSON_EVENT_TYPE))
        .when(
            mix_index < person_proportion + auction_proportion,
            F.lit(AUCTION_EVENT_TYPE),
        )
        .otherwise(F.lit(BID_EVENT_TYPE))
    )

    seller_id = F.pmod(F.col("sequence_id") * F.lit(17) + F.lit(23), F.lit(1_000_000)) + 1
    bidder_id = F.pmod(F.col("sequence_id") * F.lit(31) + F.lit(7), F.lit(1_000_000)) + 1
    auction_id = F.pmod(F.col("sequence_id") * F.lit(13) + F.lit(11), F.lit(10_000_000)) + 1
    category_id = F.pmod(F.col("sequence_id"), F.lit(50)) + 10
    reserve = F.lit(100) + F.pmod(F.col("sequence_id") * F.lit(29), F.lit(9_900))
    initial_bid = F.greatest(F.lit(1), reserve / 2)
    bid_price = reserve + F.pmod(F.col("sequence_id") * F.lit(41), F.lit(5_000))

    person_struct = F.struct(
        (F.pmod(F.col("sequence_id"), F.lit(1_000_000)) + 1).cast("bigint").alias("id"),
        F.concat(F.lit("Person-"), F.col("sequence_id").cast("string")).alias("name"),
        F.concat(F.lit("person"), F.col("sequence_id").cast("string"), F.lit("@example.com")).alias(
            "emailAddress"
        ),
        F.format_string(
            "%04d-%04d-%04d-%04d",
            F.pmod(F.col("sequence_id") * F.lit(3), F.lit(10_000)),
            F.pmod(F.col("sequence_id") * F.lit(5), F.lit(10_000)),
            F.pmod(F.col("sequence_id") * F.lit(7), F.lit(10_000)),
            F.pmod(F.col("sequence_id") * F.lit(11), F.lit(10_000)),
        ).alias("creditCard"),
        F.concat(F.lit("City-"), F.pmod(F.col("sequence_id"), F.lit(1000)).cast("string")).alias(
            "city"
        ),
        F.concat(F.lit("State-"), F.pmod(F.col("sequence_id"), F.lit(50)).cast("string")).alias(
            "state"
        ),
        F.col("source_timestamp").alias("dateTime"),
        F.concat(F.lit("person-extra-"), F.col("sequence_id").cast("string")).alias("extra"),
    )

    auction_struct = F.struct(
        auction_id.cast("bigint").alias("id"),
        F.concat(F.lit("Item-"), F.col("sequence_id").cast("string")).alias("itemName"),
        F.concat(F.lit("Description for item "), F.col("sequence_id").cast("string")).alias(
            "description"
        ),
        initial_bid.cast("bigint").alias("initialBid"),
        reserve.cast("bigint").alias("reserve"),
        F.col("source_timestamp").alias("dateTime"),
        (F.col("source_timestamp") + F.expr("INTERVAL 1 HOUR")).alias("expires"),
        seller_id.cast("bigint").alias("seller"),
        category_id.cast("bigint").alias("category"),
        F.concat(F.lit("auction-extra-"), F.col("sequence_id").cast("string")).alias("extra"),
    )

    bid_struct = F.struct(
        auction_id.cast("bigint").alias("auction"),
        bidder_id.cast("bigint").alias("bidder"),
        bid_price.cast("bigint").alias("price"),
        F.concat(F.lit("channel-"), F.pmod(F.col("sequence_id"), F.lit(8)).cast("string")).alias(
            "channel"
        ),
        F.concat(F.lit("https://example.com/item/"), auction_id.cast("string")).alias("url"),
        F.col("source_timestamp").alias("dateTime"),
        F.concat(F.lit("bid-extra-"), F.col("sequence_id").cast("string")).alias("extra"),
    )

    person_schema = (
        "STRUCT<id: BIGINT, name: STRING, emailAddress: STRING, creditCard: STRING, "
        "city: STRING, state: STRING, dateTime: TIMESTAMP, extra: STRING>"
    )
    auction_schema = (
        "STRUCT<id: BIGINT, itemName: STRING, description: STRING, initialBid: BIGINT, "
        "reserve: BIGINT, dateTime: TIMESTAMP, expires: TIMESTAMP, seller: BIGINT, "
        "category: BIGINT, extra: STRING>"
    )
    bid_schema = (
        "STRUCT<auction: BIGINT, bidder: BIGINT, price: BIGINT, channel: STRING, "
        "url: STRING, dateTime: TIMESTAMP, extra: STRING>"
    )

    return (
        source_df.withColumn("event_type", event_type.cast("int"))
        .withColumn(
            "person",
            F.when(F.col("event_type") == PERSON_EVENT_TYPE, person_struct).otherwise(
                _null_struct(person_schema)
            ),
        )
        .withColumn(
            "auction",
            F.when(F.col("event_type") == AUCTION_EVENT_TYPE, auction_struct).otherwise(
                _null_struct(auction_schema)
            ),
        )
        .withColumn(
            "bid",
            F.when(F.col("event_type") == BID_EVENT_TYPE, bid_struct).otherwise(
                _null_struct(bid_schema)
            ),
        )
        .withColumn(
            "dateTime",
            F.coalesce(
                F.col("person.dateTime"),
                F.col("auction.dateTime"),
                F.col("bid.dateTime"),
            ),
        )
        .select("event_type", "person", "auction", "bid", "dateTime")
    )


def build_nexmark_stream(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    num_partitions = args.num_partitions or spark.sparkContext.defaultParallelism

    rate_reader = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", args.rows_per_second)
        .option("numPartitions", num_partitions)
    )

    rate_df = (
        rate_reader.load()
        .withColumnRenamed("value", "sequence_id")
        .withColumnRenamed("timestamp", "source_timestamp")
    )

    return _build_nexmark_events(
        rate_df,
        args.person_proportion,
        args.auction_proportion,
        args.bid_proportion,
    )#.withWatermark("dateTime", "4 seconds")


def build_nexmark_batch(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.max_events is None:
        raise ValueError("max_events is required for batch generation")

    batch_df = (
        spark.range(args.max_events)
        .withColumnRenamed("id", "sequence_id")
        .withColumn(
            "source_timestamp",
            F.expr("timestampadd(MICROSECOND, sequence_id * 1000L, TIMESTAMP '2026-01-01 00:00:00')")
        )
    )

    return _build_nexmark_events(
        batch_df,
        args.person_proportion,
        args.auction_proportion,
        args.bid_proportion,
    )


def build_writer(df: DataFrame, args: argparse.Namespace):
    checkpoint_location = args.checkpoint_location
    if not checkpoint_location:
        checkpoint_location = os.path.join("/tmp", f"{args.query_name}_checkpoint")

    writer = (
        df.writeStream.queryName(args.query_name)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
    )

    if args.trigger_processing_time:
        writer = writer.trigger(processingTime=args.trigger_processing_time)

    if args.sink_format == "console":
        return (
            writer.format("console")
            .option("truncate", args.truncate)
            .option("numRows", 20)
        )

    return writer.format(args.sink_format).option("path", args.output_path)


def print_schema_notes() -> None:
    lines: Iterable[str] = (
        "event_type=0 -> person populated",
        "event_type=1 -> auction populated",
        "event_type=2 -> bid populated",
        "top-level dateTime is copied from the active nested event struct",
    )
    for line in lines:
        print(line)


def main() -> None:
    args = parse_args()
    validate_args(args)

    spark = (
        SparkSession.builder.appName("NexmarkSparkGenerator")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.master:
        spark = spark.master(args.master)
    spark = spark.getOrCreate()

    print_schema_notes()
    if args.max_events is not None:
        df = build_nexmark_batch(spark, args)
        if args.sink_format != "console":
            (
                df.write.format(args.sink_format)
                .mode("overwrite")
                .save(args.output_path)
            )
        else:
            df.show(args.max_events, truncate=args.truncate == "true")
        return

    df = build_nexmark_stream(spark, args)
    query = build_writer(df, args).start()
    if args.run_seconds is not None:
        query.awaitTermination(args.run_seconds * 1000)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()
