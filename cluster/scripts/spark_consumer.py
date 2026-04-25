#!/usr/bin/env python3

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Nexmark events from a TCP socket with Spark.")
    parser.add_argument("--host", default="10.1.2.104", help="Socket server host.")
    parser.add_argument("--port", type=int, default=9999, help="Socket server port.")
    parser.add_argument("--master", default=None, help="Spark master URL.")
    parser.add_argument("--query-name", default="nexmark_socket_consumer")
    parser.add_argument("--checkpoint-location", default="/tmp/nexmark_socket_consumer_checkpoint")
    parser.add_argument("--trigger-processing-time", default="200 milliseconds")
    parser.add_argument("--run-seconds", type=int, default=None)
    parser.add_argument("--sink-format", choices=("console", "noop"), default="console")
    parser.add_argument("--truncate", choices=("true", "false"), default="false")
    parser.add_argument(
        "--mode",
        choices=("raw", "q0"),
        default="q0",
        help="raw prints parsed Nexmark events; q0 selects bid rows like Nexmark q0.",
    )
    return parser.parse_args()


def build_schema() -> StructType:
    person_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("emailAddress", StringType()),
            StructField("creditCard", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("dateTime", StringType()),
            StructField("extra", StringType()),
        ]
    )
    auction_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("itemName", StringType()),
            StructField("description", StringType()),
            StructField("initialBid", LongType()),
            StructField("reserve", LongType()),
            StructField("dateTime", StringType()),
            StructField("expires", StringType()),
            StructField("seller", LongType()),
            StructField("category", LongType()),
            StructField("extra", StringType()),
        ]
    )
    bid_schema = StructType(
        [
            StructField("auction", LongType()),
            StructField("bidder", LongType()),
            StructField("price", LongType()),
            StructField("channel", StringType()),
            StructField("url", StringType()),
            StructField("dateTime", StringType()),
            StructField("extra", StringType()),
        ]
    )
    return StructType(
        [
            StructField("event_type", IntegerType()),
            StructField("dateTime", StringType()),
            StructField("person", person_schema),
            StructField("auction", auction_schema),
            StructField("bid", bid_schema),
        ]
    )


def build_query_df(spark: SparkSession, args: argparse.Namespace):
    schema = build_schema()
    raw_df = (
        spark.readStream.format("socket")
        .option("host", args.host)
        .option("port", args.port)
        .load()
    )

    parsed_df = (
        raw_df.select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("dateTime", F.to_timestamp("dateTime"))
        .withColumn("event_time", F.coalesce(F.col("bid.dateTime"), F.col("auction.dateTime"), F.col("person.dateTime")))
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    if args.mode == "raw":
        return parsed_df

    return (
        parsed_df.where("event_type = 2")
        .select(
            F.col("bid.auction").alias("auction"),
            F.col("bid.bidder").alias("bidder"),
            F.col("bid.price").alias("price"),
            F.col("event_time").alias("dateTime"),
            F.col("bid.extra").alias("extra"),
        )
    )


def main() -> None:
    args = parse_args()

    builder = (
        SparkSession.builder.appName("NexmarkSocketConsumer")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    result_df = build_query_df(spark, args)

    writer = (
        result_df.writeStream.queryName(args.query_name)
        .format(args.sink_format)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_location)
    )
    if args.sink_format == "console":
        writer = writer.option("truncate", args.truncate).option("numRows", 20)
    if args.trigger_processing_time:
        writer = writer.trigger(processingTime=args.trigger_processing_time)

    query = writer.start()
    if args.run_seconds is not None:
        query.awaitTermination(args.run_seconds * 1000)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()
