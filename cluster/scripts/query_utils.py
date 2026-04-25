#!/usr/bin/env python3

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession


def add_common_streaming_args(
    parser: argparse.ArgumentParser,
    query_name: str,
    default_output_path: str | None = None,
) -> None:
    parser.add_argument("--master", default=None, help="Spark master URL.")
    parser.add_argument(
        "--rows-per-second",
        "--tps",
        dest="rows_per_second",
        type=int,
        default=1000,
        help="Rows per second for streaming mode. Alias: --tps",
    )
    parser.add_argument("--num-partitions", type=int, default=1)
    parser.add_argument("--person-proportion", type=int, default=1)
    parser.add_argument("--auction-proportion", type=int, default=3)
    parser.add_argument("--bid-proportion", type=int, default=46)
    parser.add_argument(
        "--query-name",
        default=query_name,
        help="Structured Streaming query name.",
    )
    parser.add_argument(
        "--checkpoint-location",
        default=f"/tmp/{query_name}_checkpoint",
        help="Checkpoint directory for streaming mode.",
    )
    parser.add_argument(
        "--truncate",
        choices=("true", "false"),
        default="false",
        help="Console truncation mode.",
    )
    parser.add_argument(
        "--trigger-processing-time",
        default="200 milliseconds",
        help="Processing time trigger, for example '200 milliseconds'.",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=None,
        help="Run streaming mode for this many seconds, then stop the query.",
    )
    parser.add_argument(
        "--sink-format",
        choices=("noop", "console", "csv", "parquet"),
        default="csv",
        help="Streaming sink format.",
    )
    parser.add_argument(
        "--output-path",
        default=default_output_path,
        help="Output path for csv/parquet sinks.",
    )


def create_spark_session(
    app_name: str,
    master: str | None,
    num_partitions: int | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true") 
    )
    if master:
        builder = builder.master(master)
    if num_partitions is not None:
        builder = (
            builder.config("spark.sql.shuffle.partitions", str(num_partitions))
            .config("spark.default.parallelism", str(num_partitions))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_stream(result_df: DataFrame, args: argparse.Namespace) -> None:
    writer = (
        result_df.writeStream.queryName(args.query_name)
        .format(args.sink_format)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_location)
    )
    if args.sink_format == "console":
        writer = writer.option("truncate", args.truncate).option("numRows", 20)
    elif args.sink_format in {"csv", "parquet"}:
        if not args.output_path:
            raise ValueError("--output-path is required for csv/parquet sinks")
        writer = writer.option("path", args.output_path)
        if args.sink_format == "csv":
            writer = (
                writer.option("sep", ",")
                .option("quote", "")
                .option("escape", "")
                .option("header", "false")
            )
    if args.trigger_processing_time:
        writer = writer.trigger(processingTime=args.trigger_processing_time)

    query = writer.start()
    if args.run_seconds is not None:
        query.awaitTermination(args.run_seconds * 1000)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()


def write_partitioned_stream(
    result_df: DataFrame,
    args: argparse.Namespace,
    partition_cols: list[str],
) -> None:
    if args.sink_format not in {"csv", "parquet"}:
        raise ValueError("partitioned output requires csv or parquet sink format")
    if not args.output_path:
        raise ValueError("--output-path is required for partitioned output")

    writer = (
        result_df.writeStream.queryName(args.query_name)
        .format(args.sink_format)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_location)
        .option("path", args.output_path)
        .partitionBy(*partition_cols)
    )
    if args.sink_format == "csv":
        writer = (
            writer.option("sep", ",")
            .option("quote", "")
            .option("escape", "")
            .option("header", "false")
        )
    if args.trigger_processing_time:
        writer = writer.trigger(processingTime=args.trigger_processing_time)

    query = writer.start()
    if args.run_seconds is not None:
        query.awaitTermination(args.run_seconds * 1000)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()
