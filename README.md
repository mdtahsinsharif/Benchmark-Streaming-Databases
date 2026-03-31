# Benchmark Streaming Databases

This repository contains benchmarking code for Spark and Flink streaming systems with Kafka.

## Setup and Run Order

1. Prepare required tars and jars before configuration.
   - Create a `tars/` folder in the repository root.
   - Download the following:
     - `flink-sql-connector-kafka-3.0.1-1.18.jar`
     - `spark-sql-kafka-0-10_2.12-3.5.0.jar`
     - `flink-1.18.0-bin-scala_2.12.tar`
     - `kafka_2.12-3.7.1.tar`
     - `spark-3.5.8-bin-hadoop3.tar`

2. Create and activate the conda environment.
   - Run `conda env create -f environment.yml -n benchmark-streaming`.
   - Run `conda activate benchmark-streaming`.

3. Configure the environment.
   - For Flink, run `flink_config.sh`.
   - For Spark, run `spark_config.sh`.
   - After running the config script, source your shell profile to apply environment changes:
     - `source ~/.bashrc` (or `source ~/.bash_profile` if you use that).
   - Run `conda activate benchmark-streaming`

3. After configuration is complete, start the benchmark workflow.
   - Run `start.sh`.

## Notes

- Kafka setup is shared between Spark and Flink. Once Kafka is configured, you do not need to repeat Kafka setup for the other framework.
- Kafka setup is shared between Spark and Flink. Once Kafka is configured, you do not need to repeat Kafka setup for the other framework.
- For framework-specific instructions, see:
  - `spark/README` - Spark with Kafka setup
  - `flink/flink_with_kafka/README` - Flink with Kafka setup

