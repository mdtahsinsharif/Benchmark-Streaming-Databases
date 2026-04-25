#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = REPO_ROOT / "scripts" / "queries"
DEFAULT_SPARK_SUBMIT = REPO_ROOT / "spark" / "bin" / "spark-submit"


# ---------------- AVAILABLE QUERIES ----------------
def available_queries() -> list[str]:
    return sorted(path.stem for path in QUERIES_DIR.glob("q*.py"))


# ---------------- ARG PARSER ----------------
def parse_args():
    queries = available_queries()

    parser = argparse.ArgumentParser(
        description="Run one or more Spark Nexmark queries."
    )

    parser.add_argument(
        "target",
        choices=["all", *queries],
        help="Query name or 'all'.",
    )

    parser.add_argument("--spark-submit", default=str(DEFAULT_SPARK_SUBMIT))
    parser.add_argument("--master", default=None)
    parser.add_argument("--driver-host", default=None)

    parser.add_argument("--total-executor-cores", type=int, default=None)

    parser.add_argument("--conf", action="append", default=[])

    # ---------------- BENCHMARK CONTROLS ----------------
    parser.add_argument("--tps", type=int, default=1000)
    parser.add_argument("--num-partitions", type=int, default=1)
    parser.add_argument("--trigger-processing-time", default="1 second")

    parser.add_argument("--run-seconds", type=int, default=None)

    parser.add_argument("--dry-run", action="store_true")

    return parser.parse_known_args()


# ---------------- COMMAND BUILDER ----------------
def build_command(query_name: str, args, extra_args: list[str]) -> list[str]:
    cmd = [args.spark_submit]

    if args.master:
        cmd += ["--master", args.master]

    if args.driver_host:
        cmd += ["--conf", f"spark.driver.host={args.driver_host}"]

    if args.total_executor_cores is not None:
        cmd += ["--total-executor-cores", str(args.total_executor_cores)]

    for conf in args.conf:
        cmd += ["--conf", conf]

    cmd.append(str(QUERIES_DIR / f"{query_name}.py"))

    # ---------------- STANDARDIZED ARG INJECTION ----------------
    cmd += [
        "--tps", str(args.tps),
        "--num-partitions", str(args.num_partitions),
        "--trigger-processing-time", args.trigger_processing_time,
    ]

    if args.run_seconds is not None:
        cmd += ["--run-seconds", str(args.run_seconds)]

    # forward extra CLI args (rare case overrides)
    cmd += extra_args

    return cmd


# ---------------- MAIN ----------------
def main() -> int:
    args, extra = parse_args()

    queries = available_queries()
    selected = queries if args.target == "all" else [args.target]

    if not Path(args.spark_submit).exists():
        raise FileNotFoundError(f"spark-submit not found: {args.spark_submit}")

    env = os.environ.copy()

    for q in selected:
        cmd = build_command(q, args, extra)

        print(f"\n=== Running {q} ===")
        print(" ".join(cmd))

        if args.dry_run:
            continue

        result = subprocess.run(cmd, cwd=REPO_ROOT, env=env)

        if result.returncode != 0:
            return result.returncode

    return 0


if __name__ == "__main__":
    sys.exit(main())