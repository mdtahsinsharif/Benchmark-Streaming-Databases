#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re

# =========================
# CONFIG
# =========================
BASE_ROOT = Path("/srv/nfs/spark/nexmark/scalability")
OUT_DIR = Path("./graphs")
OUT_DIR.mkdir(exist_ok=True)

QUERIES = ["q1", "q11"]

SCHEMAS = {
    "q0": ["auction", "bidder", "price", "dateTime", "extra", "processed_time"],
    "q1": ["auction", "bidder", "price", "dateTime", "extra", "processed_time"],
    "q11": ["auction", "bidder", "price", "dateTime", "url", "extra", "processed_time"],
}

# =========================
# HELPERS
# =========================
def extract_parallelism(p_name):
    return int(re.findall(r"\d+", p_name)[0])


def compute_throughput_streaming(p_dir, schema, time_col):
    part_files = list(p_dir.glob("part-*")) + list(p_dir.glob(".part*"))

    if not part_files:
        return None

    global_counts = {}

    for f in part_files:
        try:
            df = pd.read_csv(
                f,
                header=None,
                names=schema,
                usecols=[time_col],  # read only needed column
            )
        except Exception:
            continue

        ts = pd.to_datetime(
            df[time_col],
            format="mixed",
            errors="coerce",
            utc=True
        )

        ts = ts.dropna()
        if ts.empty:
            continue

        # Convert to seconds (int64 nanoseconds → seconds)
        sec = (ts.astype("int64") // 1_000_000_000)

        # Count events per second
        counts = pd.Series(sec).value_counts()

        # Merge counts
        for k, v in counts.items():
            global_counts[k] = global_counts.get(k, 0) + v

    if not global_counts:
        return None

    counts = pd.Series(global_counts)
    counts = counts[counts > 0]

    if counts.empty:
        return None

    return counts.mean()


# =========================
# PROCESS ONE QUERY
# =========================
def process_query(q):
    print(f"\n[QUERY] {q}")

    base_dir = BASE_ROOT / q

    if q not in SCHEMAS:
        return q, {}

    schema = SCHEMAS[q]
    p_dirs = sorted(base_dir.glob("p*"))

    results = {}

    # Select correct time column
    if q == "q1":
        time_col = "processed_time"
    else:
        time_col = "processed_time"

    for p_dir in p_dirs:
        if not p_dir.is_dir():
            continue

        p = extract_parallelism(p_dir.name)

        print(f"  -> Processing P={p}")

        tp = compute_throughput_streaming(p_dir, schema, time_col)

        if tp is None:
            print(f"     [SKIP] No throughput")
            continue

        results[p] = tp
        print(f"     [DONE] P={p} → {tp:.2f} ev/s")

    return q, results


# =========================
# PLOTTING
# =========================
def plot_all_queries_subplots(all_results):
    if not all_results:
        print("[WARN] No results to plot")
        return

    ordered_queries = [q for q in QUERIES if q in all_results]

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    axes = axes.flatten()

    for i, q in enumerate(ordered_queries):
        results = all_results[q]

        ps = sorted(results.keys())
        tps = [results[p] for p in ps]

        ax = axes[i]
        ax.bar(ps, tps)

        ax.set_title(q.upper())
        ax.set_xlabel("Cores")
        ax.set_ylabel("Throughput (events/sec)")
        ax.set_xticks(ps)

        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y")

    for j in range(len(ordered_queries), 4):
        axes[j].axis("off")

    fig.suptitle("Nexmark Scalability Comparison", y=1.02)
    fig.tight_layout()

    out_path = OUT_DIR / "scalability_spark.png"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()

    print(f"\n[SAVED] {out_path}")


# =========================
# MAIN
# =========================
def main():
    print("[START] Query × P parallel scalability analysis")

    all_results = {}

    for q in QUERIES:
        q_name, results = process_query(q)
        if results:
            all_results[q_name] = results

    plot_all_queries_subplots(all_results)

    print("\n[DONE] Saved in ./graphs")


if __name__ == "__main__":
    main()