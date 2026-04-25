#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

# =========================
# CONFIG
# =========================
BASE_ROOT = Path("/srv/nfs/flink/nexmark/scalability")
OUT_DIR = Path("./graphs")
OUT_DIR.mkdir(exist_ok=True)

QUERIES = ["q1", "q11"]

SCHEMAS = {
    "q1": ["col1", "col2", "price", "event_time", "processing_time", "payload"],
    "q2": ["col1", "value", "event_time", "processing_time"],
    "q7": ["col1", "col2", "col3", "event_time", "processing_time", "payload"],
    "q11": ["col1", "col2", "value", "s", "event_time", "processing_time"],
}


# =========================
# HELPERS
# =========================
def extract_parallelism(p_name):
    return int(re.findall(r"\d+", p_name)[0])


def load_data(p_dir, schema):
    part_files = list(p_dir.glob("part-*")) + list(p_dir.glob(".part*"))
    dfs = []

    for f in part_files:
        try:
            dfs.append(pd.read_csv(f, header=None, names=schema))
        except Exception:
            continue

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)


def compute_throughput(df):
    if "processing_time" not in df.columns:
        return None

    df["processing_time"] = pd.to_datetime(
        df["processing_time"],
        format="mixed",
        errors="coerce"
    )

    df = df.dropna(subset=["processing_time"])
    if df.empty:
        return None

    df = df.sort_values("processing_time").set_index("processing_time")

    counts = df.resample("1s").size()
    counts = counts[counts > 0]

    if counts.empty:
        return None

    return counts.mean()


# =========================
# PARALLEL WORK UNIT (P LEVEL)
# =========================
def process_p(args):
    q, p_dir, schema = args

    if not p_dir.is_dir():
        return None

    p = extract_parallelism(p_dir.name)

    df = load_data(p_dir, schema)
    if df is None:
        return None

    tp = compute_throughput(df)
    if tp is None:
        return None

    return q, p, tp


# =========================
# QUERY PROCESSOR (P PARALLEL)
# =========================
def process_query(q):
    base_dir = BASE_ROOT / q

    if q not in SCHEMAS:
        return q, {}

    schema = SCHEMAS[q]

    p_dirs = sorted(base_dir.glob("p*"))
    tasks = [(q, p_dir, schema) for p_dir in p_dirs]

    results = {}

    max_workers = max(1, (os.cpu_count() or 1) // 4)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_p, t) for t in tasks]

        for fut in as_completed(futures):
            res = fut.result()
            if res is None:
                continue

            _, p, tp = res
            results[p] = tp
            print(p, " DONE", tp)

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
        ax.grid(axis='y')

    for j in range(len(ordered_queries), 4):
        axes[j].axis("off")

    fig.suptitle("Nexmark Scalability Comparison", y=1.02)
    fig.tight_layout()

    out_path = OUT_DIR / "scalability_flink.png"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()

    print(f"[SAVED] {out_path}")


# =========================
# MAIN
# =========================
def main():
    print("[START] Query × P parallel scalability analysis")

    all_results = {}

    max_workers = 1 #min(len(QUERIES), os.cpu_count() or 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_query, q): q for q in QUERIES}

        for fut in as_completed(futures):
            q, results = fut.result()
            if results:
                all_results[q] = results

    plot_all_queries_subplots(all_results)

    print("\n[DONE] Saved in ./graphs")


if __name__ == "__main__":
    main()