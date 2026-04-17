import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# =========================
# 1. LOAD DATA (JSONL)
# =========================
file_path = "metrics/spark_stream_metrics_0.000001.json"  # change to your file

data = []
with open(file_path, "r") as f:
    for line in f:
        obj = json.loads(line)
        data.append(obj)

df = pd.DataFrame(data)

# =========================
# 2. CLEAN DATA
# =========================
df = df.dropna(subset=[
    "throughput_rps",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms"
])

# optional: remove zero-throughput noise
df = df[df["throughput_rps"] > 0]

# =========================
# 3. BUCKET THROUGHPUT
# =========================
df["throughput_bin"] = pd.cut(
    df["throughput_rps"],
    bins=5
)

# =========================
# 4. BUILD BOXPLOT DATA (APPROXIMATION)
# =========================
boxplot_rows = []

for _, row in df.iterrows():

    # synthetic distribution based on percentiles
    samples = np.concatenate([
        np.full(50, row["p50_latency_ms"]),
        np.full(25, row["avg_latency_ms"]),
        np.full(15, row["p95_latency_ms"]),
        np.full(10, row["p99_latency_ms"]),
    ])

    for val in samples:
        boxplot_rows.append({
            "throughput_bin": str(row["throughput_bin"]),
            "latency": val
        })

plot_df = pd.DataFrame(boxplot_rows)

# =========================
# 5. BOXPLOT: LATENCY VS THROUGHPUT
# =========================
plt.figure(figsize=(12, 6))

plot_df.boxplot(
    column="latency",
    by="throughput_bin"
)

plt.title("Latency Distribution vs Throughput (Approximate)")
plt.suptitle("")
plt.xlabel("Throughput Bins (RPS)")
plt.ylabel("Latency (ms)")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig("figures/figures_m000001/latency_dist_vs_throughput.png", dpi=300, bbox_inches="tight")

# =========================
# 6. CLEAN AGGREGATE PLOT (RECOMMENDED)
# =========================
agg = df.groupby("throughput_bin").agg({
    "p50_latency_ms": "mean",
    "avg_latency_ms": "mean",
    "p95_latency_ms": "mean",
    "p99_latency_ms": "mean"
}).reset_index()

x = np.arange(len(agg))

plt.figure(figsize=(12, 6))

plt.plot(x, agg["p50_latency_ms"], marker="o", label="P50")
plt.plot(x, agg["avg_latency_ms"], marker="o", label="Mean")
plt.plot(x, agg["p95_latency_ms"], marker="o", label="P95")
plt.plot(x, agg["p99_latency_ms"], marker="o", label="P99")

plt.fill_between(
    x,
    agg["p50_latency_ms"],
    agg["p99_latency_ms"],
    alpha=0.2
)

plt.xticks(x, agg["throughput_bin"], rotation=45)
plt.xlabel("Throughput Bins (RPS)")
plt.ylabel("Latency (ms)")
plt.title("Latency vs Throughput (Percentile View)")
plt.legend()

plt.tight_layout()
#plt.tight_layout()
plt.savefig("figures/figures_m000001/latency_vs_throughput.png", dpi=300, bbox_inches="tight")
plt.close()
#plt.show()