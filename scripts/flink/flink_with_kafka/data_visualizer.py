import re
import pandas as pd
import matplotlib.pyplot as plt
import os

# path to your log file
log_file = "flink_latency_throughput"

# create figures directory
os.makedirs("figures", exist_ok=True)

pattern = r"window_start=(.*?), window_end=(.*?), records=(\d+), avg_latency_ms=(.*?), p50_ms=(\d+), p95_ms=(\d+), p99_ms=(\d+)"

rows = []

# ---- read file ----
with open(log_file, "r") as f:
    for line in f:
        m = re.search(pattern, line)
        if m:
            rows.append({
                "window_start": m.group(1),
                "window_end": m.group(2),
                "records": int(m.group(3)),
                "avg_latency": float(m.group(4)),
                "p50": int(m.group(5)),
                "p95": int(m.group(6)),
                "p99": int(m.group(7))
            })

df = pd.DataFrame(rows)

# convert time column
df["window_start"] = pd.to_datetime(df["window_start"])

# ---------------------------------------
# aggregate across parallel subtasks
# ---------------------------------------
agg = df.groupby("window_start").agg({
    "records": "sum",
    "avg_latency": "mean",
    "p50": "mean",
    "p95": "mean",
    "p99": "mean"
}).reset_index()

agg = agg.sort_values("window_start")

# compute throughput (5-second windows)
WINDOW_SIZE = 5
agg["throughput"] = agg["records"] / WINDOW_SIZE

print(agg.head())

# ---------------------------------------
# 1. latency percentiles vs time
# ---------------------------------------
plt.figure()
plt.plot(agg["window_start"], agg["p50"], label="p50")
plt.plot(agg["window_start"], agg["p95"], label="p95")
plt.plot(agg["window_start"], agg["p99"], label="p99")

plt.xlabel("Time")
plt.ylabel("Latency (ms)")
plt.title("Latency Percentiles Over Time")
plt.legend()
plt.grid(True)

plt.savefig("figures/latency_percentiles.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 2. throughput vs time
# ---------------------------------------
plt.figure()
plt.plot(agg["window_start"], agg["throughput"])

plt.xlabel("Time")
plt.ylabel("Throughput (events/sec)")
plt.title("Throughput Over Time")
plt.grid(True)

plt.savefig("figures/throughput_vs_time.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 3. avg latency vs time
# ---------------------------------------
plt.figure()
plt.plot(agg["window_start"], agg["avg_latency"])

plt.xlabel("Time")
plt.ylabel("Average Latency (ms)")
plt.title("Average Latency Over Time")
plt.grid(True)

plt.savefig("figures/avg_latency_vs_time.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 4. throughput vs latency
# ---------------------------------------
plt.figure()
plt.scatter(agg["throughput"], agg["avg_latency"])

plt.xlabel("Throughput (events/sec)")
plt.ylabel("Average Latency (ms)")
plt.title("Throughput vs Latency")
plt.grid(True)

plt.savefig("figures/throughput_vs_latency.png", dpi=300, bbox_inches="tight")
plt.close()