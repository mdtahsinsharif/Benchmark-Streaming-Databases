import json
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Input file
suffix = "m0000001"
file_path = "metrics/spark_stream_metrics_0."+suffix[1:]+".json"
directory = "figures/figures_"+suffix+"/"
# Output directory
os.makedirs(directory, exist_ok=True)

# ---- read JSON lines ----
data = []
with open(file_path, "r") as f:
    for line in f:
        data.append(json.loads(line))

df = pd.DataFrame(data)

# convert timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")

# ---------------------------------------
# OPTIONAL: remove idle batches
# ---------------------------------------
df = df[df["numInputRows"] > 0]

# ---------------------------------------
# Rolling smoothing (window = 5 points)
# ---------------------------------------
ROLLING_WINDOW = 5

df["p50_smooth"] = df["p50_latency_ms"].rolling(ROLLING_WINDOW).mean()
df["p95_smooth"] = df["p95_latency_ms"].rolling(ROLLING_WINDOW).mean()
df["p99_smooth"] = df["p99_latency_ms"].rolling(ROLLING_WINDOW).mean()

# ---------------------------------------
# 1. Smoothed latency percentiles
# ---------------------------------------
plt.figure()

plt.plot(df["timestamp"], df["p50_smooth"], label="p50")
plt.plot(df["timestamp"], df["p95_smooth"], label="p95")
plt.plot(df["timestamp"], df["p99_smooth"], label="p99")

plt.xlabel("Time")
plt.ylabel("Latency (ms)")
plt.title("Latency Percentiles Over Time")
plt.legend()
plt.grid(True)

plt.savefig(directory + "latency_percentiles_smoothed.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 2. Raw vs smoothed (optional but useful)
# ---------------------------------------
plt.figure()

plt.plot(df["timestamp"], df["p99_latency_ms"], alpha=0.3, label="p99 raw")
plt.plot(df["timestamp"], df["p99_smooth"], label="p99")

plt.xlabel("Time")
plt.ylabel("Latency (ms)")
plt.title("Raw vs p99 Latency")
plt.legend()
plt.grid(True)

plt.savefig(directory + "p99_raw_vs_smoothed.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 3. Throughput vs latency (scatter)
# ---------------------------------------
plt.figure()

plt.scatter(df["throughput_rps"], df["avg_latency_ms"])

plt.xlabel("Throughput (events/sec)")
plt.ylabel("Avg Latency (ms)")
plt.title("Throughput vs Latency")
plt.grid(True)

plt.savefig(directory + "throughput_vs_latency.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 4. Add trend line (correlation)
# ---------------------------------------
plt.figure()

x = df["throughput_rps"]
y = df["avg_latency_ms"]

plt.scatter(x, y)

# line of best fit
coef = np.polyfit(x, y, 1)
trend = np.poly1d(coef)
plt.plot(x, trend(x), linestyle="--")

plt.xlabel("Throughput (events/sec)")
plt.ylabel("Avg Latency (ms)")
plt.title("Throughput vs Latency (with Trend)")
plt.grid(True)

plt.savefig(directory + "throughput_vs_latency_trend.png", dpi=300, bbox_inches="tight")
plt.close()

# ---------------------------------------
# 5. Correlation value
# ---------------------------------------
correlation = df["throughput_rps"].corr(df["avg_latency_ms"])
print(f"Correlation (throughput vs latency): {correlation:.4f}")


'''
increase 0
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro spark % python3 data_visualizer_spark.py
Correlation (throughput vs latency): -0.0486
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro spark % python3 data_visualizer_spark.py
Correlation (throughput vs latency): -0.0677
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro spark % python3 data_visualizer_spark.py
Correlation (throughput vs latency): -0.1247
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro spark % python3 data_visualizer_spark.py
Correlation (throughput vs latency): -0.0726

'''