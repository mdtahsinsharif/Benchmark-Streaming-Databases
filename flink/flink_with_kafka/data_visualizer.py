import pandas as pd
import glob
import re
import matplotlib.pyplot as plt

# --------------------------------------------------
# 1. Load all .part files
# --------------------------------------------------
files = glob.glob("latency_output/20260418_123653/2026-04-18--12/*.part*")  # adjust path if needed

rows = []

pattern = re.compile(
    r"window_start=(.*?), window_end=(.*?), records=(\d+), "
    r"avg_latency_ms=([\d.]+), p50_ms=(\d+), p95_ms=(\d+), p99_ms=(\d+)"
)

for file in files:
    print(file)
    with open(file, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                rows.append({
                    "window_start": match.group(1),
                    "window_end": match.group(2),
                    "records": int(match.group(3)),
                    "avg_latency_ms": float(match.group(4)),
                    "p50_ms": int(match.group(5)),
                    "p95_ms": int(match.group(6)),
                    "p99_ms": int(match.group(7))
                })

# --------------------------------------------------
# 2. Convert to DataFrame
# --------------------------------------------------
df = pd.DataFrame(rows)

# convert timestamps
df["window_start"] = pd.to_datetime(df["window_start"])
df["window_end"] = pd.to_datetime(df["window_end"])

# --------------------------------------------------
# 3. IMPORTANT: remove duplicates (you have repeated windows)
# --------------------------------------------------
df = df.groupby("window_start").mean(numeric_only=True).reset_index()

# recompute window_end (optional consistency)
df["window_end"] = df["window_start"] + pd.Timedelta(seconds=5)

# --------------------------------------------------
# 4. Sort
# --------------------------------------------------
df = df.sort_values("window_start")

# --------------------------------------------------
# 5. Plot 1: Latency trends
# --------------------------------------------------
plt.figure()
plt.plot(df["window_start"], df["avg_latency_ms"], label="avg")
plt.plot(df["window_start"], df["p50_ms"], label="p50")
plt.plot(df["window_start"], df["p95_ms"], label="p95")
plt.plot(df["window_start"], df["p99_ms"], label="p99")
plt.legend()
plt.title("Latency Over Time")
plt.xlabel("Window")
plt.ylabel("Latency (ms)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# --------------------------------------------------
# 6. Plot 2: Throughput
# --------------------------------------------------
plt.figure()
plt.plot(df["window_start"], df["records"])
plt.title("Throughput Over Time")
plt.xlabel("Window")
plt.ylabel("Records per 5s window")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# --------------------------------------------------
# 7. Plot 3: Latency vs throughput correlation (optional but useful)
# --------------------------------------------------
plt.figure()
plt.scatter(df["records"], df["avg_latency_ms"])
plt.title("Latency vs Throughput")
plt.xlabel("Throughput (records/window)")
plt.ylabel("Avg Latency (ms)")
plt.tight_layout()
plt.show()