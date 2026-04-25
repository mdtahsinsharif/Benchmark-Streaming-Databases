#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd
import glob
import re
import matplotlib.pyplot as plt
import os


# In[46]:


base_dir_flink = "latency_output/20260418_123653_q1/2026_04_18_12"

files_flink = []

for root, dirs, filenames in os.walk(base_dir_flink):
    for name in filenames:
        if "part-" in name:   # Spark files are usually part-00000...
            files_flink.append(os.path.join(root, name))

print("Found files:", len(files_flink))
for f in files_flink[:10]:
    print(f)


# In[47]:


pwd


# In[48]:


rows_flink = []
pattern = re.compile(
    r"window_start=(.*?), window_end=(.*?), records=(\d+), "
    r"avg_latency_ms=([\d.]+), p50_ms=(\d+), p95_ms=(\d+), p99_ms=(\d+)"
)


# In[49]:


for file in files_flink:
    print(file)
    with open(file, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                rows_flink.append({
                    "window_start": match.group(1),
                    "window_end": match.group(2),
                    "records": int(match.group(3)),
                    "avg_latency_ms": float(match.group(4)),
                    "p50_ms": int(match.group(5)),
                    "p95_ms": int(match.group(6)),
                    "p99_ms": int(match.group(7))
                })


# In[50]:


rows_flink[:2]


# In[51]:


# --------------------------------------------------
# 2. Convert to DataFrame
# --------------------------------------------------
df_flink = pd.DataFrame(rows_flink)

# convert timestamps
df_flink["window_start"] = pd.to_datetime(df_flink["window_start"])
df_flink["window_end"] = pd.to_datetime(df_flink["window_end"])

# --------------------------------------------------
# 3. IMPORTANT: remove duplicates (you have repeated windows)
# --------------------------------------------------
df_flink = df_flink.groupby("window_start").mean(numeric_only=True).reset_index()

# recompute window_end (optional consistency)
df_flink["window_end"] = df_flink["window_start"] + pd.Timedelta(seconds=5)
df_flink["throughput_per_sec"] = df_flink["records"] / 5
# --------------------------------------------------
# 4. Sort
# --------------------------------------------------
df_flink = df_flink.sort_values("window_start")
df_flink["window_index"] = range(len(df_flink))

# convert to seconds (5s window size)
df_flink["time_sec"] = df_flink["window_index"] * 5


# In[52]:


df_flink["window_start"] = pd.to_datetime(df_flink["window_start"])
df_flink = df_flink.sort_values("window_start")

# convert to relative time (seconds from start)
df_flink["time_sec"] = (df_flink["window_start"] - df_flink["window_start"].min()).dt.total_seconds()


# In[53]:


df_flink = df_flink[(df_flink["time_sec"] >= 6) & (df_flink["time_sec"] <= 60)].reset_index(drop=True)

# 🔴 Shift time so it starts at 0 again
df_flink["time_sec"] = df_flink["time_sec"] - df_flink["time_sec"].min() + 2


# In[54]:


# SPARK
# Input file
suffix_spark = "_spark_m10k_q1"
file_path_spark = "metrics/spark_stream_metrics_q1.json"
directory_spark = "figures/figures_" + suffix_spark + "/"

os.makedirs(directory_spark, exist_ok=True)


# In[55]:


import json
# ---- read JSON lines ----
data_spark = []
with open(file_path_spark, "r") as f:
    for line in f:
        data_spark.append(json.loads(line))


# In[56]:


df_spark = pd.DataFrame(data_spark)

# convert timestamp
df_spark["timestamp"] = pd.to_datetime(df_spark["timestamp"])
df_spark = df_spark.sort_values("timestamp")

# ---------------------------------------
# OPTIONAL: remove idle batches
# ---------------------------------------
df_spark = df_spark[df_spark["numInputRows"] > 0]

# ---------------------------------------
# Convert time → seconds from start
# ---------------------------------------
df_spark["time_sec"] = (df_spark["timestamp"] - df_spark["timestamp"].min()).dt.total_seconds()

# ---------------------------------------
# Rolling smoothing (window = 5 points)
# ---------------------------------------
ROLLING_WINDOW_spark = 5

df_spark["p50_smooth"] = df_spark["p50_latency_ms"].rolling(ROLLING_WINDOW_spark).mean()
df_spark["p95_smooth"] = df_spark["p95_latency_ms"].rolling(ROLLING_WINDOW_spark).mean()
df_spark["p99_smooth"] = df_spark["p99_latency_ms"].rolling(ROLLING_WINDOW_spark).mean()
df_spark["avg_smooth"] = df_spark["avg_latency_ms"].rolling(ROLLING_WINDOW_spark).mean()


# In[63]:


# --------------------------------------------------
# 5. Plot 1: Latency trends
# --------------------------------------------------
t_min, t_max = 0, 80

df_flink_filtered = df_flink[
    (df_flink["time_sec"] >= t_min) &
    (df_flink["time_sec"] <= t_max)
]

df_spark_filtered = df_spark[
    (df_spark["time_sec"] >= t_min) &
    (df_spark["time_sec"] <= t_max)
]

plt.figure()

#plt.plot(df_flink["time_sec"], df_flink["avg_latency_ms"], label="avg")
plt.plot(df_flink_filtered["time_sec"], df_flink_filtered["p50_ms"], label="flink_p50")
plt.plot(df_flink_filtered["time_sec"], df_flink_filtered["p95_ms"], label="flink_p95")
plt.plot(df_spark_filtered["time_sec"], df_spark_filtered["p50_smooth"], label="spark_p50")
plt.plot(df_spark_filtered["time_sec"], df_spark_filtered["p95_smooth"], label="spark_p95")

plt.legend()
plt.title("Latency vs Time (Flink)")
plt.xlabel("Time (s)")
plt.ylabel("Latency (ms)")
#plt.xticks(rotation=45)

plt.xlim(left=0)
plt.ylim(bottom=0)

plt.tight_layout()
plt.grid(True)
plt.savefig("figures/q1_10k/latency_vs_time_0.png", dpi=300, bbox_inches="tight")
plt.show()


# In[58]:


# --------------------------------------------------
# 6. Plot 2: Throughput
# --------------------------------------------------
'''plt.figure()
plt.plot(df["window_start"], df["throughput_per_sec"])
plt.title("Throughput Over Time")
plt.xlabel("Window")
plt.ylabel("Records/s")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
'''
plt.figure()

plt.plot(df["time_sec"], df["throughput_per_sec"])

plt.title("Throughput Over Time")
plt.xlabel("Time (s)")
plt.ylabel("Records/s")

plt.xlim(left=0)
plt.ylim(bottom=0)

#plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(True)
plt.savefig("figures/q1_10k/throughput_vs_time_0.png", dpi=300, bbox_inches="tight")
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
plt.grid(True)
plt.savefig("figures/q1_10k/latency_vs_throughput.png", dpi=300, bbox_inches="tight")
plt.show()


# In[ ]:




