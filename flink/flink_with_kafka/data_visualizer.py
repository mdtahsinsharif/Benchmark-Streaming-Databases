import re
import pandas as pd
import matplotlib.pyplot as plt
import os

# path to your log file
suffix = "m000001"
log_file = "flink_latency_throughput_0." + suffix[1:]
directory = "figures/"+suffix+"/"
# create figures directory
os.makedirs(directory, exist_ok=True)

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

plt.savefig(directory + "latency_percentiles.png", dpi=300, bbox_inches="tight")
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

plt.savefig(directory + "throughput_vs_time.png", dpi=300, bbox_inches="tight")
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

plt.savefig(directory + "avg_latency_vs_time.png", dpi=300, bbox_inches="tight")
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

plt.savefig(directory + "throughput_vs_latency.png", dpi=300, bbox_inches="tight")
plt.close()

'''
0.001
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro flink_with_kafka % python3 data_visualizer.py
         window_start  records  avg_latency      p50      p95      p99  throughput
0 2026-03-31 15:03:05      723     715.0575   665.50  1371.00  1469.75       144.6
1 2026-03-31 15:03:10     2724    1001.5250   985.50  1865.25  1994.25       544.8
2 2026-03-31 15:03:15     4312    1061.0350  1052.50  1943.25  2020.50       862.4
3 2026-03-31 15:03:20     2695    1082.6750  1083.25  1930.50  2014.75       539.0
4 2026-03-31 15:03:25     3889    1082.7075  1063.75  1947.50  2021.25       777.8


0.0001
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro flink_with_kafka % python3 data_visualizer.py
         window_start  records  avg_latency       p50        p95        p99  throughput
0 2026-03-31 15:09:50     4000  398720.0725  399168.5  405854.25  406467.25       800.0
1 2026-03-31 15:09:55    31796  377488.1900  377825.5  402437.75  405884.25      6359.2
2 2026-03-31 15:10:00    61843  309106.8725  308621.0  352963.50  365139.50     12368.6
3 2026-03-31 15:10:05    49534  229958.0900  229955.5  265490.75  272001.25      9906.8
4 2026-03-31 15:10:10    48746  150414.7925  160901.0  194998.75  201586.00      9749.2


0.00001
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro flink_with_kafka % python3 data_visualizer.py
         window_start  records  avg_latency        p50        p95        p99  throughput
0 2026-03-31 15:15:00     6271  704176.6750  705584.00  714758.50  715653.00      1254.2
1 2026-03-31 15:15:05    39465  677265.2900  678531.50  710491.00  715065.00      7893.0
2 2026-03-31 15:15:10    59067  606617.0600  605542.00  651788.00  665104.25     11813.4
3 2026-03-31 15:15:15    47606  530526.9550  530399.25  565531.00  570360.75      9521.2
4 2026-03-31 15:15:20    46310  452341.4925  464621.00  496332.25  502899.50      9262.0

0.000001
(flink_env) (base) mdtahsinsharif@Mds-MacBook-Pro flink_with_kafka % python3 data_visualizer.py
         window_start  records   avg_latency         p50         p95         p99  throughput
0 2026-03-31 15:51:15    12194  2.877111e+06  2877847.50  2888124.25  2889087.50      2438.8
1 2026-03-31 15:51:20    44802  2.838630e+06  2838855.75  2872025.00  2884554.25      8960.4
2 2026-03-31 15:51:25    49861  2.770306e+06  2770037.00  2806055.00  2817448.00      9972.2
3 2026-03-31 15:51:30    41845  2.706028e+06  2706157.75  2735774.25  2743092.25      8369.0
4 2026-03-31 15:51:35    44684  2.636249e+06  2645751.50  2676765.75  2684499.50      8936.8
'''