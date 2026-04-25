import os
import re
from datetime import datetime
import matplotlib.pyplot as plt

# Dynamically set the root directory to read outputs
root_dir = os.path.join(os.getcwd(), "latency_output/20260319_181929/2026-03-19--18")

# Regex to parse your lines
pattern = re.compile(
    r"window_start=(.*?), window_end=(.*?), records=(\d+), "
    r"avg_latency_ms=([\d\.]+), p50_ms=(\d+), p95_ms=(\d+), p99_ms=(\d+)"
)

times = []
avg = []
p50 = []
p95 = []
p99 = []

# Walk all nested directories
for subdir, _, files in os.walk(root_dir):
    for file in files:
        path = os.path.join(subdir, file)
        
        with open(path, "r") as f:
            for line in f:
                match = pattern.match(line.strip())
                if match:
                    window_end = match.group(2)
                    
                    times.append(datetime.strptime(window_end, "%Y-%m-%d %H:%M:%S"))
                    avg.append(float(match.group(4)))
                    p50.append(float(match.group(5)))
                    p95.append(float(match.group(6)))
                    p99.append(float(match.group(7)))

# Sort by time
data = sorted(zip(times, avg, p50, p95, p99), key=lambda x: x[0])
times, avg, p50, p95, p99 = zip(*data)

# Plot
plt.figure()
plt.plot(times, avg, label="avg_latency")
plt.plot(times, p50, label="p50")
plt.plot(times, p95, label="p95")
plt.plot(times, p99, label="p99")

plt.xlabel("Time")
plt.ylabel("Latency (ms)")
plt.title("Latency over Time")
plt.legend()
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()