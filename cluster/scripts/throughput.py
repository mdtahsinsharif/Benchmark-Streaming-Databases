#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# CONFIG
# =========================
BASE_DIR = Path("/srv/nfs/flink/nexmark/throughput/q11/p5/")
OUT_FILE = Path("./graphs/throughput_single_run.png")

WINDOW = "1s"
MA_WINDOW = 15  # seconds

# boundary (seconds from start)
MARK_T = 17.0


# =========================
# LOAD DATA
# =========================
def load_data(base_dir):
    part_files = list(base_dir.glob("part-*")) + list(base_dir.glob(".part*"))
    dfs = []

    for f in part_files:
        try:
            df = pd.read_csv(
                f,
                header=None,
                names=["col1", "col2", "event_time", "other", "payload", "processing_time"],
                engine="python",
                on_bad_lines="skip"
            )
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)


# =========================
# THROUGHPUT SERIES
# =========================
def compute_series(df):
    df["processing_time"] = pd.to_datetime(df["processing_time"], errors="coerce")
    df = df.dropna(subset=["processing_time"])

    if df.empty:
        return None, None

    df = df.sort_values("processing_time").set_index("processing_time")

    raw = df.resample(WINDOW).size()
    raw = raw.asfreq(WINDOW, fill_value=0)

    ma = raw.rolling(MA_WINDOW, min_periods=1).mean()

    return raw, ma


# =========================
# MAIN
# =========================
def main():
    print("[START] single-run throughput plot")

    df = load_data(BASE_DIR)
    if df is None:
        print("[ERROR] no data found")
        return

    raw, ma = compute_series(df)
    if raw is None:
        print("[ERROR] empty time series")
        return

    # relative time axis
    t0 = raw.index.min()
    t_raw = (raw.index - t0).total_seconds().values
    t_ma = (ma.index - t0).total_seconds().values

    # boundary index
    idx = np.searchsorted(t_raw, MARK_T)
    idx = min(max(idx, 0), len(t_raw) - 1)
    tm = t_raw[idx]

    # =========================
    # PLOT
    # =========================
    plt.figure(figsize=(10, 5))

    # signals
    plt.plot(t_raw, raw.values, color="#9ecae1", alpha=0.4, linewidth=1, label="raw")
    plt.plot(t_ma, ma.values, color="#08519c", linewidth=2, label="moving avg")

    ymax = max(ma.values)

    # =========================
    # REGIONS (colored)
    # =========================
    # warmup
    plt.axvspan(0, tm, color="#fdae61", alpha=0.25)
    plt.text(
        tm * 0.5,
        ymax * 0.2,
        "warmup",
        ha="center",
        va="center",
        color="#7f2704"
    )

    # steady-state
    plt.axvspan(tm, t_raw[-1], color="#74add1", alpha=0.20)
    plt.text(
        tm + (t_raw[-1] - tm) * 0.5,
        ymax * 0.2,
        "steady state",
        ha="center",
        va="center",
        color="#08306b"
    )

    # boundary marker
    plt.axvline(tm, color="black", linestyle="--", linewidth=2)
    plt.text(tm, ymax * 0.40, f"t={tm:.1f}s", rotation=90, ha="right")

    # formatting
    plt.title("Throughput (Q11, 5 cores)")
    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.savefig(OUT_FILE, dpi=150, bbox_inches="tight")
    plt.show()

    print(f"[DONE] saved {OUT_FILE}")


if __name__ == "__main__":
    main()