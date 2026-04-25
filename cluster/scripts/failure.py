#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# CONFIG
# =========================
BASE_DIRS = {
    "Same node allocation": Path("/srv/nfs/flink/nexmark/failure/q1/p10/"),
    "External node allocation": Path("/srv/nfs/flink/nexmark/failure/q1/p10n2/"),
}

OUT_DIR = Path("./graphs")
OUT_DIR.mkdir(exist_ok=True)

WINDOW = "1s"
MA_WINDOW = 15  # seconds


# =========================
# MANUAL MARKERS (seconds from start)
# =========================
MANUAL_MARKERS = {
    "Same node allocation": {
        "fail_t": 80.0,
        "rec_t": 123.0
    },
    "External node allocation": {
        "fail_t": 62.0,
        "rec_t": 110.0
    }
}


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
                names=["col1", "col2", "price", "event_time", "processing_time", "payload"],
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
# SAFE INDEX MAPPING
# =========================
def time_to_index(t_array, t_seconds):
    """
    Map time -> index safely.
    Clamps to valid range to avoid IndexError.
    """
    if len(t_array) == 0:
        return None

    idx = np.searchsorted(t_array, t_seconds)

    if idx >= len(t_array):
        idx = len(t_array) - 1
    if idx < 0:
        idx = 0

    return idx


# =========================
# MAIN
# =========================
def main():
    print("[START] Failure-recovery analysis (manual markers)")

    fig, axs = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    for ax, (label, path) in zip(axs, BASE_DIRS.items()):
        df = load_data(path)

        if df is None:
            print(f"[WARN] no data for {label}")
            continue

        raw, ma = compute_series(df)
        if raw is None:
            continue

        # time axis (relative)
        t0 = raw.index.min()
        t_raw = (raw.index - t0).total_seconds().values
        t_ma = (ma.index - t0).total_seconds().values

        # manual markers
        cfg = MANUAL_MARKERS.get(label, None)

        fail_i = rec_i = None

        if cfg is not None:
            fail_i = time_to_index(t_raw, cfg["fail_t"])
            rec_i = time_to_index(t_raw, cfg["rec_t"])

        # =========================
        # PLOT
        # =========================
        ax.plot(t_raw, raw.values, alpha=0.25, linewidth=1, label="raw")
        ax.plot(t_ma, ma.values, linewidth=2, label="moving avg")

        # failure marker
        if fail_i is not None:
            tf = t_raw[fail_i]
            ax.axvline(tf, color="red", linestyle="--", linewidth=2)
            ax.text(tf, ax.get_ylim()[1] * 0.7, "failure",
                    rotation=90, color="red")

        # recovery marker
        if rec_i is not None:
            tr = t_raw[rec_i]
            ax.axvline(tr, color="green", linestyle="--", linewidth=2)
            ax.text(tr, ax.get_ylim()[1] * 0.7, "recovery",
                    rotation=90, color="green")

            if fail_i is not None:
                delta = tr - tf
                ax.text(
                    (tf + tr) / 2,
                    ax.get_ylim()[1] * 0.7,
                    f"Δ={delta:.1f}s",
                    ha="center"
                )

        ax.set_title(label)
        ax.set_xlabel("Time (s)")
        ax.grid(True)
        ax.legend()

    axs[0].set_ylabel("Throughput (events/sec)")

    plt.suptitle("Failure recovery (Q1)", y=1.05)
    plt.tight_layout()

    out_file = OUT_DIR / "failure_recovery_q1.png"
    plt.savefig(out_file, dpi=150, bbox_inches="tight")
    plt.show()

    print(f"[DONE] saved {out_file}")


if __name__ == "__main__":
    main()