#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# CONFIG
# =========================
BASE_DIR = Path("/srv/nfs/spark/nexmark/failure/q1/p10/")
OUT_FILE = Path("./graphs/events_per_100ms.png")
OUT_FILE.parent.mkdir(exist_ok=True)

BIN = "100ms"


# =========================
# LOAD DATA (unchanged)
# =========================
def load_data(base_dir: Path) -> pd.DataFrame:
    files = list(base_dir.glob("part-*")) + list(base_dir.glob(".part*"))
    if not files:
        return None

    dfs = []

    for f in files:
        try:
            df = pd.read_csv(
                f,
                usecols=["dateTime"],
                dtype={"dateTime": "string"},
                engine="c",
                on_bad_lines="skip",
            )
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)

    df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce", utc=True)
    df = df.dropna(subset=["dateTime"])

    return df


# =========================
# BUILD 100ms AGGREGATION
# =========================
def build_counts(df: pd.DataFrame) -> pd.Series:
    df["time_bin"] = df["dateTime"].dt.floor(BIN)

    counts = df.groupby("time_bin").size()

    full_index = pd.date_range(
        start=counts.index.min(),
        end=counts.index.max(),
        freq=BIN,
        tz="UTC",
    )

    counts = counts.reindex(full_index, fill_value=0)

    return counts


# =========================
# MAIN
# =========================
def main():
    df = load_data(BASE_DIR)

    if df is None or df.empty:
        print("No data found")
        return

    counts = build_counts(df)

    # diagnostics
    print("Total events:", len(df))
    print("Total 100ms bins:", len(counts))
    print("Max events / 100ms:", counts.max())
    print("Non-zero bins:", (counts > 0).sum())

    # plot
    plt.figure(figsize=(14, 4))
    plt.plot(counts.index, counts.values, linewidth=0.8)

    plt.title("Event rate (100ms bins)")
    plt.xlabel("dateTime (UTC)")
    plt.ylabel("events / 100ms")
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(OUT_FILE, dpi=150)
    plt.show()

    print(f"[DONE] saved: {OUT_FILE}")


if __name__ == "__main__":
    main()