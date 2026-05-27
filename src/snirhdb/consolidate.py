"""
ANA HidroConv CSV Consolidator
================================

Location: src/snirhdb/consolidate.py

Merges paired tier-1 and tier-2 CSV files for the same site-variable into a
single consolidated series, then applies cleanup and consolidation rules.

Input file pattern
------------------
    ANA_HIDROCONV_{site_code}_{variable_code}L{tier}_X_1900U2026.csv

Each site-variable is expected to have up to two files, one per tier:
    ANA_HIROCONV_61315000_D3L1_X_1900U2026.csv  ← tier 1 (raw)
    ANA_HIROCONV_61315000_D3L2_X_1900U2026.csv  ← tier 2 (revised)

Processing pipeline
-------------------
For each site-variable pair:

1. Merge   – concatenate tier 1 and tier 2 series into one DataFrame.
2. Cleanup –
       a. Drop rows where value is NaN.
       b. Deduplicate by (datetime, tier): keep the mean value of duplicates.
3. Consolidate –
       Discard tier 1 rows where a tier 2 observation exists for the same
       datetime. Result is a single authoritative series per datetime.

Output
------
One CSV per site-variable, written to output_dir, named:
    ANA_HIROCONV_{site_code}_{variable_code}_X_1900U2026.csv
    (tier designator removed — series now contains mixed tiers)

The preamble from the tier-2 file is preserved (falling back to tier 1 if
tier 2 is absent). Separator and encoding match the parser output:
semicolon-separated, utf-8-sig.

Usage
-----
    python consolidate.py consolidate.json

Config file (JSON)
------------------
    {
        "input_dir":  "./output",
        "output_dir": "./consolidated",
        "limit":      10,         ← optional, for testing
        "overwrite":  true        ← optional, default true
    }
"""

from __future__ import annotations

import glob
import json
import sys
from itertools import groupby
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_preamble(path: Path) -> tuple[str, int]:
    """Return the raw preamble string and the number of lines to skip."""
    lines = []
    skip = 0
    with path.open(encoding="utf-8-sig") as fh:
        for line in fh:
            if line.startswith("#"):
                lines.append(line.rstrip("\n"))
                skip += 1
            else:
                break
    return "\n".join(lines) + "\n", skip


def _read_csv(path: Path, skip: int) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep=";",
        skiprows=skip,
        parse_dates=["datetime"],
        dtype={"status": "Int8", "tier": "Int8"},
    )


def _site_variable_key(stem: str) -> str | None:
    """
    Extract the site-variable key from a filename stem, stripping the tier.

    ANA_HIROCONV_61315000_D3L1_X_1900U2026  →  61315000_D3
    Returns None if the pattern doesn't match.
    """
    parts = stem.split("_")
    # Expected: ANA HIROCONV {site_code} {varL tier} X {range}
    try:
        site_code = parts[2]
        var_tier  = parts[3]          # e.g. D3L1
        variable  = var_tier[:2]      # e.g. D3
        return f"{site_code}_{variable}"
    except IndexError:
        return None


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def merge(frames: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True)


def cleanup(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows with no measurement
    df = df.dropna(subset=["value"])

    # Deduplicate by (datetime, tier): keep mean value
    df = (
        df.groupby(["datetime", "tier"], as_index=False)
        .agg({"value": "mean", "status": "first"})
    )
    df = df[["datetime", "tier", "status", "value"]]
    return df


def consolidate(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    # For each datetime, keep tier 2 if available, else tier 1
    has_tier2 = df[df["tier"] == 2]["datetime"]
    discarded = ((df["tier"] == 1) & (df["datetime"].isin(has_tier2))).sum()
    mask = ~((df["tier"] == 1) & (df["datetime"].isin(has_tier2)))
    return df[mask].reset_index(drop=True), int(discarded)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(config_path: Path) -> None:
    with config_path.open(encoding="utf-8") as fh:
        config = json.load(fh)

    input_dir  = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    limit      = config.get("limit", None)
    overwrite  = config.get("overwrite", True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Discover and group files by site-variable key
    all_files = sorted(glob.glob(str(input_dir / "*.csv")))
    groups: dict[str, list[Path]] = {}
    for f in all_files:
        fi = Path(f)
        key = _site_variable_key(fi.stem)
        if key is None:
            continue
        groups.setdefault(key, []).append(fi)

    keys = sorted(groups.keys())
    if limit:
        keys = keys[:limit]

    total = len(keys)

    for i, key in enumerate(keys, start=1):
        pct = i / total * 100
        print(f"[{i}/{total}, {pct:.0f}%] {key}", end="  ")

        files = groups[key]

        # Read all tiers for this site-variable
        frames = []
        preamble = None
        for path in sorted(files, reverse=True):   # tier 2 first for preamble
            raw_preamble, skip = _read_preamble(path)
            if preamble is None:
                preamble = raw_preamble
            try:
                df = _read_csv(path, skip)
                frames.append(df)
            except Exception as exc:
                print(f"\n  [error] reading {path.name}: {exc}")
                continue

        if not frames:
            print("→ no data")
            continue

        # Pipeline
        df = merge(frames)
        df = cleanup(df)
        df, discarded = consolidate(df)

        if df.empty:
            print("→ empty after consolidation")
            continue

        # Output filename: drop the Ltier designator
        site_code, variable = key.split("_")
        out_name = f"ANA_HIROCONV_{site_code}_{variable}_X_1900U2026.csv"
        out_path = output_dir / out_name

        if out_path.exists() and not overwrite:
            print("→ skipping: already saved")
            continue

        with out_path.open("w", encoding="utf-8-sig") as fh:
            fh.write(preamble)
            df.to_csv(fh, sep=";", index=False)

        print(f"→ {len(df)} rows  (tier 1 discarded by tier 2: {discarded})")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: consolidate.py <config.json>", file=sys.stderr)
        sys.exit(1)
    main(Path(sys.argv[1]))