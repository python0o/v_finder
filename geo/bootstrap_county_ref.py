"""
generate_county_2025.py
-----------------------

Validates and rewrites county_2025.csv so it has the correct columns
required by the V_FINDER system.

Fixes:
 - Added REQUIRED_BASE_COLS (previously missing)
 - Safe normalization (NAME_NORM)
 - Ensures proper GEOID, STATEFP, COUNTYFP typing
"""

import argparse
import os
import pandas as pd

from utils.normalize import norm_county_name


# ---------------------------------------------------------------------
# REQUIRED columns for county_ref tables across V_FINDER
# ---------------------------------------------------------------------
REQUIRED_BASE_COLS = {
    "GEOID",
    "STATEFP",
    "COUNTYFP",
    "STUSPS",
    "NAME",
    "NAME_NORM",
    "INTPTLAT",
    "INTPTLONG",
}


# ---------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--in",
        dest="inp",
        default=os.path.join("data", "geo", "county_2025.csv")
    )
    ap.add_argument(
        "--out",
        dest="out",
        default=os.path.join("data", "geo", "county_2025.csv")
    )
    args = ap.parse_args()

    df = pd.read_csv(args.inp, dtype=str, encoding="utf-8", na_filter=False)
    df.columns = [c.strip().upper() for c in df.columns]

    # -------------------------------
    # GEOID → STATEFP + COUNTYFP
    # -------------------------------
    if "GEOID" in df.columns:
        df["GEOID"] = df["GEOID"].astype(str).str.zfill(5)

        if "STATEFP" not in df.columns:
            df["STATEFP"] = df["GEOID"].str.slice(0, 2)

        if "COUNTYFP" not in df.columns:
            df["COUNTYFP"] = df["GEOID"].str.slice(2, 5)

    # -------------------------------
    # Normalize NAME → NAME_NORM
    # -------------------------------
    if "NAME_NORM" not in df.columns:
        if "NAME" not in df.columns:
            raise RuntimeError("county_2025.csv missing column NAME")
        df["NAME_NORM"] = df["NAME"].apply(norm_county_name)

    # -------------------------------
    # Validate Required Columns
    # -------------------------------
    missing = REQUIRED_BASE_COLS - set(df.columns)
    if missing:
        raise RuntimeError(f"county_2025.csv missing required columns: {missing}")

    # -------------------------------
    # Write output
    # -------------------------------
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)

    print(f"Wrote {args.out} with columns: {list(df.columns)}")


# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
