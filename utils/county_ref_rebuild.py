# utils/county_ref_rebuild.py
#
# Rebuilds county_ref from data/geo/county_2025.csv
# Canonical schema:
#   GEOID, STATEFP, COUNTYFP, STUSPS, NAME, NAME_NORM, INTPTLAT, INTPTLONG, ALAND
#
# Currently this is AL-only (67 rows), but the logic will work
# for a full US county file if you later expand county_2025.csv.

from __future__ import annotations

import os
import sys
from typing import Optional

import duckdb
import pandas as pd


def project_root() -> str:
    """Return the V_FINDER project root (parent of utils/)."""
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.dirname(here)


ROOT = project_root()
DB_PATH = os.path.join(ROOT, "data", "db", "v_finder.duckdb")
COUNTY_CSV = os.path.join(ROOT, "data", "geo", "county_2025.csv")


def normalize_county_name(name: Optional[str]) -> str:
    """
    Normalize county name into a join key compatible with PPP county_norm:

    - Uppercase
    - Strip leading/trailing whitespace
    - Remove obvious trailing descriptors (COUNTY, PARISH, BOROUGH, etc.)
    - Replace hyphens with spaces
    - Standardize SAINT / SAINTE / ST. / STE.
    - Collapse multiple spaces
    """
    if name is None:
        return ""

    n = str(name).upper().strip()

    # Remove quotes
    n = n.replace("'", "")

    # Hyphens to spaces (MIAMI-DADE -> MIAMI DADE)
    n = n.replace("-", " ")

    # Standardize saint variations
    n = n.replace("SAINTE ", "STE ")
    n = n.replace("SAINT ", "ST ")
    n = n.replace("STE. ", "STE ")
    n = n.replace("ST. ", "ST ")

    # Strip common suffixes
    SUFFIXES = [
        " COUNTY",
        " PARISH",
        " MUNICIPALITY",
        " MUNICIPIO",
        " CITY AND BOROUGH",
        " CITY AND",
        " BOROUGH",
        " CENSUS AREA",
    ]
    for suf in SUFFIXES:
        if n.endswith(suf):
            n = n[: -len(suf)]
            break

    # Collapse whitespace
    n = " ".join(n.split())
    return n


def load_source_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"county_2025.csv not found at: {path}")

    df = pd.read_csv(path, dtype={"GEOID": "Int64", "STUSPS": "string", "NAME": "string"})

    required = {"GEOID", "STUSPS", "NAME"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"county_2025.csv is missing required columns: {sorted(missing)}")

    return df


def build_county_ref_frame(src: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw CSV frame into the canonical county_ref schema.
    """
    df = src.copy()

    # GEOID comes in as e.g. 1001 (county FIPS); pad to 5 digits.
    df["GEOID_STR"] = df["GEOID"].apply(lambda x: f"{int(x):05d}" if pd.notna(x) else None)

    df["STATEFP"] = df["GEOID_STR"].str.slice(0, 2)
    df["COUNTYFP"] = df["GEOID_STR"].str.slice(2, 5)

    # Canonical GEOID string column
    df["GEOID_CANON"] = df["GEOID_STR"]

    # Normalized name
    df["NAME_NORM"] = df["NAME"].apply(normalize_county_name)

    # Geometry/area placeholders (kept for map compatibility)
    df["INTPTLAT"] = pd.Series([None] * len(df), dtype="float64")
    df["INTPTLONG"] = pd.Series([None] * len(df), dtype="float64")
    df["ALAND"] = pd.Series([None] * len(df), dtype="float64")

    # Reorder/rename to match final schema
    out = df[[
        "GEOID_CANON",
        "STATEFP",
        "COUNTYFP",
        "STUSPS",
        "NAME",
        "NAME_NORM",
        "INTPTLAT",
        "INTPTLONG",
        "ALAND",
    ]].rename(columns={"GEOID_CANON": "GEOID"})

    return out


def rebuild_county_ref() -> None:
    print("=== county_ref_rebuild ===")
    print(f"Project root: {ROOT}")
    print(f"DB path     : {DB_PATH}")
    print(f"Source CSV  : {COUNTY_CSV}")

    src = load_source_csv(COUNTY_CSV)
    print(f"Loaded county_2025.csv with {len(src):,} rows")

    ref_df = build_county_ref_frame(src)
    print("Transformed to canonical county_ref schema:")
    print(ref_df.head())

    # Connect and replace county_ref
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS county_ref")
    con.execute("CREATE TABLE county_ref AS SELECT * FROM ref_df", {"ref_df": ref_df})

    # Basic sanity check
    check = con.execute("SELECT COUNT(*) AS n, MIN(GEOID) AS min_geoid, MAX(GEOID) AS max_geoid FROM county_ref").fetchdf()
    print("\n=== county_ref summary ===")
    print(check.to_string(index=False))

    # Optional: quick look at keys for AL
    sample = con.execute("""
        SELECT STUSPS, COUNT(*) AS n
        FROM county_ref
        GROUP BY 1
        ORDER BY STUSPS
    """).fetchdf()
    print("\n=== county_ref state coverage ===")
    print(sample.to_string(index=False))

    con.close()
    print("\ncounty_ref rebuild COMPLETE.")


if __name__ == "__main__":
    try:
        rebuild_county_ref()
    except Exception as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
