# geo/loader.py
from __future__ import annotations

import os
import pandas as pd
import duckdb
import numpy as np

from utils.normalize import norm_county_name

REQUIRED_BASE_COLS = {"GEOID", "STATEFP", "COUNTYFP", "STUSPS", "NAME", "NAME_NORM"}
OPTIONAL_COLS = {"INTPTLAT", "INTPTLONG", "ALAND"}

def load_county_reference(path: str) -> pd.DataFrame:
    """
    Load county reference.

    Minimal required columns:
      GEOID, STATEFP, COUNTYFP, STUSPS, NAME, NAME_NORM

    Optional (enables map centroids):
      INTPTLAT, INTPTLONG, ALAND
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"County reference file not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    sep = '\t' if ext == ".txt" else ','
    if 'gaz' in path.lower():
        sep = '|'

    df = pd.read_csv(path, dtype=str, encoding="utf-8", na_filter=False, sep=sep)
    df.columns = [c.strip().upper() for c in df.columns]

    if "GEOID" in df.columns:
        df["GEOID"] = df["GEOID"].str.replace(r"\D", "", regex=True).str.zfill(5)
        if "STATEFP" not in df.columns:
            df["STATEFP"] = df["GEOID"].str[:2]
        if "COUNTYFP" not in df.columns:
            df["COUNTYFP"] = df["GEOID"].str[2:]

    if "GEOID" not in df.columns:
        if "STATEFP" in df.columns and "COUNTYFP" in df.columns:
            df["GEOID"] = df["STATEFP"].str.zfill(2) + df["COUNTYFP"].str.zfill(3)

    if "NAME_NORM" not in df.columns and "NAME" in df.columns:
        df["NAME_NORM"] = df["NAME"].apply(norm_county_name)

    missing = REQUIRED_BASE_COLS - set(df.columns)
    if missing:
        raise ValueError(f"County reference missing columns: {missing}")

    # Add missing optional columns as np.nan
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # Numeric for optional
    for c in ["INTPTLAT", "INTPTLONG"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "ALAND" in df.columns:
        df["ALAND"] = pd.to_numeric(df["ALAND"], errors="coerce")

    keep = list(REQUIRED_BASE_COLS) + list(OPTIONAL_COLS)
    return df[keep].copy()

def ensure_county_ref(con: duckdb.DuckDBPyConnection, county_ref_csv: str) -> dict:
    df = load_county_reference(county_ref_csv)

    con.execute("DROP TABLE IF EXISTS county_ref")
    con.register("county_ref_df", df)
    con.execute("""
        CREATE TABLE county_ref AS
        SELECT
            GEOID::VARCHAR AS GEOID,
            STATEFP::VARCHAR AS STATEFP,
            COUNTYFP::VARCHAR AS COUNTYFP,
            STUSPS::VARCHAR AS STUSPS,
            NAME::VARCHAR AS NAME,
            NAME_NORM::VARCHAR AS NAME_NORM,
            INTPTLAT::DOUBLE AS INTPTLAT,
            INTPTLONG::DOUBLE AS INTPTLONG,
            ALAND::DOUBLE AS ALAND
        FROM county_ref_df
    """)
    return {"rows": int(len(df))}