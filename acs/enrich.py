"""
acs/enrich.py

Pulls raw ACS county data and produces a clean `acs_county` table
with human-readable column names.
"""

from __future__ import annotations

import duckdb
import pandas as pd


ACS_RENAME_MAP = {
    # Poverty Income Ratio (C17002)
    "C17002_002E": "poverty_under_050",
    "C17002_003E": "poverty_050_099",
    "C17002_004E": "poverty_100_124",
    "C17002_005E": "poverty_125_149",
    "C17002_006E": "poverty_150_184",
    "C17002_007E": "poverty_185_plus",

    # Employment (B23025)
    "B23025_001E": "pop_16_plus",
    "B23025_002E": "labor_force",
    "B23025_003E": "civilian_labor_force",
    "B23025_004E": "employed",
    "B23025_005E": "unemployed",
}


def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns that *look* numeric into numeric,
    preserving None for blanks and non-numeric input.
    """
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def load_raw_acs(filepath: str) -> pd.DataFrame:
    """
    Load raw ACS CSV or TXT, applying renaming and numeric coercion.
    """
    print(f"Loading raw ACS file: {filepath}")

    df = pd.read_csv(filepath, dtype=str)

    # Apply renaming
    rename_cols = {
        col: ACS_RENAME_MAP[col]
        for col in df.columns
        if col in ACS_RENAME_MAP
    }
    df = df.rename(columns=rename_cols)

    # Convert numeric columns
    df = df.apply(pd.to_numeric, errors="ignore")

    print(f"ACS records loaded: {len(df)}")
    print(f"Renamed columns: {list(rename_cols.values())}")

    return df


def build_acs_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """
    Replace `acs_county` with cleaned ACS dataframe.
    """
    print("Building acs_county table...")

    con.execute("DROP TABLE IF EXISTS acs_county")

    con.execute(
        "CREATE TABLE acs_county AS SELECT * FROM df",
        {"df": df},
    )

    n = con.execute("SELECT COUNT(*) FROM acs_county").fetchone()[0]
    print(f"acs_county built. Rows: {n}")
    return n


def refresh_acs_county(con: duckdb.DuckDBPyConnection, filepath: str) -> int:
    """
    Main entrypoint called from app.py
    """
    df = load_raw_acs(filepath)
    return build_acs_table(con, df)


if __name__ == "__main__":
    con = duckdb.connect("data/db/v_finder.duckdb")
    refresh_acs_county(con, "data/raw/your_acs_file.csv")
