import os
import pandas as pd
from typing import Optional

from acs.schema import POP_ALIASES, MINORITY_SHARE_ALIASES, WHITE_NH_SHARE_ALIASES, NAME_ALIASES

def _pick(df: pd.DataFrame, aliases):
    for a in aliases:
        if a in df.columns:
            return a
    return None

def load_acs_county(path: str) -> Optional[pd.DataFrame]:
    path = os.path.abspath(path)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, dtype=str, low_memory=False)
    # normalize GEOID
    if "GEOID" not in df.columns:
        # try alt
        if "geoid" in df.columns:
            df = df.rename(columns={"geoid": "GEOID"})
    if "GEOID" not in df.columns:
        return None
    df["GEOID"] = df["GEOID"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(5).str[-5:]

    name_col = _pick(df, NAME_ALIASES)
    pop_col  = _pick(df, POP_ALIASES)
    min_col  = _pick(df, MINORITY_SHARE_ALIASES)
    wnh_col  = _pick(df, WHITE_NH_SHARE_ALIASES)

    out = pd.DataFrame({"GEOID": df["GEOID"]})
    if name_col:
        out["NAME"] = df[name_col]
    if pop_col:
        out["population"] = pd.to_numeric(df[pop_col].astype(str).str.replace(r"[^0-9\.-]", "", regex=True), errors="coerce")
    if min_col:
        out["minority_share"] = pd.to_numeric(df[min_col], errors="coerce")
    elif wnh_col:
        wnh = pd.to_numeric(df[wnh_col], errors="coerce")
        out["minority_share"] = 1.0 - wnh

    return out

def ensure_acs_table(con, acs_df: Optional[pd.DataFrame]) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS acs_county(
        GEOID VARCHAR PRIMARY KEY,
        NAME VARCHAR,
        population BIGINT,
        minority_share DOUBLE
    );
    """)
    if acs_df is None or acs_df.empty:
        return
    # deterministic replace
    con.execute("DELETE FROM acs_county")
    con.register("acs_df", acs_df)
    con.execute("""
        INSERT INTO acs_county
        SELECT
            GEOID,
            COALESCE(NAME, '') AS NAME,
            CAST(population AS BIGINT) AS population,
            CAST(minority_share AS DOUBLE) AS minority_share
        FROM acs_df
    """)

def refresh_county_stats(con) -> None:
    # Ensure county_stats exists (created by ppp.schema)
    # Replace contents deterministically
    con.execute("DELETE FROM county_stats")

    # Join ppp_agg to acs_county when available
    con.execute("""
    INSERT INTO county_stats
    SELECT
        a.GEOID,
        COALESCE(b.NAME, '') AS NAME,
        CAST(b.population AS BIGINT) AS population,
        CAST(b.minority_share AS DOUBLE) AS minority_share,
        a.loans,
        a.loan_total,
        a.forgiven_total
    FROM ppp_agg_by_geoid a
    LEFT JOIN acs_county b
      ON a.GEOID = b.GEOID
    """)
