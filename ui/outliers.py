"""
outliers.py — v7 Ultra (Hybrid Robust)
Unified outlier & anomaly engine for the V-FINDER Fraud Stack.

Key fixes for Launch Stability (Local + Cloud):
- Schema-adaptive county_scores loader (no phantom columns)
- Optional joins to county_ref / acs_county when fields are not in county_scores
- Case-insensitive information_schema checks
- Preserves full outlier model + peer normalization
"""

import duckdb
import pandas as pd
from typing import Dict, Any
import numpy as np


# =====================================================================
# Utility: table/column exists (case-insensitive)
# =====================================================================

def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
    """
    return bool(con.execute(q, [name]).fetchone()[0])


def col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    q = """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?) AND lower(column_name) = lower(?)
    """
    return bool(con.execute(q, [table, col]).fetchone()[0])


# =====================================================================
# Load core scoring data (HYBRID SAFE)
# =====================================================================

def load_county_scores(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Loads a county-level frame for the outlier engine.

    Robust behavior:
    - Works whether identifiers/demographics live in county_scores OR in county_ref/acs_county
    - Never selects a column that doesn't exist
    - Maintains a stable output contract for Mission Control
    """
    if not table_exists(con, "county_scores"):
        return pd.DataFrame()

    # Determine what exists in county_scores
    cs_has = lambda c: col_exists(con, "county_scores", c)

    need_ref = (not cs_has("STUSPS")) or (not cs_has("NAME"))
    need_acs = (not cs_has("Total_Pop")) or (not cs_has("Poverty_Rate")) or (not cs_has("Unemployment_Rate"))

    has_ref = table_exists(con, "county_ref")
    has_acs = table_exists(con, "acs_county")

    join_ref = need_ref and has_ref
    join_acs = need_acs and has_acs

    # Build SELECT list with safe fallbacks
    select_parts = []

    # Required key
    select_parts.append("cs.GEOID")

    # Identifiers (prefer county_scores; fallback to county_ref)
    if cs_has("STUSPS"):
        select_parts.append("cs.STUSPS")
    elif join_ref and col_exists(con, "county_ref", "STUSPS"):
        select_parts.append("cr.STUSPS")
    else:
        select_parts.append("NULL::VARCHAR AS STUSPS")

    if cs_has("NAME"):
        select_parts.append("cs.NAME")
    elif join_ref and col_exists(con, "county_ref", "NAME"):
        select_parts.append("cr.NAME")
    else:
        select_parts.append("NULL::VARCHAR AS NAME")

    # Demographics (prefer county_scores; fallback to acs_county)
    if cs_has("Total_Pop"):
        select_parts.append("cs.Total_Pop")
    elif join_acs and col_exists(con, "acs_county", "Total_Pop"):
        select_parts.append("ac.Total_Pop")
    else:
        select_parts.append("NULL::DOUBLE AS Total_Pop")

    if cs_has("Poverty_Rate"):
        select_parts.append("cs.Poverty_Rate")
    elif join_acs and col_exists(con, "acs_county", "Poverty_Rate"):
        select_parts.append("ac.Poverty_Rate")
    else:
        select_parts.append("NULL::DOUBLE AS Poverty_Rate")

    if cs_has("Unemployment_Rate"):
        select_parts.append("cs.Unemployment_Rate")
    elif join_acs and col_exists(con, "acs_county", "Unemployment_Rate"):
        select_parts.append("ac.Unemployment_Rate")
    else:
        select_parts.append("NULL::DOUBLE AS Unemployment_Rate")

    # PPP metrics + risk outputs (only include if present; otherwise safe defaults)
    def num_or_zero(col: str, alias: str | None = None) -> str:
        if cs_has(col):
            return f"cs.{col}" if not alias else f"cs.{col} AS {alias}"
        return f"0::DOUBLE AS {alias or col}"

    def txt_or_null(col: str, alias: str | None = None) -> str:
        if cs_has(col):
            return f"cs.{col}" if not alias else f"cs.{col} AS {alias}"
        return f"NULL::VARCHAR AS {alias or col}"

    select_parts.append(num_or_zero("ppp_loan_count"))
    select_parts.append(num_or_zero("ppp_current_total"))
    select_parts.append(num_or_zero("ppp_per_capita"))
    select_parts.append(num_or_zero("risk_score"))
    select_parts.append(txt_or_null("risk_tier"))
    select_parts.append(num_or_zero("risk_rank"))
    select_parts.append(num_or_zero("risk_percentile_rank"))
    select_parts.append(num_or_zero("hidden_signal_score"))
    select_parts.append(txt_or_null("hidden_signal_tier"))

    sql = f"""
        SELECT
            {", ".join(select_parts)}
        FROM county_scores cs
        {"LEFT JOIN county_ref cr ON cs.GEOID = cr.GEOID" if join_ref else ""}
        {"LEFT JOIN acs_county ac ON cs.GEOID = ac.GEOID" if join_acs else ""}
    """

    df = con.execute(sql).fetchdf()

    # Type enforcement (best-effort)
    for c in [
        "Total_Pop", "Poverty_Rate", "Unemployment_Rate",
        "ppp_loan_count", "ppp_current_total", "ppp_per_capita",
        "risk_score", "risk_rank", "risk_percentile_rank",
        "hidden_signal_score",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    for c in ["GEOID", "STUSPS", "NAME", "risk_tier", "hidden_signal_tier"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    return df


# =====================================================================
# Load county-lender interaction table
# =====================================================================

def load_county_lender_signals(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if not table_exists(con, "county_lender_signals"):
        return pd.DataFrame()

    sql = """
        SELECT
            GEOID,
            LenderName,
            loan_count,
            dominance_score,
            concentration_z,
            anomaly_flag
        FROM county_lender_signals
    """
    return con.execute(sql).fetchdf()


# =====================================================================
# County-to-county comparison (for radar / side-by-side view)
# =====================================================================

RADAR_FEATURES = [
    "risk_score",
    "ppp_per_capita",
    "ppp_loan_count",
    "Poverty_Rate",
    "Unemployment_Rate",
    "hidden_signal_score",
]


def build_radar_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes radar features to 0–1 scale for Plotly radar charts."""
    df = df.copy()

    for col in RADAR_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
        vals = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        if vals.max() == vals.min():
            df[col + "_norm"] = 0.5
        else:
            df[col + "_norm"] = (vals - vals.min()) / (vals.max() - vals.min())

    return df


# =====================================================================
# Advanced Outlier Detection
# =====================================================================

def detect_population_ppp_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Detect counties with mismatched PPP flow vs population."""
    df = df.copy()

    if "ppp_per_capita" not in df.columns:
        df["ppp_per_capita"] = 0.0

    with np.errstate(divide="ignore", invalid="ignore"):
        sd = float(np.nanstd(df["ppp_per_capita"])) if len(df) else 0.0
        mu = float(np.nanmean(df["ppp_per_capita"])) if len(df) else 0.0
        df["ppp_vs_pop_z"] = 0.0 if sd == 0 else (df["ppp_per_capita"] - mu) / sd

    df["ppp_population_flag"] = df["ppp_vs_pop_z"] > 2.5
    return df


def detect_low_poverty_high_ppp(df: pd.DataFrame) -> pd.DataFrame:
    """Classic fraud indicator: affluent counties with extreme PPP volume."""
    df = df.copy()

    if "Poverty_Rate" not in df.columns:
        df["Poverty_Rate"] = 0.0
    if "ppp_loan_count" not in df.columns:
        df["ppp_loan_count"] = 0.0

    q90 = df["ppp_loan_count"].quantile(0.90) if len(df) else 0.0
    df["affluent_ppp_flag"] = (df["Poverty_Rate"] < 10) & (df["ppp_loan_count"] > q90)
    return df


def detect_unemployment_ppp_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """Low unemployment + high PPP per capita = potential opportunistic fraud."""
    df = df.copy()

    if "Unemployment_Rate" not in df.columns:
        df["Unemployment_Rate"] = 0.0
    if "ppp_per_capita" not in df.columns:
        df["ppp_per_capita"] = 0.0

    q85 = df["ppp_per_capita"].quantile(0.85) if len(df) else 0.0
    df["unemployment_ppp_flag"] = (df["Unemployment_Rate"] < 4) & (df["ppp_per_capita"] > q85)
    return df


# =====================================================================
# Combined Outlier Model
# =====================================================================

def build_outlier_model(df: pd.DataFrame) -> pd.DataFrame:
    """Adds multiple flags & composite anomaly indicator."""
    df = detect_population_ppp_anomalies(df)
    df = detect_low_poverty_high_ppp(df)
    df = detect_unemployment_ppp_conflicts(df)

    df["outlier_score"] = (
        df["ppp_population_flag"].astype(int)
        + df["affluent_ppp_flag"].astype(int)
        + df["unemployment_ppp_flag"].astype(int)
    )

    df["outlier_tier"] = df["outlier_score"].apply(
        lambda s: "SEVERE" if s >= 3 else "HIGH" if s == 2 else "MILD" if s == 1 else "NORMAL"
    )
    return df


# =====================================================================
# Master loading pipeline used by Mission Control
# =====================================================================

def _peer_normalize(df: pd.DataFrame, col: str, group_col: str = "STUSPS") -> pd.Series:
    """Z-score normalize `col` within `group_col` (state peer group)."""
    if col not in df.columns or group_col not in df.columns:
        return pd.Series(0.0, index=df.index)

    g = df.groupby(group_col)[col]
    mu = g.transform("mean")
    sd = g.transform("std").replace(0, np.nan)
    z = (df[col] - mu) / sd
    return z.fillna(0.0)


def load_outliers(con: duckdb.DuckDBPyConnection, use_peer_norm: bool = False) -> pd.DataFrame:
    """Primary outlier frame for Mission Control."""
    df = load_county_scores(con)
    if df.empty:
        return df

    df = build_outlier_model(df)

    if use_peer_norm:
        df["ppp_per_capita_z_peer"] = _peer_normalize(df, "ppp_per_capita")
        df["ppp_loan_count_z_peer"] = _peer_normalize(df, "ppp_loan_count")
        df["peer_outlier_flag"] = (df["ppp_per_capita_z_peer"] > 2.5) | (df["ppp_loan_count_z_peer"] > 2.5)

    # Always provide a boolean flag used by the UI
    df["outlier_flag"] = df.get("outlier_score", 0).astype(int) >= 2

    return df


# =====================================================================
# Utility for county_profile navigation
# =====================================================================

def make_radar_clickmap(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out

    for _, r in df.iterrows():
        geoid = str(r.get("GEOID", ""))
        out[geoid] = {
            "state": str(r.get("STUSPS", "")),
            "name": str(r.get("NAME", "")),
            "risk": float(r.get("risk_score", 0.0)),
            "hs": float(r.get("hidden_signal_score", 0.0)),
            "tier": str(r.get("hidden_signal_tier", "")),
        }
    return out
