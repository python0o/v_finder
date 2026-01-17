"""
outliers.py — v7 Ultra
Unified outlier & anomaly engine for the V-FINDER Fraud Stack.

Supports:
    - County-level anomaly scoring (PPP flow vs ACS)
    - Lender-level dominance deviations
    - Cross-county comparisons
    - Radar-mode JSON shaping
"""

import duckdb
import pandas as pd
from typing import Dict, Any, List
import numpy as np


# =====================================================================
# Utility: table exists
# =====================================================================

def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
    """
    return bool(con.execute(q, [name]).fetchone()[0])


# =====================================================================
# Load core scoring data
# =====================================================================

def load_county_scores(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load county scoring data for Mission Control/outliers.

    V1 reality: county_scores schema can vary depending on whether you're using
    a monolithic DB with schemas (analytics/core) or legacy/split layouts.

    This loader is intentionally defensive:
    - Reads from the compatibility view/table `county_scores` (created in app.py)
    - Normalizes/derives the fields needed by the outlier engine
    """

    if not table_exists(con, "county_scores"):
        return pd.DataFrame()

    df = con.execute("SELECT * FROM county_scores").fetchdf()
    if df is None or df.empty:
        return pd.DataFrame()

    # Normalize column access (DuckDB is case-insensitive, pandas is not)
    lower_to_actual = {c.lower(): c for c in df.columns}

    def _pick(*names: str):
        for n in names:
            c = lower_to_actual.get(n.lower())
            if c is not None:
                return c
        return None

    # Required identifiers
    c_geoid = _pick("GEOID", "geoid")
    if not c_geoid:
        # Without GEOID we cannot join/navigate; return empty to avoid false output
        return pd.DataFrame()

    c_st = _pick("STUSPS", "stusps", "state", "borrowerstate")
    c_name = _pick("NAME", "name", "county_name")

    # Core metrics
    c_pop = _pick("Total_Pop", "total_pop", "population", "pop")
    c_pov = _pick("Poverty_Rate", "poverty_rate")
    c_unemp = _pick("Unemployment_Rate", "unemployment_rate")

    c_ppp_cnt = _pick("ppp_loan_count", "loan_count")
    c_ppp_tot = _pick("ppp_current_total", "ppp_total", "ppp_amount_total")
    c_ppp_pc = _pick("ppp_per_capita", "ppp_pc")

    c_risk = _pick("risk_score", "score", "risk")
    c_risk_tier = _pick("risk_tier")

    c_hs = _pick("hidden_signal_score", "hidden_signal")
    c_hs_tier = _pick("hidden_signal_tier")

    out = pd.DataFrame()
    out["GEOID"] = df[c_geoid].astype(str)
    out["STUSPS"] = df[c_st].astype(str) if c_st else ""
    out["NAME"] = df[c_name].astype(str) if c_name else out["GEOID"]

    # Numerics (best-effort)
    def _num(col, default=0.0):
        if not col:
            return pd.Series([default] * len(df))
        return pd.to_numeric(df[col], errors="coerce").fillna(default)

    out["Total_Pop"] = _num(c_pop, default=float("nan"))
    out["Poverty_Rate"] = _num(c_pov, default=float("nan"))
    out["Unemployment_Rate"] = _num(c_unemp, default=float("nan"))

    out["ppp_loan_count"] = _num(c_ppp_cnt, default=0.0)
    out["ppp_current_total"] = _num(c_ppp_tot, default=0.0)

    if c_ppp_pc:
        out["ppp_per_capita"] = _num(c_ppp_pc, default=0.0)
    else:
        # Derive if possible; otherwise 0.
        with np.errstate(divide="ignore", invalid="ignore"):
            out["ppp_per_capita"] = (out["ppp_current_total"] / out["Total_Pop"]).replace([np.inf, -np.inf], np.nan)
        out["ppp_per_capita"] = out["ppp_per_capita"].fillna(0.0)

    out["risk_score"] = _num(c_risk, default=0.0)
    out["risk_tier"] = df[c_risk_tier].astype(str) if c_risk_tier else "UNKNOWN"

    out["hidden_signal_score"] = _num(c_hs, default=0.0)
    out["hidden_signal_tier"] = df[c_hs_tier].astype(str) if c_hs_tier else "UNKNOWN"

    return out


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
    """
    Normalizes radar features to 0–1 scale for Plotly radar charts.
    """
    df = df.copy()

    for col in RADAR_FEATURES:
        vals = df[col].astype(float).fillna(0)
        if vals.max() == vals.min():
            df[col + "_norm"] = 0.5
        else:
            df[col + "_norm"] = (vals - vals.min()) / (vals.max() - vals.min())

    return df


# =====================================================================
# Advanced Outlier Detection
# =====================================================================

def detect_population_ppp_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect counties with mismatched PPP flow vs population.
    """
    df = df.copy()

    df["ppp_vs_pop_z"] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'):
        df["ppp_vs_pop_z"] = (
            (df["ppp_per_capita"] - df["ppp_per_capita"].mean())
            / df["ppp_per_capita"].std()
        )

    df["ppp_population_flag"] = df["ppp_vs_pop_z"] > 2.5

    return df


def detect_low_poverty_high_ppp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classic fraud indicator: affluent counties with extreme PPP volume.
    """
    df = df.copy()

    df["affluent_ppp_flag"] = (
        (df["Poverty_Rate"] < 10) &
        (df["ppp_loan_count"] > df["ppp_loan_count"].quantile(0.90))
    )

    return df


def detect_unemployment_ppp_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Low unemployment + high PPP per capita = potential opportunistic fraud.
    """
    df = df.copy()
    df["unemployment_ppp_flag"] = (
        (df["Unemployment_Rate"] < 4) &
        (df["ppp_per_capita"] > df["ppp_per_capita"].quantile(0.85))
    )
    return df


# =====================================================================
# Combined Outlier Model
# =====================================================================

def build_outlier_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds multiple flags & composite anomaly indicator.
    """
    df = detect_population_ppp_anomalies(df)
    df = detect_low_poverty_high_ppp(df)
    df = detect_unemployment_ppp_conflicts(df)

    df["outlier_score"] = (
        df["ppp_population_flag"].astype(int)
        + df["affluent_ppp_flag"].astype(int)
        + df["unemployment_ppp_flag"].astype(int)
    )

    df["outlier_tier"] = df["outlier_score"].apply(
        lambda s:
            "SEVERE" if s >= 3 else
            "HIGH"    if s == 2 else
            "MILD"    if s == 1 else
            "NORMAL"
    )

    return df


# =====================================================================
# Master loading pipeline used by Mission Control
# =====================================================================

def _peer_normalize(df: pd.DataFrame, col: str, group_col: str = "STUSPS") -> pd.Series:
    """Z-score normalize `col` within `group_col` (state peer group).

    Returns a float series aligned to df index.
    """
    g = df.groupby(group_col)[col]
    mu = g.transform("mean")
    sd = g.transform("std").replace(0, np.nan)
    z = (df[col] - mu) / sd
    return z.fillna(0.0)


def load_outliers(con: duckdb.DuckDBPyConnection, use_peer_norm: bool = False) -> pd.DataFrame:
    """Primary outlier frame for Mission Control.

    Args:
        use_peer_norm: If True, compute anomaly signals relative to *state peers*
                       (STUSPS) rather than nationally. This makes the table more
                       useful for within-state triage.
    """
    df = load_county_scores(con)
    if df.empty:
        return df

    df = build_outlier_model(df)

    # Optional peer normalization overlays (does not change fraud.score outputs)
    if use_peer_norm:
        # Preserve original for UI toggles/inspection
        df["ppp_per_capita_z_peer"] = _peer_normalize(df, "ppp_per_capita")
        df["ppp_loan_count_z_peer"] = _peer_normalize(df, "ppp_loan_count")
        # A slightly stricter peer outlier flag
        df["peer_outlier_flag"] = (df["ppp_per_capita_z_peer"] > 2.5) | (df["ppp_loan_count_z_peer"] > 2.5)

    # Always provide a boolean flag used by the UI (avoid df.get(...)=False pitfalls)
    df["outlier_flag"] = df["outlier_score"].astype(int) >= 2

    return df


# =====================================================================
# Utility for county_profile navigation
# =====================================================================

def make_radar_clickmap(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Returns:
        {
            GEOID: {
                "state": ...,
                "name": ...,
                "risk": ...,
                "hs": ...,
                "tier": ...
            }
        }
    """
    out = {}

    for _, r in df.iterrows():
        out[r["GEOID"]] = {
            "state": r["STUSPS"],
            "name": r["NAME"],
            "risk": float(r["risk_score"]),
            "hs": float(r["hidden_signal_score"]),
            "tier": r["hidden_signal_tier"],
        }

    return out
