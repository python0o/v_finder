"""
ui/outliers.py — v9 ULTRA (FINAL)

Adds PEER NORMALIZATION to county anomaly detection.
Peer groups are built from demographic similarity, not geography.

Peer dimensions:
- Population
- Poverty rate
- Unemployment rate
"""

import duckdb
import pandas as pd
import numpy as np


# =====================================================================
# Utilities
# =====================================================================

def table_exists(con, name: str) -> bool:
    try:
        return bool(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [name.lower()],
            ).fetchone()[0]
        )
    except Exception:
        return False


def zscore(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    mu = s.mean(skipna=True)
    sd = s.std(skipna=True, ddof=0)
    if sd <= 1e-9 or pd.isna(sd):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (s - mu) / sd


# =====================================================================
# Load base data
# =====================================================================

def load_county_scores(con) -> pd.DataFrame:
    sql = """
        SELECT
            GEOID,
            STUSPS,
            NAME,
            Total_Pop,
            Poverty_Rate,
            Unemployment_Rate,
            ppp_loan_count,
            ppp_current_total,
            ppp_per_capita,
            risk_score,
            risk_tier,
            hidden_signal_score
        FROM county_scores
    """
    return con.execute(sql).fetchdf()


# =====================================================================
# Peer grouping
# =====================================================================

def assign_peer_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign demographic peer buckets.
    """

    df = df.copy()

    # Log population for scale stability
    df["log_pop"] = np.log10(df["Total_Pop"].clip(lower=1))

    df["peer_pop"] = pd.qcut(df["log_pop"], 4, labels=False, duplicates="drop")
    df["peer_poverty"] = pd.qcut(df["Poverty_Rate"], 3, labels=False, duplicates="drop")
    df["peer_unemp"] = pd.qcut(df["Unemployment_Rate"], 3, labels=False, duplicates="drop")

    df["peer_group"] = (
        df["peer_pop"].astype(str)
        + "-"
        + df["peer_poverty"].astype(str)
        + "-"
        + df["peer_unemp"].astype(str)
    )

    return df


# =====================================================================
# Peer-normalized anomaly detection
# =====================================================================

def peer_normalized_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute anomaly flags *within peer groups*.
    """

    df = df.copy()
    df["ppp_peer_z"] = 0.0
    df["peer_outlier_flag"] = False

    for peer, g in df.groupby("peer_group"):
        if len(g) < 10:
            continue  # insufficient peers

        z = zscore(g["ppp_per_capita"])
        df.loc[g.index, "ppp_peer_z"] = z
        df.loc[g.index, "peer_outlier_flag"] = z.abs() >= 2.5

    return df


# =====================================================================
# Global anomaly detection (legacy)
# =====================================================================

def global_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ppp_global_z"] = zscore(df["ppp_per_capita"])
    df["global_outlier_flag"] = df["ppp_global_z"].abs() >= 2.5
    return df


# =====================================================================
# Composite model
# =====================================================================

def build_outliers(df: pd.DataFrame, use_peer_norm: bool = True) -> pd.DataFrame:
    df = assign_peer_group(df)
    df = global_flags(df)

    if use_peer_norm:
        df = peer_normalized_flags(df)
        df["outlier_flag"] = df["peer_outlier_flag"]
        df["outlier_basis"] = "PEER"
    else:
        df["outlier_flag"] = df["global_outlier_flag"]
        df["outlier_basis"] = "GLOBAL"

    df["outlier_tier"] = df["outlier_flag"].map(
        {True: "SEVERE", False: "NORMAL"}
    )

    return df


# =====================================================================
# Public API
# =====================================================================

def load_outliers(con, use_peer_norm: bool = True) -> pd.DataFrame:
    if not table_exists(con, "county_scores"):
        return pd.DataFrame()

    df = load_county_scores(con)
    if df.empty:
        return df

    df = build_outliers(df, use_peer_norm=use_peer_norm)
    return df
