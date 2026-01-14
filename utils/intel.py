"""
intel.py — higher-order PPP + ACS intelligence utilities.

Side-effect free: everything takes a DuckDB connection and returns
pandas DataFrames for the UI to render.
"""

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Core loader
# ---------------------------------------------------------------------------

def load_county_scores_for_intel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Return a county_scores DataFrame with the columns needed for analytics.

    Expected minimal schema:
        GEOID, STUSPS, NAME,
        risk_score, risk_tier,
        ppp_loan_count, ppp_current_total, ppp_per_capita,
        Total_Pop, Poverty_Rate, Unemployment_Rate

    If the table or columns are missing, an empty DataFrame is returned.
    """
    try:
        df = con.execute(
            """
            SELECT
                GEOID,
                STUSPS,
                NAME,
                risk_score,
                risk_tier,
                ppp_loan_count,
                ppp_current_total,
                ppp_per_capita,
                Total_Pop,
                Poverty_Rate,
                Unemployment_Rate
            FROM county_scores
            """
        ).fetchdf()
    except Exception:
        return pd.DataFrame()

    numeric_cols = [
        "risk_score",
        "ppp_loan_count",
        "ppp_current_total",
        "ppp_per_capita",
        "Total_Pop",
        "Poverty_Rate",
        "Unemployment_Rate",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Hidden-structure / anomaly-style analytics
# ---------------------------------------------------------------------------

def _zscore(series: pd.Series) -> pd.Series:
    """
    Simple z-score helper that is robust to zero / NaN std-dev.
    """
    s = pd.to_numeric(series, errors="coerce")
    mu = s.mean()
    sigma = s.std(ddof=0)

    if sigma is None or pd.isna(sigma) or sigma == 0:
        return pd.Series(0.0, index=s.index)

    return (s - mu) / sigma


def compute_hidden_signals(
    con: duckdb.DuckDBPyConnection,
    top_n: int = 50,
    min_population: int = 5000,
) -> pd.DataFrame:
    """
    Build a "hidden signal" view of counties whose PPP footprint looks
    structurally unusual relative to ACS distress.

    Not a fraud verdict — this is a *triage list* of interesting points.

    Heuristic components:
      - PPP per capita vs poverty / unemployment (structural gap)
      - Loan density (loans per 1k residents)
      - Average loan size

    Returns top_n counties with a composite Hidden Risk Index.
    """
    base = load_county_scores_for_intel(con)
    if base.empty:
        return base

    df = base.copy()

    # Guard: only counties with enough population to be meaningful
    if "Total_Pop" in df.columns:
        df = df[df["Total_Pop"].fillna(0) >= float(min_population)].copy()

    # Derived measures
    df["loans_per_1k_residents"] = 0.0
    if "ppp_loan_count" in df.columns and "Total_Pop" in df.columns:
        denom = df["Total_Pop"].replace(0, pd.NA)
        df["loans_per_1k_residents"] = df["ppp_loan_count"].fillna(0) * 1000.0 / denom

    df["avg_loan_size"] = 0.0
    if "ppp_current_total" in df.columns and "ppp_loan_count" in df.columns:
        denom_loans = df["ppp_loan_count"].replace(0, pd.NA)
        df["avg_loan_size"] = df["ppp_current_total"].fillna(0) / denom_loans

    # Z-scores for intensity and distress
    df["ppp_per_capita_z"] = 0.0
    if "ppp_per_capita" in df.columns:
        df["ppp_per_capita_z"] = _zscore(df["ppp_per_capita"])

    df["poverty_z"] = 0.0
    if "Poverty_Rate" in df.columns:
        df["poverty_z"] = _zscore(df["Poverty_Rate"])

    df["unemp_z"] = 0.0
    if "Unemployment_Rate" in df.columns:
        df["unemp_z"] = _zscore(df["Unemployment_Rate"])

    df["loan_density_z"] = _zscore(df["loans_per_1k_residents"])
    df["avg_loan_z"] = _zscore(df["avg_loan_size"])

    # Structural gap: "more PPP per capita than distress would predict"
    df["structural_gap_z"] = (
        df["ppp_per_capita_z"] - (0.6 * df["poverty_z"] + 0.4 * df["unemp_z"])
    )

    # Size / concentration signal
    df["size_concentration_z"] = df["loan_density_z"] + df["avg_loan_z"]

    # Composite hidden index
    df["hidden_risk_index"] = df["structural_gap_z"] + 0.7 * df["size_concentration_z"]

    df_sorted = df.sort_values("hidden_risk_index", ascending=False).head(int(top_n)).copy()

    # User-friendly labels
    df_sorted["PPP per Capita"] = df_sorted.get("ppp_per_capita")
    df_sorted["Loans per 1k Residents"] = df_sorted.get("loans_per_1k_residents")
    df_sorted["Avg Loan Size"] = df_sorted.get("avg_loan_size")
    df_sorted["Poverty Rate"] = df_sorted.get("Poverty_Rate")
    df_sorted["Unemployment Rate"] = df_sorted.get("Unemployment_Rate")
    df_sorted["Hidden Risk Index"] = df_sorted.get("hidden_risk_index")

    cols = [
        "GEOID",
        "STUSPS",
        "NAME",
        "risk_tier",
        "risk_score",
        "PPP per Capita",
        "Loans per 1k Residents",
        "Avg Loan Size",
        "Poverty Rate",
        "Unemployment Rate",
        "Hidden Risk Index",
    ]

    cols_present = [c for c in cols if c in df_sorted.columns]

    return df_sorted[cols_present].reset_index(drop=True)
