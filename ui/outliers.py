from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st


# -----------------------------------------------------------------------------
# Base loaders
# -----------------------------------------------------------------------------

def load_county_scores(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load county-level analytics scores (V1 schema).
    """
    sql = """
    SELECT
        GEOID,
        risk_score,
        hidden_signal_score,
        loans_per_1k,
        ppp_loan_count,
        ppp_current_total
    FROM analytics.county_scores
    """
    return con.execute(sql).fetchdf()


def load_county_ref(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load county reference metadata (V1 schema).
    """
    sql = """
    SELECT
        GEOID,
        NAME,
        STUSPS
    FROM core.county_ref
    """
    return con.execute(sql).fetchdf()


# -----------------------------------------------------------------------------
# Outliers logic
# -----------------------------------------------------------------------------

def load_outliers(
    con: duckdb.DuckDBPyConnection,
    use_peer_norm: bool = False,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Build outlier frame for Mission Control.

    V1 behavior:
    - Primary ranking: risk_score
    - Secondary signal: hidden_signal_score
    - PPP metrics already aggregated (counts/totals)
    """

    try:
        df_scores = load_county_scores(con)
        df_ref = load_county_ref(con)
    except Exception as e:
        st.error(f"Failed to load analytics tables: {e}")
        return pd.DataFrame()

    if df_scores.empty or df_ref.empty:
        return pd.DataFrame()

    # Merge analytics with county metadata
    df = df_scores.merge(df_ref, on="GEOID", how="left")

    # Rank strictly by risk_score (V1 canonical)
    df = df[df["risk_score"].notna()].copy()
    df = df.sort_values("risk_score", ascending=False)

    if top_n:
        df = df.head(top_n)

    return df.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Render helper (used by Mission Control)
# -----------------------------------------------------------------------------

def render_outliers_table(
    con: duckdb.DuckDBPyConnection,
    use_peer_norm: bool = False,
):
    """
    Render the Outliers table in the UI.
    """
    df = load_outliers(con, use_peer_norm=use_peer_norm)

    if df.empty:
        st.info("No outlier counties available.")
        return

    display_cols = [
        "NAME",
        "STUSPS",
        "risk_score",
        "hidden_signal_score",
        "loans_per_1k",
        "ppp_loan_count",
        "ppp_current_total",
    ]

    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols],
        width="stretch",
        hide_index=True,
    )
