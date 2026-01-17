from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st


# -----------------------------------------------------------------------------
# Base loaders
# -----------------------------------------------------------------------------

def load_county_scores(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load county-level risk scores.
    MUST be schema-qualified for Cloud reliability.
    """
    sql = """
    SELECT
        GEOID,
        risk_score,
        risk_tier,
        peer_norm_score,
        peer_rank,
        z_risk_score
    FROM analytics.county_scores
    """
    return con.execute(sql).fetchdf()


def load_county_ref(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load county reference table.
    """
    sql = """
    SELECT
        GEOID,
        NAME,
        STUSPS,
        lat,
        lon
    FROM core.county_ref
    """
    return con.execute(sql).fetchdf()


# -----------------------------------------------------------------------------
# Outliers logic
# -----------------------------------------------------------------------------

def load_outliers(
    con: duckdb.DuckDBPyConnection,
    use_peer_norm: bool = True,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Build outlier frame for Mission Control.

    PPP is OPTIONAL.
    This function must work with analytics-only DBs.
    """

    # --- Load required analytics tables ---
    try:
        df_scores = load_county_scores(con)
        df_ref = load_county_ref(con)
    except Exception as e:
        st.error(f"Failed to load analytics tables: {e}")
        return pd.DataFrame()

    if df_scores.empty or df_ref.empty:
        return pd.DataFrame()

    # --- Merge scores with county metadata ---
    df = df_scores.merge(df_ref, on="GEOID", how="left")

    # --- Select scoring column ---
    score_col = "peer_norm_score" if use_peer_norm and "peer_norm_score" in df.columns else "risk_score"

    df = df[df[score_col].notna()].copy()

    # --- Sort descending risk ---
    df = df.sort_values(score_col, ascending=False)

    # --- Trim ---
    if top_n:
        df = df.head(top_n)

    return df.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Render helper (used by Mission Control)
# -----------------------------------------------------------------------------

def render_outliers_table(
    con: duckdb.DuckDBPyConnection,
    use_peer_norm: bool = True,
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
        "risk_tier",
        "peer_norm_score",
        "peer_rank",
    ]

    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols],
        width="stretch",
        hide_index=True,
    )
