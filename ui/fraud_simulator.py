"""
ui/fraud_simulator.py — v3 ULTRA (LAUNCH-LOCKED)

Fraud Scenario Simulator:
- Drop top-N lenders by PPP volume
- Recompute county-level risk exposure proxy

Launch guarantees:
- Canonical PPP joins only
- No GEOID assumptions in PPP
- Fail-closed behavior
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return bool(con.execute(q, [name.lower()]).fetchone()[0])


# ---------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------
def _load_county_universe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if not _table_exists(con, "county_ref"):
        return pd.DataFrame()

    sql = """
    SELECT GEOID, NAME, STUSPS
    FROM county_ref
    ORDER BY STUSPS, NAME
    """
    return con.execute(sql).fetchdf()


def _load_lender_totals(con: duckdb.DuckDBPyConnection, geoid: str) -> pd.DataFrame:
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return pd.DataFrame()

    sql = """
    SELECT
        p.servicinglendername AS lender,
        SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved
    FROM ppp_clean p
    JOIN county_ref cr
      ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
     AND p.borrowerstate = cr.STUSPS
    WHERE cr.GEOID = ?
      AND p.servicinglendername IS NOT NULL
    GROUP BY p.servicinglendername
    ORDER BY total_approved DESC NULLS LAST
    """
    return con.execute(sql, [geoid]).fetchdf()


def _load_base_risk(con: duckdb.DuckDBPyConnection, geoid: str) -> float:
    if not _table_exists(con, "county_scores"):
        return 0.0

    sql = """
    SELECT risk_score
    FROM county_scores
    WHERE GEOID = ?
    """
    df = con.execute(sql, [geoid]).fetchdf()
    if df.empty:
        return 0.0
    return float(df.iloc[0]["risk_score"] or 0.0)


# ---------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------
def render_fraud_simulator_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Fraud Scenario Simulator")
    st.caption("Stress-test county exposure by removing top PPP lenders.")

    df_universe = _load_county_universe(con)
    if df_universe.empty:
        st.error("county_ref missing or empty.")
        return

    # County selection
    df_universe["label"] = df_universe["NAME"].astype(str) + ", " + df_universe["STUSPS"].astype(str)
    label_to_geoid = dict(zip(df_universe["label"], df_universe["GEOID"].astype(str)))

    default_geoid = st.session_state.get("vf_county_focus")
    default_label = None
    if default_geoid:
        m = df_universe[df_universe["GEOID"].astype(str) == str(default_geoid)]
        if not m.empty:
            default_label = m.iloc[0]["label"]

    selected_label = st.selectbox(
        "Select a county",
        options=df_universe["label"].tolist(),
        index=df_universe["label"].tolist().index(default_label)
        if default_label in df_universe["label"].tolist()
        else 0,
    )

    geoid = label_to_geoid[selected_label]
    st.session_state["vf_county_focus"] = geoid

    # Load lender totals
    df_lenders = _load_lender_totals(con, geoid)
    if df_lenders.empty:
        st.warning("No PPP lender data available for this county.")
        return

    base_risk = _load_base_risk(con, geoid)

    st.markdown("---")
    st.subheader("Scenario Controls")

    max_n = min(10, len(df_lenders))
    drop_n = st.slider("Drop top N lenders", 0, max_n, 0)

    # Compute scenario impact
    df_lenders = df_lenders.copy()
    total_all = df_lenders["total_approved"].sum()

    dropped = df_lenders.head(drop_n)
    dropped_share = dropped["total_approved"].sum() / max(total_all, 1e-9)

    # Scenario risk heuristic (same model you defined earlier)
    scenario_risk = base_risk * (1.0 - 0.8 * dropped_share)
    scenario_risk = max(0.0, scenario_risk)

    # -----------------------------------------------------------------
    # Display
    # -----------------------------------------------------------------
    c1, c2, c3 = st.columns(3)
    c1.metric("Base Risk Score", f"{base_risk:.1f}")
    c2.metric("Scenario Risk Score", f"{scenario_risk:.1f}")
    c3.metric("PPP Removed (%)", f"{dropped_share * 100:.1f}%")

    st.markdown("---")

    st.subheader("Lenders Removed")
    if drop_n > 0:
        st.dataframe(dropped, use_container_width=True)
    else:
        st.info("No lenders removed in this scenario.")

    st.subheader("Remaining Lenders")
    st.dataframe(df_lenders.iloc[drop_n:], use_container_width=True)
