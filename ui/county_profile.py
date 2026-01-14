"""
ui/county_profile.py — v9 ULTRA (LAUNCH-LOCKED)

County deep-dive profile:
- Risk overview
- PPP metrics
- Top lenders
- Borrower sample

Launch guarantees:
- No PPP → GEOID assumptions
- Canonical join via (county_norm, borrowerstate) → county_ref
- Lowercase PPP column names only
- Fail-closed behavior (no runtime crashes)
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
def _load_county_header(con: duckdb.DuckDBPyConnection, geoid: str) -> pd.Series | None:
    if not _table_exists(con, "county_ref"):
        return None

    sql = """
    SELECT
        GEOID,
        NAME,
        STUSPS
    FROM county_ref
    WHERE GEOID = ?
    """
    df = con.execute(sql, [geoid]).fetchdf()
    return df.iloc[0] if not df.empty else None


def _load_county_risk(con: duckdb.DuckDBPyConnection, geoid: str) -> pd.Series | None:
    if not _table_exists(con, "county_scores"):
        return None

    sql = """
    SELECT
        risk_score,
        risk_tier,
        risk_percentile_rank
    FROM county_scores
    WHERE GEOID = ?
    """
    df = con.execute(sql, [geoid]).fetchdf()
    return df.iloc[0] if not df.empty else None


def _load_ppp_summary(con: duckdb.DuckDBPyConnection, geoid: str) -> pd.Series | None:
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return None

    sql = """
    SELECT
        COUNT(*) AS loan_count,
        SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved,
        AVG(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS avg_loan,
        SUM(TRY_CAST(p.jobsreported AS DOUBLE)) AS jobs_reported
    FROM ppp_clean p
    JOIN county_ref cr
      ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
     AND p.borrowerstate = cr.STUSPS
    WHERE cr.GEOID = ?
    """
    df = con.execute(sql, [geoid]).fetchdf()
    return df.iloc[0] if not df.empty else None


def _load_top_lenders(con: duckdb.DuckDBPyConnection, geoid: str, limit: int = 10) -> pd.DataFrame:
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return pd.DataFrame()

    sql = """
    SELECT
        p.servicinglendername AS lender,
        COUNT(*) AS loan_count,
        SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved
    FROM ppp_clean p
    JOIN county_ref cr
      ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
     AND p.borrowerstate = cr.STUSPS
    WHERE cr.GEOID = ?
      AND p.servicinglendername IS NOT NULL
    GROUP BY p.servicinglendername
    ORDER BY total_approved DESC NULLS LAST
    LIMIT ?
    """
    return con.execute(sql, [geoid, limit]).fetchdf()


def _load_borrowers(con: duckdb.DuckDBPyConnection, geoid: str, limit: int = 50) -> pd.DataFrame:
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return pd.DataFrame()

    sql = """
    SELECT
        p.borrowername,
        p.borrowercity,
        p.borrowerstate,
        p.county_norm,
        TRY_CAST(p.currentapprovalamount AS DOUBLE) AS currentapprovalamount,
        p.naicscode,
        TRY_CAST(p.jobsreported AS DOUBLE) AS jobsreported
    FROM ppp_clean p
    JOIN county_ref cr
      ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
     AND p.borrowerstate = cr.STUSPS
    WHERE cr.GEOID = ?
    ORDER BY currentapprovalamount DESC NULLS LAST
    LIMIT ?
    """
    return con.execute(sql, [geoid, limit]).fetchdf()


# ---------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------
def render_county_profile_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("County Profile")
    st.caption("County-level PPP exposure, lender concentration, and risk indicators.")

    geoid = st.session_state.get("vf_county_focus")
    if not geoid:
        st.warning("No county selected. Use Mission Control or the map to select a county.")
        return

    header = _load_county_header(con, geoid)
    if header is None:
        st.error("County not found in reference table.")
        return

    st.subheader(f"{header['NAME']}, {header['STUSPS']}")

    # Risk metrics
    risk = _load_county_risk(con, geoid)
    if risk is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk Score", f"{risk['risk_score']:.1f}")
        c2.metric("Risk Tier", risk["risk_tier"])
        c3.metric("Percentile", f"{risk['risk_percentile_rank']:.0f}")
    else:
        st.info("No county risk score available.")

    st.markdown("---")

    # PPP summary
    ppp = _load_ppp_summary(con, geoid)
    if ppp is not None:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("PPP Loans", f"{int(ppp['loan_count']):,}")
        c2.metric("Total Approved ($)", f"${float(ppp['total_approved'] or 0):,.0f}")
        c3.metric("Avg Loan ($)", f"${float(ppp['avg_loan'] or 0):,.0f}")
        c4.metric("Jobs Reported", f"{int(ppp['jobs_reported'] or 0):,}")
    else:
        st.info("No PPP summary available for this county.")

    st.markdown("---")

    # Top lenders
    st.subheader("Top Lenders")
    lenders = _load_top_lenders(con, geoid, limit=15)
    if lenders.empty:
        st.info("No lender data available for this county.")
    else:
        st.dataframe(lenders, use_container_width=True)

        # Lender deep-dive hook
        lender_names = lenders["lender"].astype(str).tolist()
        lender_choice = st.selectbox("Open Lender Profile", [""] + lender_names)
        if lender_choice:
            st.session_state["vf_lender_focus"] = lender_choice
            st.experimental_rerun()

    st.markdown("---")

    # Borrower sample
    st.subheader("Borrower Sample")
    borrowers = _load_borrowers(con, geoid, limit=75)
    if borrowers.empty:
        st.info("No borrower rows available.")
    else:
        st.dataframe(
            borrowers.rename(
                columns={
                    "borrowername": "Borrower",
                    "borrowercity": "City",
                    "borrowerstate": "State",
                    "county_norm": "County",
                    "currentapprovalamount": "Approved ($)",
                    "naicscode": "NAICS",
                    "jobsreported": "Jobs",
                }
            ),
            use_container_width=True,
        )
