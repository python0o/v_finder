"""
ui/lender_profile.py — v9 FINAL

Lender Profile:
- County footprint with dominance safeguards
- Borrower evidence (copy/paste fraud)
- No dependency on ui.outliers (county-only logic)
- Network-aligned scoring
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st


# =====================================================================
# Utilities
# =====================================================================

def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        return bool(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [name.lower()],
            ).fetchone()[0]
        )
    except Exception:
        return False


def _safe_df(con, sql, params=None):
    try:
        return con.execute(sql, params or []).fetchdf()
    except Exception:
        return pd.DataFrame()


# =====================================================================
# Loaders
# =====================================================================

def _load_lenders(con):
    if not _table_exists(con, "ppp_clean"):
        return []
    df = _safe_df(
        con,
        """
        SELECT DISTINCT servicinglendername
        FROM ppp_clean
        WHERE servicinglendername IS NOT NULL
        ORDER BY servicinglendername
        """
    )
    return df.iloc[:, 0].astype(str).tolist() if not df.empty else []


def _load_lender_summary(con, lender):
    return _safe_df(
        con,
        """
        SELECT
            COUNT(*) AS loan_count,
            SUM(TRY_CAST(currentapprovalamount AS DOUBLE)) AS total_approved,
            AVG(TRY_CAST(currentapprovalamount AS DOUBLE)) AS avg_loan
        FROM ppp_clean
        WHERE servicinglendername = ?
        """,
        [lender],
    )


def _load_lender_counties(con, lender):
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return pd.DataFrame()

    df = _safe_df(
        con,
        """
        SELECT
            cr.GEOID,
            cr.NAME AS county,
            cr.STUSPS AS state,
            COUNT(*) AS loan_count,
            SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved
        FROM ppp_clean p
        JOIN county_ref cr
          ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
         AND p.borrowerstate = cr.STUSPS
        WHERE p.servicinglendername = ?
        GROUP BY cr.GEOID, cr.NAME, cr.STUSPS
        ORDER BY total_approved DESC NULLS LAST
        """,
        [lender],
    )

    if df.empty:
        return df

    # -----------------------------------------------------------------
    # Dominance / visibility safeguard (same logic as network graph)
    # -----------------------------------------------------------------
    max_amt = df["total_approved"].max() or 1.0
    max_loans = df["loan_count"].max() or 1.0

    df["footprint_strength"] = (
        0.7 * (df["total_approved"] / max_amt)
        + 0.3 * (df["loan_count"] / max_loans)
    ).fillna(0.01)

    df["footprint_strength"] = df["footprint_strength"].clip(lower=0.03)

    return df


def _load_borrowers(con, lender, limit=500):
    return _safe_df(
        con,
        f"""
        SELECT
            borrowername AS business_name,
            borroweraddress AS address,
            borrowercity AS city,
            borrowerstate AS state,
            borrowerzip AS zip,
            businesstype AS business_type,
            naicscode AS naics,
            TRY_CAST(currentapprovalamount AS DOUBLE) AS approved_amount,
            TRY_CAST(jobsreported AS DOUBLE) AS jobs_reported,
            projectcountyname AS county
        FROM ppp_clean
        WHERE servicinglendername = ?
        ORDER BY approved_amount DESC NULLS LAST
        LIMIT {int(limit)}
        """,
        [lender],
    )


# =====================================================================
# Page
# =====================================================================

def render_lender_profile_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Lender Profile")

    lenders = _load_lenders(con)
    if not lenders:
        st.error("No lenders available.")
        return

    lender = st.selectbox("Select Lender", lenders)
    st.session_state["vf_lender_focus"] = lender

    summary = _load_lender_summary(con, lender)
    counties = _load_lender_counties(con, lender)

    # -----------------------------------------------------------------
    # Summary KPIs
    # -----------------------------------------------------------------
    if not summary.empty:
        r = summary.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("PPP Loans", f"{int(r['loan_count']):,}")
        c2.metric("Total Approved", f"${float(r['total_approved']):,.0f}")
        c3.metric("Avg Loan", f"${float(r['avg_loan']):,.0f}")

    st.markdown("---")

    # -----------------------------------------------------------------
    # County Footprint
    # -----------------------------------------------------------------
    st.subheader("County Footprint")

    if counties.empty:
        st.info("No county-level activity found.")
    else:
        top_share = counties.iloc[0]["total_approved"] / counties["total_approved"].sum()

        if top_share > 0.5:
            st.warning(
                "⚠️ High geographic concentration detected. "
                "More than 50% of PPP volume is concentrated in a single county."
            )
        elif top_share < 0.15:
            st.info(
                "This lender shows a broad geographic spread, typical of "
                "regional or national lenders."
            )

        st.dataframe(
            counties[[
                "county",
                "state",
                "loan_count",
                "total_approved",
                "footprint_strength",
            ]],
            use_container_width=True,
        )

    st.markdown("---")

    # -----------------------------------------------------------------
    # Borrower Evidence (Copy/Paste Fraud Detection)
    # -----------------------------------------------------------------
    st.subheader("Borrower Evidence")

    borrowers = _load_borrowers(con, lender)
    if borrowers.empty:
        st.info("No borrower-level data available.")
    else:
        f1, f2 = st.columns(2)
        with f1:
            name_q = st.text_input("Business name contains")
        with f2:
            addr_q = st.text_input("Address contains")

        view = borrowers.copy()
        if name_q:
            view = view[view["business_name"].str.contains(name_q, case=False, na=False)]
        if addr_q:
            view = view[view["address"].str.contains(addr_q, case=False, na=False)]

        st.dataframe(view, use_container_width=True)

        st.download_button(
            "Download Borrower Evidence",
            data=view.to_csv(index=False),
            file_name=f"{lender}_borrower_evidence.csv",
            mime="text/csv",
        )
