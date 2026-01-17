from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

import duckdb
import pandas as pd
import streamlit as st


# =============================================================================
# Hybrid-safe helpers
# =============================================================================

def _table_exists(con: duckdb.DuckDBPyConnection, name: str, schema: str | None = None) -> bool:
    try:
        if schema:
            return bool(con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
                """,
                [schema, name],
            ).fetchone()[0])
        return bool(con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            """,
            [name],
        ).fetchone()[0])
    except Exception:
        return False


def _resolve_table(con: duckdb.DuckDBPyConnection, name: str) -> Optional[str]:
    """
    Resolve a table/view reference in hybrid deployments.

    Preference:
      1) main.<name> (compat view or monolithic)
      2) analytics.<name> (split db)
    """
    if _table_exists(con, name):
        return name
    if _table_exists(con, name, schema="analytics"):
        return f"analytics.{name}"
    return None


def _safe_secrets_get(key: str, default=None):
    """
    Streamlit st.secrets raises if no secrets.toml exists.
    This returns default safely.
    """
    try:
        return st.secrets.get(key, default)  # type: ignore[attr-defined]
    except Exception:
        return default


def _fmt_money(x) -> str:
    try:
        return f"${float(x):,.0f}"
    except Exception:
        return "—"


def _fmt_num(x) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "—"


def _fmt_float(x, nd=2) -> str:
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "—"


# =============================================================================
# Optional Capella hook (won't crash if missing)
# =============================================================================

def _capella_available() -> bool:
    # If you later add utils/capella.py with a get_cluster() or similar, you can upgrade this.
    enabled = _safe_secrets_get("CAPELLA_ENABLED", False)
    return bool(enabled)


# =============================================================================
# Core page
# =============================================================================

def render_lender_profile_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Lender Profile")
    st.caption("Hybrid mode: DuckDB analytics for scoring + Capella for PPP drilldowns (on-demand).")

    lp = _resolve_table(con, "lender_profiles")
    cls = _resolve_table(con, "county_lender_signals")

    if not lp and not cls:
        st.error(
            "Lender analytics tables are not available in this DuckDB. "
            "Expected `lender_profiles` and/or `county_lender_signals` in main or analytics schema."
        )
        st.info("This does not affect county scoring; it only impacts lender pages.")
        return

    # -------------------------------------------------------------------------
    # Lender selection model
    # -------------------------------------------------------------------------
    lender_name = _lender_selector(con, lp, cls)
    if not lender_name:
        st.info("Select a lender to view profile analytics.")
        return

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Summary metrics (prefer lender_profiles, fall back to aggregates from signals)
    # -------------------------------------------------------------------------
    left, right = st.columns([0.55, 0.45])

    with left:
        st.subheader("Summary")
        summary = _load_lender_summary(con, lp, cls, lender_name)
        if summary is None or summary.empty:
            st.warning("No lender summary available.")
        else:
            _render_summary_cards(summary)

    with right:
        st.subheader("Hybrid Status")
        st.write(
            {
                "DuckDB lender_profiles": bool(lp),
                "DuckDB county_lender_signals": bool(cls),
                "Capella available": _capella_available(),
                "PPP local table required": False,
            }
        )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Signals detail: top counties, distribution, anomalies
    # -------------------------------------------------------------------------
    st.subheader("County Footprint & Signals")

    if cls:
        tab1, tab2, tab3 = st.tabs(["Top Counties", "Signal Distribution", "Anomalies"])
        with tab1:
            df_top = _load_top_counties_for_lender(con, cls, lender_name, limit=50)
            if df_top.empty:
                st.info("No county footprint rows found for this lender.")
            else:
                st.dataframe(df_top, use_container_width=True)

        with tab2:
            df_dist = _load_signal_distribution(con, cls, lender_name)
            if df_dist.empty:
                st.info("No signal distribution available.")
            else:
                st.dataframe(df_dist, use_container_width=True)

        with tab3:
            df_anom = _load_lender_anomalies(con, cls, lender_name, limit=200)
            if df_anom.empty:
                st.info("No anomaly flags present for this lender.")
            else:
                st.dataframe(df_anom, use_container_width=True)
    else:
        st.info("`county_lender_signals` table not available — footprint analysis is unavailable in this environment.")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Optional PPP drilldown (Capella) — placeholder hook
    # -------------------------------------------------------------------------
    st.subheader("PPP Drilldown (Capella)")
    st.caption("This panel is Capella-backed and intentionally on-demand. It will not run without Capella wiring.")

    if not _capella_available():
        st.info("Capella not enabled (no secrets/config). PPP drilldown is disabled locally.")
        return

    st.warning(
        "Capella is enabled, but this project requires your `utils/capella.py` accessor to be wired into this page. "
        "Once wired, this section can fetch PPP documents for this lender without loading PPP locally."
    )


# =============================================================================
# Components
# =============================================================================

def _lender_selector(con: duckdb.DuckDBPyConnection, lp: Optional[str], cls: Optional[str]) -> Optional[str]:
    st.sidebar.subheader("Lender Selection")

    # Pull a lender list from lender_profiles if available, otherwise from signals.
    lenders: List[str] = []
    try:
        if lp:
            lenders = con.execute(
                f"""
                SELECT DISTINCT LenderName
                FROM {lp}
                WHERE LenderName IS NOT NULL AND LenderName <> ''
                ORDER BY LenderName
                """
            ).fetchnumpy()["LenderName"].tolist()
        elif cls:
            lenders = con.execute(
                f"""
                SELECT DISTINCT LenderName
                FROM {cls}
                WHERE LenderName IS NOT NULL AND LenderName <> ''
                ORDER BY LenderName
                """
            ).fetchnumpy()["LenderName"].tolist()
    except Exception:
        lenders = []

    lenders = [str(x) for x in lenders if str(x).strip()]

    if not lenders:
        # Allow manual entry as fallback
        manual = st.sidebar.text_input("Lender name (manual)", value="")
        return manual.strip() or None

    # Searchable selector
    default_ix = 0
    pick = st.sidebar.selectbox("Lender", lenders, index=default_ix)
    return str(pick).strip() if pick else None


def _load_lender_summary(con: duckdb.DuckDBPyConnection, lp: Optional[str], cls: Optional[str], lender: str) -> pd.DataFrame:
    """
    Build a robust lender summary.
    Priority:
      - lender_profiles (if present)
      - aggregates from county_lender_signals
    """
    try:
        if lp:
            # Try to read common lender_profiles schema. If columns are missing, we degrade.
            # We select only what exists by probing information_schema.columns would be more elaborate;
            # here we defensively COALESCE to avoid hard failures.
            q = f"""
            SELECT
                LenderName,
                COALESCE(total_loan_count, loan_count, 0) AS loan_count,
                COALESCE(total_approval_amount, approval_amount, 0) AS approval_amount,
                COALESCE(avg_risk_score, 0) AS avg_risk_score,
                COALESCE(risk_weighted_score, 0) AS risk_weighted_score,
                COALESCE(county_coverage, 0) AS county_coverage
            FROM {lp}
            WHERE LenderName = ?
            LIMIT 1
            """
            df = con.execute(q, [lender]).fetchdf()
            if df is not None and not df.empty:
                return df

        if cls:
            q = f"""
            SELECT
                ? AS LenderName,
                SUM(COALESCE(loan_count, 0))::BIGINT AS loan_count,
                AVG(COALESCE(dominance_score, 0)) AS avg_dominance,
                AVG(COALESCE(concentration_z, 0)) AS avg_concentration_z,
                COUNT(DISTINCT GEOID)::BIGINT AS county_coverage,
                SUM(CASE WHEN COALESCE(anomaly_flag, FALSE) THEN 1 ELSE 0 END)::BIGINT AS anomaly_count
            FROM {cls}
            WHERE LenderName = ?
            """
            return con.execute(q, [lender, lender]).fetchdf()

    except Exception:
        return pd.DataFrame()

    return pd.DataFrame()


def _render_summary_cards(summary: pd.DataFrame) -> None:
    row = summary.iloc[0].to_dict()

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Lender", str(row.get("LenderName", "—")))
    if "loan_count" in row:
        c2.metric("Loan Count", _fmt_num(row.get("loan_count", 0)))
    else:
        c2.metric("Loan Count", "—")

    if "approval_amount" in row:
        c3.metric("Approval Amount", _fmt_money(row.get("approval_amount", 0)))
    else:
        c3.metric("Coverage Counties", _fmt_num(row.get("county_coverage", 0)))

    # Best-effort risk metrics
    if "avg_risk_score" in row:
        c4.metric("Avg Risk", _fmt_float(row.get("avg_risk_score", 0), 2))
        c5.metric("Risk Weighted", _fmt_float(row.get("risk_weighted_score", 0), 2))
    else:
        c4.metric("Avg Dominance", _fmt_float(row.get("avg_dominance", 0), 3))
        c5.metric("Avg Concentration Z", _fmt_float(row.get("avg_concentration_z", 0), 3))


def _load_top_counties_for_lender(con: duckdb.DuckDBPyConnection, cls: str, lender: str, limit: int = 50) -> pd.DataFrame:
    # Join county_ref if available for names
    has_ref = _table_exists(con, "county_ref") or _table_exists(con, "county_ref", schema="core")
    cr = "county_ref" if _table_exists(con, "county_ref") else ("core.county_ref" if _table_exists(con, "county_ref", "core") else None)

    q = f"""
    SELECT
        s.GEOID,
        {("cr.NAME AS CountyName, cr.STUSPS AS STUSPS," if cr else "")}
        SUM(COALESCE(s.loan_count, 0))::BIGINT AS loan_count,
        AVG(COALESCE(s.dominance_score, 0)) AS dominance_score,
        AVG(COALESCE(s.concentration_z, 0)) AS concentration_z,
        MAX(CASE WHEN COALESCE(s.anomaly_flag, FALSE) THEN 1 ELSE 0 END)::INT AS anomaly_flag
    FROM {cls} s
    {("LEFT JOIN " + cr + " cr ON s.GEOID = cr.GEOID" if cr else "")}
    WHERE s.LenderName = ?
    GROUP BY s.GEOID {(", cr.NAME, cr.STUSPS" if cr else "")}
    ORDER BY loan_count DESC
    LIMIT {int(limit)}
    """
    try:
        return con.execute(q, [lender]).fetchdf()
    except Exception:
        return pd.DataFrame()


def _load_signal_distribution(con: duckdb.DuckDBPyConnection, cls: str, lender: str) -> pd.DataFrame:
    q = f"""
    SELECT
        CASE
          WHEN COALESCE(dominance_score, 0) >= 0.80 THEN '0.80+'
          WHEN COALESCE(dominance_score, 0) >= 0.60 THEN '0.60-0.79'
          WHEN COALESCE(dominance_score, 0) >= 0.40 THEN '0.40-0.59'
          WHEN COALESCE(dominance_score, 0) >= 0.20 THEN '0.20-0.39'
          ELSE '<0.20'
        END AS dominance_bucket,
        COUNT(*)::BIGINT AS rows,
        SUM(COALESCE(loan_count, 0))::BIGINT AS loans
    FROM {cls}
    WHERE LenderName = ?
    GROUP BY 1
    ORDER BY 1
    """
    try:
        return con.execute(q, [lender]).fetchdf()
    except Exception:
        return pd.DataFrame()


def _load_lender_anomalies(con: duckdb.DuckDBPyConnection, cls: str, lender: str, limit: int = 200) -> pd.DataFrame:
    # Join county_ref if available
    cr = "county_ref" if _table_exists(con, "county_ref") else ("core.county_ref" if _table_exists(con, "county_ref", "core") else None)

    q = f"""
    SELECT
        s.GEOID,
        {("cr.NAME AS CountyName, cr.STUSPS AS STUSPS," if cr else "")}
        COALESCE(s.loan_count, 0) AS loan_count,
        COALESCE(s.dominance_score, 0) AS dominance_score,
        COALESCE(s.concentration_z, 0) AS concentration_z,
        COALESCE(s.anomaly_flag, FALSE) AS anomaly_flag
    FROM {cls} s
    {("LEFT JOIN " + cr + " cr ON s.GEOID = cr.GEOID" if cr else "")}
    WHERE s.LenderName = ?
      AND COALESCE(s.anomaly_flag, FALSE) = TRUE
    ORDER BY COALESCE(s.concentration_z, 0) DESC
    LIMIT {int(limit)}
    """
    try:
        return con.execute(q, [lender]).fetchdf()
    except Exception:
        return pd.DataFrame()
