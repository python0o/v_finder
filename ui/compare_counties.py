"""
ui/compare_counties.py — v4 ULTRA (LAUNCH-LOCKED)

2–5 County comparison lab with shared metrics:
- Risk tier + score
- PPP loan count, total approved, avg loan, jobs reported
- Top-lender concentration proxy (top-1 / top-3 share)
- Simple hidden-signal heuristics (PPP intensity + risk-weight)

Launch guarantees:
- No PPP → GEOID assumptions
- PPP join is canonical: (county_norm, borrowerstate) -> county_ref -> GEOID
- Lowercase PPP column names only
- Fail-closed behavior (never hard-crashes)
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


def _safe_num(s: pd.Series | int | float, default: float = 0.0) -> float:
    try:
        if isinstance(s, pd.Series):
            v = pd.to_numeric(s, errors="coerce").fillna(default)
            return float(v.iloc[0]) if len(v) else float(default)
        return float(s)
    except Exception:
        return float(default)


# ---------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------
def _load_county_universe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """County list for selection."""
    if not _table_exists(con, "county_ref"):
        return pd.DataFrame()

    sql = """
    SELECT
        GEOID,
        NAME,
        STUSPS
    FROM county_ref
    ORDER BY STUSPS, NAME
    """
    return con.execute(sql).fetchdf()


def _load_compare_rows(con: duckdb.DuckDBPyConnection, geoids: list[str]) -> pd.DataFrame:
    """Primary compare table: risk + PPP summary per county."""
    if not geoids:
        return pd.DataFrame()

    if not (_table_exists(con, "county_ref") and _table_exists(con, "county_scores")):
        return pd.DataFrame()

    # PPP is optional for compare output, but if present we compute it.
    has_ppp = _table_exists(con, "ppp_clean")

    if has_ppp:
        sql = f"""
        WITH target AS (
            SELECT GEOID, NAME, STUSPS
            FROM county_ref
            WHERE GEOID IN ({",".join(["?"] * len(geoids))})
        ),
        ppp_sum AS (
            SELECT
                cr.GEOID,
                COUNT(*) AS loan_count,
                SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved,
                AVG(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS avg_loan,
                SUM(TRY_CAST(p.jobsreported AS DOUBLE)) AS jobs_reported
            FROM ppp_clean p
            JOIN county_ref cr
              ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
             AND p.borrowerstate = cr.STUSPS
            JOIN target t
              ON cr.GEOID = t.GEOID
            GROUP BY cr.GEOID
        )
        SELECT
            t.GEOID,
            t.NAME,
            t.STUSPS,
            cs.risk_score,
            cs.risk_tier,
            cs.risk_percentile_rank,
            COALESCE(ps.loan_count, 0) AS loan_count,
            COALESCE(ps.total_approved, 0) AS total_approved,
            COALESCE(ps.avg_loan, 0) AS avg_loan,
            COALESCE(ps.jobs_reported, 0) AS jobs_reported
        FROM target t
        LEFT JOIN county_scores cs ON t.GEOID = cs.GEOID
        LEFT JOIN ppp_sum ps ON t.GEOID = ps.GEOID
        ORDER BY cs.risk_score DESC NULLS LAST, ps.total_approved DESC NULLS LAST
        """
        return con.execute(sql, geoids).fetchdf()

    # No PPP: return risk-only compare rows
    sql = f"""
    SELECT
        cr.GEOID,
        cr.NAME,
        cr.STUSPS,
        cs.risk_score,
        cs.risk_tier,
        cs.risk_percentile_rank,
        0::BIGINT AS loan_count,
        0::DOUBLE AS total_approved,
        0::DOUBLE AS avg_loan,
        0::DOUBLE AS jobs_reported
    FROM county_ref cr
    LEFT JOIN county_scores cs ON cr.GEOID = cs.GEOID
    WHERE cr.GEOID IN ({",".join(["?"] * len(geoids))})
    ORDER BY cs.risk_score DESC NULLS LAST
    """
    return con.execute(sql, geoids).fetchdf()


def _load_lender_concentration(con: duckdb.DuckDBPyConnection, geoids: list[str]) -> pd.DataFrame:
    """
    Concentration proxy using PPP lender totals within each county:
      - top1_share
      - top3_share

    Returns one row per GEOID.
    """
    if not geoids:
        return pd.DataFrame()
    if not (_table_exists(con, "ppp_clean") and _table_exists(con, "county_ref")):
        return pd.DataFrame()

    sql = f"""
    WITH target AS (
        SELECT GEOID, NAME, STUSPS
        FROM county_ref
        WHERE GEOID IN ({",".join(["?"] * len(geoids))})
    ),
    lender_totals AS (
        SELECT
            cr.GEOID,
            p.servicinglendername AS lender,
            SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved
        FROM ppp_clean p
        JOIN county_ref cr
          ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
         AND p.borrowerstate = cr.STUSPS
        JOIN target t
          ON cr.GEOID = t.GEOID
        WHERE p.servicinglendername IS NOT NULL
        GROUP BY cr.GEOID, p.servicinglendername
    ),
    ranked AS (
        SELECT
            GEOID,
            lender,
            total_approved,
            ROW_NUMBER() OVER (PARTITION BY GEOID ORDER BY total_approved DESC NULLS LAST) AS rn,
            SUM(total_approved) OVER (PARTITION BY GEOID) AS county_total
        FROM lender_totals
    )
    SELECT
        GEOID,
        MAX(CASE WHEN rn = 1 THEN total_approved ELSE 0 END) / NULLIF(MAX(county_total), 0) AS top1_share,
        SUM(CASE WHEN rn <= 3 THEN total_approved ELSE 0 END) / NULLIF(MAX(county_total), 0) AS top3_share
    FROM ranked
    GROUP BY GEOID
    """
    return con.execute(sql, geoids).fetchdf()


def _compute_hidden_signal(row: pd.Series) -> float:
    """
    A simple, launch-safe hidden-signal heuristic (0..100):
    - weighted by risk_score
    - penalizes high concentration
    - rewards PPP intensity (log-scaled)
    """
    risk = float(row.get("risk_score") or 0.0)
    total = float(row.get("total_approved") or 0.0)
    loans = float(row.get("loan_count") or 0.0)
    top3 = float(row.get("top3_share") or 0.0)

    # PPP intensity: log-ish
    intensity = 0.0
    if total > 0:
        # conservative scaling to avoid huge jumps
        intensity = min(35.0, (total ** 0.25) * 2.0)

    # loan volume factor
    vol = min(20.0, (loans ** 0.5) * 1.5) if loans > 0 else 0.0

    # concentration penalty
    conc_penalty = min(30.0, top3 * 40.0)

    score = (risk * 0.7) + intensity + vol - conc_penalty
    return float(max(0.0, min(100.0, score)))


# ---------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------
def render_compare_counties_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Compare Counties")
    st.caption("Side-by-side comparison for 2–5 counties across risk, PPP metrics, and concentration signals.")

    df_universe = _load_county_universe(con)
    if df_universe.empty:
        st.error("county_ref is missing or empty. Cannot build comparison universe.")
        return

    # Build selection labels
    df_universe["label"] = df_universe["NAME"].astype(str) + ", " + df_universe["STUSPS"].astype(str)
    label_to_geoid = dict(zip(df_universe["label"], df_universe["GEOID"].astype(str)))

    # Default preselect if session state exists (optional)
    focus_geoid = st.session_state.get("vf_county_focus")
    default_labels = []
    if focus_geoid:
        match = df_universe[df_universe["GEOID"].astype(str) == str(focus_geoid)]
        if not match.empty:
            default_labels = [match.iloc[0]["label"]]

    selected_labels = st.multiselect(
        "Select 2–5 counties",
        options=df_universe["label"].tolist(),
        default=default_labels,
        max_selections=5,
    )

    if len(selected_labels) < 2:
        st.info("Select at least 2 counties to run comparison.")
        return

    geoids = [label_to_geoid[lbl] for lbl in selected_labels]

    df_core = _load_compare_rows(con, geoids)
    if df_core.empty:
        st.warning("No comparison data returned. Check county_scores and PPP coverage.")
        return

    df_conc = _load_lender_concentration(con, geoids)
    if not df_conc.empty:
        df = df_core.merge(df_conc, on="GEOID", how="left")
    else:
        df = df_core.copy()
        df["top1_share"] = 0.0
        df["top3_share"] = 0.0

    # Hidden signal score
    df["hidden_signal_score"] = df.apply(_compute_hidden_signal, axis=1)

    # Friendly formatting
    df_display = df.copy()
    df_display["County"] = df_display["NAME"].astype(str) + ", " + df_display["STUSPS"].astype(str)

    # Ensure numeric types
    for col in ["risk_score", "risk_percentile_rank", "loan_count", "total_approved", "avg_loan", "jobs_reported", "top1_share", "top3_share", "hidden_signal_score"]:
        if col in df_display.columns:
            df_display[col] = pd.to_numeric(df_display[col], errors="coerce").fillna(0)

    # Order columns
    cols = [
        "County",
        "risk_score",
        "risk_tier",
        "risk_percentile_rank",
        "loan_count",
        "total_approved",
        "avg_loan",
        "jobs_reported",
        "top1_share",
        "top3_share",
        "hidden_signal_score",
        "GEOID",
    ]
    cols = [c for c in cols if c in df_display.columns]
    df_display = df_display[cols].sort_values(["hidden_signal_score", "risk_score", "total_approved"], ascending=[False, False, False])

    st.subheader("Comparison Table")
    st.dataframe(df_display, use_container_width=True)

    st.markdown("---")

    # Quick drill-down buttons
    st.subheader("Drill Down")
    drill = st.selectbox("Open County Profile", [""] + df_display["County"].astype(str).tolist())
    if drill:
        geoid = df_display[df_display["County"] == drill].iloc[0]["GEOID"]
        st.session_state["vf_county_focus"] = str(geoid)
        st.experimental_rerun()
