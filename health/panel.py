from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st


def render_health_panel(con: duckdb.DuckDBPyConnection) -> None:
    st.subheader("System Health")

    # -----------------------------
    # Helpers
    # -----------------------------
    def _table_exists(name: str) -> bool:
        try:
            q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
            return bool(con.execute(q, [name.lower()]).fetchone()[0])
        except Exception:
            return False

    def _columns(name: str) -> list[str]:
        try:
            df = con.execute(f"PRAGMA table_info('{name}')").fetchdf()
            return df["name"].astype(str).tolist()
        except Exception:
            return []

    def _count(name: str) -> int:
        try:
            return int(con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
        except Exception:
            return 0

    def _max_ts(table: str, candidate_cols: list[str]) -> str | None:
        if not _table_exists(table):
            return None
        cols = set(c.lower() for c in _columns(table))
        for c in candidate_cols:
            if c.lower() in cols:
                try:
                    v = con.execute(f"SELECT MAX({c}) FROM {table}").fetchone()[0]
                    return str(v) if v is not None else None
                except Exception:
                    continue
        return None

    def _safe_df(sql: str, params: list | None = None) -> pd.DataFrame:
        try:
            return con.execute(sql, params or []).fetchdf()
        except Exception:
            return pd.DataFrame()

    # -----------------------------
    # Tables overview (kept from your version)
    # -----------------------------
    tables = _safe_df("SHOW TABLES")
    st.caption("Tables")
    if not tables.empty:
        st.code(tables.to_string(index=False))
    else:
        st.warning("No tables returned from SHOW TABLES.")

    # -----------------------------
    # Core counts (launch-relevant)
    # -----------------------------
    core_tables = [
        ("ppp_clean", "PPP Clean"),
        ("county_ref", "County Ref"),
        ("acs_county", "ACS County"),
        ("county_scores", "County Scores"),
        ("lender_profiles", "Lender Profiles"),
        ("county_lender_signals", "County↔Lender Signals"),
    ]

    counts = {t: _count(t) for t, _ in core_tables if _table_exists(t)}
    ppp_rows = counts.get("ppp_clean", 0)
    county_ref_rows = counts.get("county_ref", 0)

    # -----------------------------
    # Canonical PPP→GEOID mapping health (replaces legacy ppp_agg_by_geoid)
    # -----------------------------
    mapped_geoids = 0
    mapped_ppp_rows = 0

    if _table_exists("ppp_clean") and _table_exists("county_ref"):
        # How many PPP rows map to a county_ref row?
        df_map = _safe_df(
            """
            SELECT
                COUNT(*) AS mapped_ppp_rows,
                COUNT(DISTINCT cr.GEOID) AS mapped_geoids
            FROM ppp_clean p
            JOIN county_ref cr
              ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
             AND p.borrowerstate = cr.STUSPS
            """
        )
        if not df_map.empty:
            mapped_ppp_rows = int(df_map.iloc[0]["mapped_ppp_rows"] or 0)
            mapped_geoids = int(df_map.iloc[0]["mapped_geoids"] or 0)

    map_pct = (mapped_ppp_rows / max(ppp_rows, 1)) * 100.0 if ppp_rows else 0.0

    # -----------------------------
    # Ingest timestamp (fixes Last ingest: None)
    # -----------------------------
    last_ingest = (
        _max_ts("ppp_ingest_log", ["finished_at", "completed_at", "ended_at", "end_ts", "run_ts", "ts", "timestamp", "started_at"])
        or _max_ts("ppp_ingest_log", ["created_at", "ingest_ts", "last_ingest_ts"])
        or None
    )

    # -----------------------------
    # KPI Row
    # -----------------------------
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("ppp_clean rows", f"{ppp_rows:,}")
    c2.metric("county_ref rows", f"{county_ref_rows:,}")
    c3.metric("PPP→GEOID mapped rows", f"{mapped_ppp_rows:,}")
    c4.metric("PPP mapping %", f"{map_pct:.1f}%")
    c5.metric("Last ingest", last_ingest or "Unknown")

    # -----------------------------
    # Spicier graphs
    # -----------------------------
    st.markdown("---")
    st.subheader("Health Visuals")

    # 1) Bar chart of key table sizes
    df_counts = pd.DataFrame(
        [
            {"table": label, "rows": _count(tbl) if _table_exists(tbl) else 0}
            for tbl, label in core_tables
        ]
    )

    left, right = st.columns(2)
    with left:
        st.caption("Key table row counts")
        st.bar_chart(df_counts.set_index("table")["rows"])

    # 2) Risk score distribution & tier counts (if present)
    with right:
        if _table_exists("county_scores"):
            df_scores = _safe_df(
                """
                SELECT risk_score, risk_tier
                FROM county_scores
                WHERE risk_score IS NOT NULL
                """
            )
            if not df_scores.empty:
                st.caption("Risk score distribution (county_scores)")
                # Streamlit will bin automatically in histogram-like feel via bar chart of value_counts on rounded scores
                df_scores["risk_score_bucket"] = pd.to_numeric(df_scores["risk_score"], errors="coerce").fillna(0).round(0)
                bucket = df_scores["risk_score_bucket"].value_counts().sort_index()
                st.bar_chart(bucket)

                st.caption("Risk tier breakdown")
                tiers = df_scores["risk_tier"].fillna("UNKNOWN").astype(str).value_counts()
                st.bar_chart(tiers)
            else:
                st.info("county_scores has no non-null risk_score values to chart.")
        else:
            st.info("county_scores not found; skipping risk charts.")

    # 3) Top counties by risk + PPP dollars (joined canonically)
    st.markdown("---")
    st.subheader("Top Counties (Risk + PPP)")

    if _table_exists("county_scores") and _table_exists("county_ref"):
        # PPP is OPTIONAL in V1 (Capella is source of truth). If ppp_clean exists locally,
        # we enrich; otherwise we render risk-only.
        if _table_exists("ppp_clean"):
            df_top = _safe_df(
                """
                WITH ppp_by_geoid AS (
                    SELECT
                        cr.GEOID,
                        SUM(TRY_CAST(p.currentapprovalamount AS DOUBLE)) AS total_approved,
                        COUNT(*) AS loan_count
                    FROM ppp_clean p
                    JOIN county_ref cr
                      ON UPPER(TRIM(p.county_norm)) = UPPER(TRIM(cr.NAME))
                     AND p.borrowerstate = cr.STUSPS
                    GROUP BY cr.GEOID
                )
                SELECT
                    cr.NAME,
                    cr.STUSPS,
                    cs.risk_score,
                    cs.risk_tier,
                    COALESCE(p.total_approved, 0) AS total_approved,
                    COALESCE(p.loan_count, 0) AS loan_count
                FROM county_scores cs
                JOIN county_ref cr ON cs.GEOID = cr.GEOID
                LEFT JOIN ppp_by_geoid p ON cs.GEOID = p.GEOID
                ORDER BY cs.risk_score DESC NULLS LAST, total_approved DESC NULLS LAST
                LIMIT 25
                """
            )
        else:
            df_top = _safe_df(
                """
                SELECT
                    cr.NAME,
                    cr.STUSPS,
                    cs.risk_score,
                    cs.risk_tier,
                    NULL AS total_approved,
                    NULL AS loan_count
                FROM county_scores cs
                JOIN county_ref cr ON cs.GEOID = cr.GEOID
                ORDER BY cs.risk_score DESC NULLS LAST
                LIMIT 25
                """
            )

        if not df_top.empty:
            st.dataframe(df_top, width='stretch')
        else:
            st.info("No rows returned for Top Counties.")
    else:
        st.info("Missing required analytics tables (county_scores, county_ref). Skipping top county panel.")

    # 4) Top lenders by PPP dollars (spice: quick whale list)
    st.markdown("---")
    st.subheader("Top Lenders (PPP)")

    if _table_exists("ppp_clean"):
        df_lenders = _safe_df(
            """
            SELECT
                servicinglendername AS lender,
                COUNT(*) AS loan_count,
                SUM(TRY_CAST(currentapprovalamount AS DOUBLE)) AS total_approved
            FROM ppp_clean
            WHERE servicinglendername IS NOT NULL
            GROUP BY servicinglendername
            ORDER BY total_approved DESC NULLS LAST
            LIMIT 25
            """
        )
        if not df_lenders.empty:
            st.dataframe(df_lenders, width='stretch')
        else:
            st.info("No lender totals returned.")
    else:
        st.info("ppp_clean not found; skipping lender totals.")

    # -----------------------------
    # Schema snapshots (kept, corrected, no legacy truncation)
    # -----------------------------
    st.markdown("---")
    st.subheader("Schema Snapshots")

    snapshot_tables = [
        "county_ref",
        "ppp_clean",
        "county_scores",
        "lender_profiles",
        "county_lender_signals",
        "acs_county",
        "ppp_ingest_log",
    ]

    for tbl in snapshot_tables:
        if not _table_exists(tbl):
            continue
        try:
            df = con.execute(f"PRAGMA table_info('{tbl}')").fetchdf()
            st.markdown(f"**{tbl} schema**")
            st.dataframe(df[["name", "type", "notnull"]], width='stretch', height=210)
        except Exception:
            pass
