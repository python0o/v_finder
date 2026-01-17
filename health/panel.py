from __future__ import annotations

import duckdb
import streamlit as st


def render_health_panel(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Health Panel")
    st.caption("Launch diagnostics for DuckDB analytics + optional local PPP artifacts.")

    def _table_exists(name: str) -> bool:
        try:
            return bool(
                con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower(?)",
                    [name],
                ).fetchone()[0]
            )
        except Exception:
            return False

    # -----------------------------------------------------------------
    # Table counts
    # -----------------------------------------------------------------
    st.subheader("Table Presence + Row Counts")

    tables = [
        ("county_ref", "County Reference"),
        ("acs_county", "ACS County"),
        ("county_agg", "County Aggregates"),
        ("county_scores", "County Scores"),
        ("lender_profiles", "Lender Profiles"),
        ("county_lender_signals", "County Lender Signals"),
        ("ppp_clean", "PPP Clean (optional)"),
    ]

    counts = {}
    for t, _label in tables:
        if _table_exists(t):
            try:
                counts[t] = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            except Exception:
                counts[t] = 0
        else:
            counts[t] = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("county_ref rows", f"{counts.get('county_ref', 0):,}")
    c2.metric("acs_county rows", f"{counts.get('acs_county', 0):,}")
    c3.metric("county_scores rows", f"{counts.get('county_scores', 0):,}")
    c4.metric("ppp_clean rows (optional)", f"{counts.get('ppp_clean', 0):,}")

    st.markdown("---")

    # -----------------------------------------------------------------
    # Core validation
    # -----------------------------------------------------------------
    st.subheader("Core Integrity Checks")

    missing_core = [t for t in ["county_scores", "county_ref"] if not _table_exists(t)]
    if missing_core:
        st.error(f"Missing core analytics tables: {', '.join(missing_core)}")
        return

    st.success("Core analytics tables present.")

    # -----------------------------------------------------------------
    # Top counties (risk-only, always valid)
    # -----------------------------------------------------------------
    st.subheader("Top Counties by Risk (Risk-only baseline)")

    try:
        df = con.execute(
            """
            SELECT
                cs.GEOID,
                cr.NAME,
                cr.STUSPS,
                cs.risk_score,
                cs.hidden_signal_score
            FROM county_scores cs
            LEFT JOIN county_ref cr ON cs.GEOID = cr.GEOID
            ORDER BY cs.risk_score DESC
            LIMIT 25
            """
        ).fetchdf()
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"Risk leaderboard query failed: {e}")

    st.markdown("---")

    # -----------------------------------------------------------------
    # Optional PPP-local sections (DO NOT BLOCK HEALTH PANEL)
    # -----------------------------------------------------------------
    if not _table_exists("ppp_clean"):
        st.info("PPP local table `ppp_clean` not present (expected in Hybrid/Capella mode). Skipping PPP-local diagnostics.")
        return

    st.subheader("PPP Local Diagnostics (Optional)")

    try:
        # Basic state distribution
        if _table_exists("ppp_clean"):
            df_states = con.execute(
                """
                SELECT BorrowerState AS ST, COUNT(*) AS n
                FROM ppp_clean
                WHERE BorrowerState IS NOT NULL AND BorrowerState <> ''
                GROUP BY 1
                ORDER BY n DESC
                LIMIT 20
                """
            ).fetchdf()
            st.caption("Top PPP borrower states (local ppp_clean only)")
            st.dataframe(df_states, use_container_width=True)
    except Exception as e:
        st.warning(f"PPP-local diagnostics failed: {e}")

    # Optional join sanity (only if county_ref exists, already validated)
    try:
        st.subheader("Known County Join Rate (PPP → county_ref)")
        df_join = con.execute(
            """
            SELECT
              COUNT(*) AS ppp_rows,
              SUM(CASE WHEN cr.GEOID IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows
            FROM ppp_clean p
            LEFT JOIN county_ref cr
              ON p.BorrowerCounty = cr.NAME AND p.BorrowerState = cr.STUSPS
            """
        ).fetchdf()
        st.dataframe(df_join, use_container_width=True)
    except Exception as e:
        st.warning(f"PPP join-rate check failed: {e}")
