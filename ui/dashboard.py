from __future__ import annotations

import duckdb
import streamlit as st


def render_dashboard_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Dashboard")
    st.caption("Hybrid launch overview (DuckDB analytics + Capella PPP drilldowns).")

    # Keep this lightweight; dashboards should not pull PPP locally.
    try:
        df = con.execute(
            """
            SELECT
              COUNT(*) AS counties,
              AVG(risk_score) AS avg_risk,
              MAX(risk_score) AS max_risk
            FROM county_scores
            """
        ).fetchdf()
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"Dashboard summary unavailable: {e}")
