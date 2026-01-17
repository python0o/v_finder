from __future__ import annotations

import duckdb
import streamlit as st


def _table_exists(con: duckdb.DuckDBPyConnection, name: str, schema: str | None = None) -> bool:
    try:
        if schema:
            return bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE lower(table_schema) = lower(?)
                      AND lower(table_name) = lower(?)
                    """,
                    [schema, name],
                ).fetchone()[0]
            )
        return bool(
            con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE lower(table_name) = lower(?)
                """,
                [name],
            ).fetchone()[0]
        )
    except Exception:
        return False


def _resolve_county_scores(con: duckdb.DuckDBPyConnection) -> str | None:
    """
    Resolve the correct reference for county_scores.

    Preference order:
    1) main.county_scores (compat view)
    2) analytics.county_scores (split DB)
    """
    if _table_exists(con, "county_scores"):
        return "county_scores"
    if _table_exists(con, "county_scores", schema="analytics"):
        return "analytics.county_scores"
    return None


def render_dashboard_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Dashboard")
    st.caption("Hybrid launch overview (DuckDB analytics + Capella PPP drilldowns).")

    cs = _resolve_county_scores(con)
    if not cs:
        st.warning("County scoring table not available.")
        return

    try:
        df = con.execute(
            f"""
            SELECT
              COUNT(*)              AS counties,
              AVG(risk_score)       AS avg_risk,
              MAX(risk_score)       AS max_risk
            FROM {cs}
            """
        ).fetchdf()

        st.subheader("System Summary")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Dashboard summary unavailable: {e}")
