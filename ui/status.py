# =============================================================================
# ui/status.py – Pipeline Health Panel
# Schema-agnostic table detection (supports v_finder.main confusion)
# =============================================================================

import streamlit as st
import duckdb


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = f"""
        SELECT COUNT(*) 
        FROM duckdb_tables()
        WHERE lower(table_name) = lower('{name}')
    """
    return con.execute(q).fetchone()[0] > 0


def _has_centroids(con):
    q = """
        SELECT COUNT(*)
        FROM duckdb_columns()
        WHERE lower(table_name) = 'county_ref'
          AND lower(column_name) = 'intptlat'
    """
    return con.execute(q).fetchone()[0] > 0


def _has_ppp(con):
    return _table_exists(con, "ppp_clean")


def _has_ppp_agg(con):
    return _table_exists(con, "county_agg")


def _has_acs(con):
    return _table_exists(con, "acs_county")


def _has_scores(con):
    return _table_exists(con, "county_scores")


# ---------------------------------------------------------------------
# Main renderer
# ---------------------------------------------------------------------

def render_pipeline_status(con):
    st.subheader("Pipeline Health")

    rows = [
        ("County Reference Loaded", _table_exists(con, "county_ref")),
        ("Centroids Present", _has_centroids(con)),
        ("PPP Raw → ppp_clean", _has_ppp(con)),
        ("PPP Aggregation → county_agg", _has_ppp_agg(con)),
        ("ACS Enriched → acs_county", _has_acs(con)),
        ("Risk Scoring → county_scores", _has_scores(con)),
    ]

    for label, ok in rows:
        if ok:
            st.success(label)
        else:
            st.warning(label)
