# db/core.py
# =============================================================================
# V_FINDER – Base Schema Manager
#
# This module provides a single public entrypoint:
#
#     ensure_base_schema(con)
#
# It guarantees that required tables exist:
#     - ppp_clean
#     - county_agg
#     - county_ref
#     - county_scores
#     - acs_county
#     - ppp_ingest_log
#
# and does *nothing* destructive—only creates tables if missing.
# =============================================================================

from __future__ import annotations
import duckdb


# -------------------------------------------------------------------------
# Utility
# -------------------------------------------------------------------------
def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    sql = """
        SELECT 1
        FROM duckdb_tables
        WHERE table_name = ?
        LIMIT 1
    """
    row = con.execute(sql, [name]).fetchone()
    return row is not None


# -------------------------------------------------------------------------
# Main API
# -------------------------------------------------------------------------
def ensure_base_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure all core V_FINDER tables exist.
    This function is SAFE to call repeatedly.
    """

    # -------------------------------------------------
    # ppp_clean (skeleton – real ingest fills it later)
    # -------------------------------------------------
    if not _table_exists(con, "ppp_clean"):
        con.execute(
            """
            CREATE TABLE ppp_clean AS
            SELECT 
                ''::VARCHAR AS loannumber
            WHERE 0=1
            """
        )

    # -------------------------------------------------
    # ppp_ingest_log
    # -------------------------------------------------
    if not _table_exists(con, "ppp_ingest_log"):
        con.execute(
            """
            CREATE TABLE ppp_ingest_log (
                filename    VARCHAR PRIMARY KEY,
                ingested_at TIMESTAMP
            )
            """
        )

    # -------------------------------------------------
    # county_ref
    # (Populated by the standalone repair scripts)
    # -------------------------------------------------
    if not _table_exists(con, "county_ref"):
        con.execute(
            """
            CREATE TABLE county_ref AS
            SELECT 
                ''::VARCHAR AS GEOID,
                ''::VARCHAR AS STATEFP,
                ''::VARCHAR AS COUNTYFP,
                ''::VARCHAR AS STUSPS,
                ''::VARCHAR AS NAME,
                ''::VARCHAR AS NAME_NORM,
                NULL::DOUBLE AS INTPTLAT,
                NULL::DOUBLE AS INTPTLONG,
                NULL::DOUBLE AS ALAND
            WHERE 0=1
            """
        )

    # -------------------------------------------------
    # county_agg
    # (Rebuilt by PPP ingest)
    # -------------------------------------------------
    if not _table_exists(con, "county_agg"):
        con.execute(
            """
            CREATE TABLE county_agg AS
            SELECT 
                ''::VARCHAR AS borrowerstate_u,
                ''::VARCHAR AS county_norm,
                0::BIGINT AS loan_count,
                0::DOUBLE AS loan_total,
                0::DOUBLE AS forgiven_total,
                0::DOUBLE AS jobs_reported
            WHERE 0=1
            """
        )

    # -------------------------------------------------
    # county_scores (from scoring scripts)
    # -------------------------------------------------
    if not _table_exists(con, "county_scores"):
        con.execute(
            """
            CREATE TABLE county_scores AS
            SELECT 
                ''::VARCHAR AS GEOID,
                ''::VARCHAR AS NAME,
                0::DOUBLE AS score_raw,
                0::DOUBLE AS score_norm
            WHERE 0=1
            """
        )

    # -------------------------------------------------
    # acs_county
    # (Populated by ACS pipeline)
    # -------------------------------------------------
    if not _table_exists(con, "acs_county"):
        con.execute(
            """
            CREATE TABLE acs_county AS
            SELECT 
                ''::VARCHAR AS GEOID,
                ''::VARCHAR AS NAME,
                ''::VARCHAR AS state,
                ''::VARCHAR AS county,
                0::BIGINT AS Total_Pop,
                0::BIGINT AS Median_Income,
                0::BIGINT AS Poverty_Count,
                0::BIGINT AS Poverty_Universe,
                0::DOUBLE AS Poverty_Rate,
                0::BIGINT AS Unemployed,
                0::BIGINT AS Labor_Force,
                0::DOUBLE AS Unemployment_Rate
            WHERE 0=1
            """
        )

    # Done
    return None
