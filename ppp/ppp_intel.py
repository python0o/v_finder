# =====================================================================
# ppp_intel.py — v5 Ultra
# Dynamic PPP analytics engine (schema-agnostic + fraud-integrated)
# =====================================================================

from __future__ import annotations

import duckdb
import pandas as pd
from typing import Optional, List, Dict


# =====================================================================
# Helpers
# =====================================================================


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()
        return True
    except Exception:
        return False


def _get_columns(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    try:
        df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
        return df["name"].str.lower().tolist()
    except Exception:
        return []


# =====================================================================
# Column detection (makes module bulletproof across PPP datasets)
# =====================================================================


def detect_ppp_columns(con: duckdb.DuckDBPyConnection) -> Dict[str, Optional[str]]:
    """
    PPP files differ month-to-month, year-to-year, and by SBA revisions.
    This dynamically detects the best matching columns.

    Returns dictionary:
        borrower_col
        borrower_city
        borrower_state
        borrower_zip
        county_norm
        loan_amount
        forgiveness_amount
        lender_name
        lender_state
        naics
        jobs
        age_desc
    """

    cols = _get_columns(con, "ppp_clean")

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in cols:
                return n
        return None

    return {
        "borrower_col": pick("borrowername", "borrower_name"),
        "borrower_city": pick("borrowercity", "projectcity"),
        "borrower_state": pick("borrowerstate", "projectstate", "borrowerstate_u"),
        "borrower_zip": pick("borrowerzip", "projectzip"),
        "county_norm": pick("county_norm", "projectcountyname"),
        "loan_amount": pick("currentapprovalamount", "initialapprovalamount", "loanamount"),
        "forgiveness_amount": pick("forgivenessamount"),
        "lender_name": pick("originatinglender", "servicinglendername"),
        "lender_state": pick("originatinglenderstate", "servicinglenderstate"),
        "naics": pick("naicscode"),
        "jobs": pick("jobsreported"),
        "age_desc": pick("businessagedescription"),
    }


# =====================================================================
# Main Intel Queries
# =====================================================================


def get_top_borrowers_for_county(
    con: duckdb.DuckDBPyConnection,
    county_name_norm: str,
    limit: int = 25,
) -> pd.DataFrame:
    """
    Returns the largest PPP borrowers in a county.
    """

    if not table_exists(con, "ppp_clean"):
        return pd.DataFrame()

    c = detect_ppp_columns(con)

    needed = [
        "borrower_col",
        "loan_amount",
        "county_norm",
    ]
    if any(c[k] is None for k in needed):
        return pd.DataFrame()  # schema incomplete

    sql = f"""
        SELECT
            {c['borrower_col']} AS borrower,
            {c['borrower_city']} AS city,
            {c['borrower_state']} AS state,
            {c['loan_amount']} AS loan_amount,
            {c['forgiveness_amount']} AS forgiveness_amount,
            {c['jobs']} AS jobs,
            {c['naics']} AS naics,
            {c['age_desc']} AS business_age

        FROM ppp_clean
        WHERE LOWER({c['county_norm']}) = LOWER(?)
        ORDER BY CAST({c['loan_amount']} AS DOUBLE) DESC NULLS LAST
        LIMIT {limit}
    """

    return con.execute(sql, [county_name_norm]).fetchdf()


def get_largest_single_loans(
    con: duckdb.DuckDBPyConnection,
    limit: int = 25,
) -> pd.DataFrame:
    """
    National-level biggest PPP loans.
    """

    if not table_exists(con, "ppp_clean"):
        return pd.DataFrame()

    c = detect_ppp_columns(con)

    if c["loan_amount"] is None:
        return pd.DataFrame()

    sql = f"""
        SELECT
            {c['borrower_col']} AS borrower,
            {c['borrower_city']} AS city,
            {c['borrower_state']} AS state,
            {c['county_norm']} AS county,
            CAST({c['loan_amount']} AS DOUBLE) AS loan_amount,
            {c['naics']} AS naics,
            {c['jobs']} AS jobs,
            {c['age_desc']} AS business_age
        FROM ppp_clean
        ORDER BY CAST({c['loan_amount']} AS DOUBLE) DESC NULLS LAST
        LIMIT {limit}
    """

    return con.execute(sql).fetchdf()


def get_top_lenders_by_dollars(
    con: duckdb.DuckDBPyConnection,
    limit: int = 25,
) -> pd.DataFrame:
    """
    Returns top PPP lenders nationally by approved dollars.
    """

    if not table_exists(con, "ppp_clean"):
        return pd.DataFrame()

    c = detect_ppp_columns(con)

    if c["lender_name"] is None or c["loan_amount"] is None:
        return pd.DataFrame()

    sql = f"""
        SELECT
            {c['lender_name']} AS lender,
            COUNT(*) AS total_loans,
            SUM(CAST({c['loan_amount']} AS DOUBLE)) AS total_dollars
        FROM ppp_clean
        GROUP BY {c['lender_name']}
        ORDER BY total_dollars DESC NULLS LAST
        LIMIT {limit}
    """

    return con.execute(sql).fetchdf()


def get_top_lenders_for_county(
    con: duckdb.DuckDBPyConnection,
    county_name_norm: str,
    limit: int = 20,
) -> pd.DataFrame:
    """
    Returns lenders active in a given county.
    """

    if not table_exists(con, "ppp_clean"):
        return pd.DataFrame()

    c = detect_ppp_columns(con)

    if c["lender_name"] is None or c["county_norm"] is None:
        return pd.DataFrame()

    sql = f"""
        SELECT
            {c['lender_name']} AS lender,
            COUNT(*) AS loans,
            SUM(CAST({c['loan_amount']} AS DOUBLE)) AS total_dollars
        FROM ppp_clean
        WHERE LOWER({c['county_norm']}) = LOWER(?)
        GROUP BY {c['lender_name']}
        ORDER BY total_dollars DESC NULLS LAST
        LIMIT {limit}
    """

    return con.execute(sql, [county_name_norm]).fetchdf()


# =====================================================================
# Borrower ↔ Lender Joined Intel
# =====================================================================


def get_borrower_lender_pairs(
    con: duckdb.DuckDBPyConnection,
    county_name_norm: Optional[str] = None,
    limit: int = 50,
) -> pd.DataFrame:
    """
    Retrieves borrower+lender pairs with loan metrics.

    Example columns:
      borrower, lender, county, loan_amount, forgiveness_amount
    """

    if not table_exists(con, "ppp_clean"):
        return pd.DataFrame()

    c = detect_ppp_columns(con)

    required = [
        "borrower_col",
        "lender_name",
        "county_norm",
        "loan_amount",
    ]
    if any(c[k] is None for k in required):
        return pd.DataFrame()

    where_clause = ""
    params = []

    if county_name_norm:
        where_clause = f"WHERE LOWER({c['county_norm']}) = LOWER(?)"
        params = [county_name_norm]

    sql = f"""
        SELECT
            {c['borrower_col']} AS borrower,
            {c['lender_name']} AS lender,
            {c['county_norm']} AS county,
            CAST({c['loan_amount']} AS DOUBLE) AS loan_amount,
            {c['forgiveness_amount']} AS forgiveness_amount,
            {c['naics']} AS naics,
            {c['jobs']} AS jobs
        FROM ppp_clean
        {where_clause}
        ORDER BY loan_amount DESC NULLS LAST
        LIMIT {limit}
    """

    return con.execute(sql, params).fetchdf()

# ============================================================================
# LENDER–BORROWER WEB (COUNTY-SCOPED)
# ============================================================================

def get_lender_borrower_web(
    con: duckdb.DuckDBPyConnection,
    geoid: str,
    lender_name: str,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Borrower-level view for a given (county, lender).

    Relies on the same GEOID join logic as get_top_borrowers_for_county:
        ppp_clean p  JOIN county_ref c
          ON p.borrowerstate_u = c.STUSPS
         AND LOWER(p.county_norm) = LOWER(c.NAME)

    Returns columns:
        Borrower, Address, City, State, LoanAmount, Forgiveness, JobsReported,
        NAICS, BusinessAge, Lender
    """
    sql = """
    SELECT
        p.borrowername              AS Borrower,
        p.borroweraddress           AS Address,
        p.borrowercity              AS City,
        p.borrowerstate_u           AS State,
        TRY_CAST(p.currentapprovalamount AS DOUBLE)  AS LoanAmount,
        TRY_CAST(p.forgivenessamount      AS DOUBLE)  AS ForgivenessAmount,
        TRY_CAST(p.jobsreported           AS BIGINT)  AS JobsReported,
        p.naicscode                AS NAICS,
        p.businessagedescription   AS BusinessAge,
        p.lendername               AS Lender
    FROM ppp_clean p
    JOIN county_ref c
      ON p.borrowerstate_u = c.STUSPS
     AND LOWER(p.county_norm) = LOWER(c.NAME)
    WHERE c.GEOID = ?
      AND p.lendername = ?
    ORDER BY LoanAmount DESC NULLS LAST
    LIMIT ?
    """

    df = con.execute(sql, [geoid, lender_name, limit]).df()

    # Basic numeric cleaning
    if "LoanAmount" in df.columns:
        df["LoanAmount"] = df["LoanAmount"].fillna(0).astype(float).round(2)
    if "ForgivenessAmount" in df.columns:
        df["ForgivenessAmount"] = df["ForgivenessAmount"].fillna(0).astype(float).round(2)
    if "JobsReported" in df.columns:
        df["JobsReported"] = df["JobsReported"].fillna(0).astype(int)

    return df

