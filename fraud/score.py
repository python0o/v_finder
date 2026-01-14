"""
fraud/score.py — Ultra Fraud & Risk Scoring Engine (v7)

This module owns the analytical side of the PPP + ACS stack:

* acs_dictionary         – human-readable metadata for ACS/PPP fields
* county_scores          – county-level risk model (PPP + ACS)
* lender_profiles        – lender-level influence / exposure index
* county_lender_signals  – county × lender dominance & share

Design goals:
    - Deterministic
    - DuckDB-native (pure SQL, no Pandas DataFrame binding)
    - Safe to import from Streamlit (no side effects on import)
"""

from pathlib import Path
from typing import Dict, Any

import duckdb

# ---------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------

DB_PATH = Path("data") / "db" / "v_finder.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Simple local DuckDB connection.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=False)


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """
    Check if a table exists in the current DuckDB connection.
    """
    q = """
    SELECT COUNT(*) 
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return bool(con.execute(q, [name.lower()]).fetchone()[0])


# ---------------------------------------------------------------------
# ACS Dictionary
# ---------------------------------------------------------------------


def ensure_acs_dictionary(con: duckdb.DuckDBPyConnection) -> int:
    """
    Build a small ACS+PPP data dictionary inside DuckDB.

    Pure SQL (no DataFrame binding) to avoid
    "Unable to transform python value of type DataFrame" errors.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS acs_dictionary (
            variable_code TEXT,
            name          TEXT,
            description   TEXT,
            topic         TEXT,
            table_id      TEXT
        )
        """
    )

    # Idempotent: refresh contents each run
    con.execute("DELETE FROM acs_dictionary")

    con.execute(
        """
        INSERT INTO acs_dictionary (variable_code, name, description, topic, table_id)
        VALUES
            ('Total_Pop',       'Total Population',
             'Total resident population for the county (ACS).',
             'Demographics',    'S0101'),

            ('Poverty_Rate',    'Poverty Rate',
             'Share of people whose income is below the poverty threshold.',
             'Poverty',         'S1701'),

            ('Unemployment_Rate','Unemployment Rate',
             'Unemployment rate for the civilian labor force (16+).',
             'Labor',           'S2301'),

            ('ppp_loan_count', 'PPP Loan Count',
             'Number of PPP loans successfully matched and cleaned for the county.',
             'PPP',             'PPP_AGG'),

            ('ppp_current_total', 'PPP Current Approval Total',
             'Current PPP approval dollars for matched loans.',
             'PPP',             'PPP_AGG'),

            ('ppp_per_capita', 'PPP Dollars per Resident',
             'PPP current approval dollars divided by total population.',
             'PPP',             'PPP_AGG'),

            ('risk_score',     'Risk Score',
             'Composite PPP–ACS fraud exposure index.',
             'Fraud',          'VF_RISK'),

            ('risk_tier',      'Risk Tier',
             'Qualitative banding of risk score (SEVERE/HIGH/ELEVATED/BASELINE/LOW).',
             'Fraud',          'VF_RISK'),

            ('risk_percentile_rank', 'Risk Percentile Rank',
             'Percentile rank of county risk score among all counties (0–100).',
             'Fraud',          'VF_RISK'),

            ('hidden_signal_score', 'Hidden Signal Score',
             'Anomaly score for unusual PPP patterns vs. ACS context.',
             'Fraud',          'VF_HIDDEN'),

            ('hidden_signal_tier', 'Hidden Signal Tier',
             'Tiered classification of anomaly intensity (CRITICAL/ANOMALOUS/WATCH/INTERESTING/MILD/NEUTRAL).',
             'Fraud',          'VF_HIDDEN')
        """
    )

    return con.execute("SELECT COUNT(*) FROM acs_dictionary").fetchone()[0]


# ---------------------------------------------------------------------
# County Scores (core risk + hidden signal)
# ---------------------------------------------------------------------


def build_county_scores(con: duckdb.DuckDBPyConnection) -> int:
    """
    Build county_scores from county_agg.

    Schema (aligned to UI + hidden signal use):
        GEOID, STUSPS, NAME,
        Total_Pop, Poverty_Rate, Unemployment_Rate,
        ppp_loan_count, ppp_current_total, ppp_per_capita,
        loans_per_1k, dollars_per_capita,
        risk_score, risk_rank, risk_percentile_rank, risk_tier,
        hidden_signal_score, hidden_signal_tier
    """
    if not table_exists(con, "county_agg"):
        raise RuntimeError(
            "county_agg does not exist. Run the ACS/PPP aggregation pipeline first."
        )

    con.execute("DROP TABLE IF EXISTS county_scores")

    con.execute(
        """
        CREATE TABLE county_scores AS
        WITH base AS (
            SELECT
                GEOID,
                STUSPS,
                NAME,
                Total_Pop,
                Poverty_Rate,
                Unemployment_Rate,
                ppp_loan_count,
                ppp_current_total,
                ppp_per_capita,
                CASE
                    WHEN Total_Pop IS NULL OR Total_Pop <= 0 THEN NULL
                    ELSE ppp_loan_count * 1000.0 / Total_Pop
                END AS loans_per_1k,
                CASE
                    WHEN Total_Pop IS NULL OR Total_Pop <= 0 THEN NULL
                    ELSE ppp_current_total / Total_Pop
                END AS dollars_per_capita
            FROM county_agg
        ),
        stats AS (
            SELECT
                avg(dollars_per_capita)              AS mu_dpc,
                stddev_pop(dollars_per_capita)       AS sd_dpc,
                avg(loans_per_1k)                    AS mu_lpk,
                stddev_pop(loans_per_1k)             AS sd_lpk,
                avg(Poverty_Rate)                    AS mu_pov,
                stddev_pop(Poverty_Rate)             AS sd_pov,
                avg(Unemployment_Rate)               AS mu_unemp,
                stddev_pop(Unemployment_Rate)        AS sd_unemp
            FROM base
        ),
        scored AS (
            SELECT
                b.*,
                CASE
                    WHEN sd_dpc IS NOT NULL AND sd_dpc > 0
                        THEN (dollars_per_capita - mu_dpc) / sd_dpc
                    ELSE 0
                END AS z_dpc,
                CASE
                    WHEN sd_lpk IS NOT NULL AND sd_lpk > 0
                        THEN (loans_per_1k - mu_lpk) / sd_lpk
                    ELSE 0
                END AS z_lpk,
                CASE
                    WHEN sd_pov IS NOT NULL AND sd_pov > 0
                        THEN (Poverty_Rate - mu_pov) / sd_pov
                    ELSE 0
                END AS z_pov,
                CASE
                    WHEN sd_unemp IS NOT NULL AND sd_unemp > 0
                        THEN (Unemployment_Rate - mu_unemp) / sd_unemp
                    ELSE 0
                END AS z_unemp
            FROM base b
            CROSS JOIN stats
        ),
        risk_raw AS (
            SELECT
                GEOID,
                STUSPS,
                NAME,
                Total_Pop,
                Poverty_Rate,
                Unemployment_Rate,
                ppp_loan_count,
                ppp_current_total,
                ppp_per_capita,
                loans_per_1k,
                dollars_per_capita,
                z_dpc,
                z_lpk,
                z_pov,
                z_unemp,
                (z_dpc * 0.40
                 + z_lpk * 0.30
                 + z_pov * 0.20
                 + z_unemp * 0.10) AS risk_score_raw
            FROM scored
        ),
        risk_bounded AS (
            SELECT
                *,
                CASE
                    WHEN risk_score_raw IS NULL THEN 0
                    WHEN risk_score_raw < -3.0 THEN -3.0
                    WHEN risk_score_raw >  5.0 THEN  5.0
                    ELSE risk_score_raw
                END AS risk_score
            FROM risk_raw
        ),
        risk_ranked AS (
            SELECT
                *,
                row_number() OVER (ORDER BY risk_score DESC) AS risk_rank,
                100.0 * (row_number() OVER (ORDER BY risk_score DESC) - 0.5)
                    / COUNT(*) OVER () AS risk_percentile_rank
            FROM risk_bounded
        ),
        risk_tiered AS (
            SELECT
                *,
                CASE
                    WHEN risk_score >= 3.0 THEN 'SEVERE'
                    WHEN risk_score >= 2.0 THEN 'HIGH'
                    WHEN risk_score >= 1.0 THEN 'ELEVATED'
                    WHEN risk_score >= 0.0 THEN 'BASELINE'
                    ELSE 'LOW'
                END AS risk_tier
            FROM risk_ranked
        ),
        hidden_raw AS (
            SELECT
                *,
                (
                    GREATEST(z_dpc, 0) * 0.50 +
                    GREATEST(z_lpk, 0) * 0.30 +
                    CASE
                        WHEN Poverty_Rate < 10 AND z_dpc > 1.5 THEN 0.50
                        ELSE 0
                    END +
                    CASE
                        WHEN Unemployment_Rate < 4 AND z_dpc > 1.5 THEN 0.30
                        ELSE 0
                    END
                ) AS hidden_signal_score_raw
            FROM risk_tiered
        ),
        hidden_final AS (
            SELECT
                GEOID,
                STUSPS,
                NAME,
                Total_Pop,
                Poverty_Rate,
                Unemployment_Rate,
                ppp_loan_count,
                ppp_current_total,
                ppp_per_capita,
                loans_per_1k,
                dollars_per_capita,
                risk_score,
                risk_rank,
                risk_percentile_rank,
                risk_tier,
                hidden_signal_score_raw AS hidden_signal_score,
                CASE
                    WHEN hidden_signal_score_raw >= 4.0 THEN 'CRITICAL'
                    WHEN hidden_signal_score_raw >= 3.0 THEN 'ANOMALOUS'
                    WHEN hidden_signal_score_raw >= 2.0 THEN 'WATCH'
                    WHEN hidden_signal_score_raw >= 1.0 THEN 'INTERESTING'
                    WHEN hidden_signal_score_raw >  0.0 THEN 'MILD'
                    ELSE 'NEUTRAL'
                END AS hidden_signal_tier
            FROM hidden_raw
        )
        SELECT * FROM hidden_final
        """
    )

    return con.execute("SELECT COUNT(*) FROM county_scores").fetchone()[0]


# ---------------------------------------------------------------------
# Lender Profiles (national)
# ---------------------------------------------------------------------


def build_lender_profiles(con: duckdb.DuckDBPyConnection) -> int:
    """
    Build lender_profiles from ppp_clean.

    Aggregates lender-level volume, average size, jobs, and influence rank.
    """
    if not table_exists(con, "ppp_clean"):
        raise RuntimeError("ppp_clean does not exist. Run the PPP ingest/normalize pipeline first.")

    con.execute("DROP TABLE IF EXISTS lender_profiles")

    con.execute(
        """
        CREATE TABLE lender_profiles AS
        WITH base AS (
            SELECT
                COALESCE(
                    NULLIF(originatinglender, ''),
                    NULLIF(servicinglendername, '')
                ) AS lender_name,
                borrowerstate_u AS borrower_state,
                county_norm,
                CAST(NULLIF(currentapprovalamount, '') AS DOUBLE) AS current_approval,
                CAST(NULLIF(initialapprovalamount, '') AS DOUBLE) AS initial_approval,
                CAST(NULLIF(jobsreported, '') AS DOUBLE) AS jobs_reported
            FROM ppp_clean
        ),
        cleaned AS (
            SELECT
                lender_name,
                borrower_state,
                county_norm,
                COALESCE(current_approval, initial_approval, 0) AS approval_amount,
                COALESCE(jobs_reported, 0) AS jobs_reported
            FROM base
            WHERE lender_name IS NOT NULL
        ),
        agg AS (
            SELECT
                lender_name,
                COUNT(*)              AS loan_count,
                SUM(approval_amount)  AS total_approved,
                AVG(approval_amount)  AS avg_loan,
                SUM(jobs_reported)    AS total_jobs
            FROM cleaned
            GROUP BY lender_name
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (ORDER BY total_approved DESC) AS influence_rank,
                100.0 * (row_number() OVER (ORDER BY total_approved DESC) - 0.5)
                    / COUNT(*) OVER () AS influence_percentile
            FROM agg
        )
        SELECT * FROM ranked
        """
    )

    return con.execute("SELECT COUNT(*) FROM lender_profiles").fetchone()[0]


# ---------------------------------------------------------------------
# County × Lender Signals (dominance per county)
# ---------------------------------------------------------------------


def build_county_lender_signals(con: duckdb.DuckDBPyConnection) -> int:
    """
    Build county_lender_signals:

    Per-county lender dominance and share, for Fraud Alert band / hidden overlays.
    """
    if not table_exists(con, "ppp_clean"):
        raise RuntimeError("ppp_clean does not exist.")
    if not table_exists(con, "county_agg"):
        raise RuntimeError("county_agg does not exist.")

    con.execute("DROP TABLE IF EXISTS county_lender_signals")

    con.execute(
        """
        CREATE TABLE county_lender_signals AS
        WITH base AS (
            SELECT
                COALESCE(
                    NULLIF(originatinglender, ''),
                    NULLIF(servicinglendername, '')
                ) AS lender_name,
                county_norm,
                CAST(NULLIF(currentapprovalamount, '') AS DOUBLE) AS current_approval,
                CAST(NULLIF(initialapprovalamount, '') AS DOUBLE) AS initial_approval
            FROM ppp_clean
        ),
        cleaned AS (
            SELECT
                lender_name,
                county_norm,
                COALESCE(current_approval, initial_approval, 0) AS approval_amount
            FROM base
            WHERE lender_name IS NOT NULL
        ),
        joined AS (
            SELECT
                c.GEOID,
                c.STUSPS,
                c.NAME,
                cleaned.lender_name,
                cleaned.approval_amount
            FROM cleaned
            JOIN county_agg c
                ON UPPER(cleaned.county_norm) = UPPER(c.NAME)
        ),
        agg AS (
            SELECT
                GEOID,
                STUSPS,
                NAME,
                lender_name,
                COUNT(*)             AS loan_count,
                SUM(approval_amount) AS total_approved
            FROM joined
            GROUP BY GEOID, STUSPS, NAME, lender_name
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY GEOID
                    ORDER BY loan_count DESC, total_approved DESC
                ) AS lender_rank,
                loan_count * 1.0
                    / SUM(loan_count) OVER (PARTITION BY GEOID) AS loan_share
            FROM agg
        )
        SELECT * FROM ranked
        """
    )

    return con.execute("SELECT COUNT(*) FROM county_lender_signals").fetchone()[0]


# ---------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------


def run_full_scoring(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Run the full scoring stack and return row counts.
    """
    results: Dict[str, Any] = {}
    results["acs_dictionary"] = ensure_acs_dictionary(con)
    results["county_scores"] = build_county_scores(con)
    results["lender_profiles"] = build_lender_profiles(con)
    results["county_lender_signals"] = build_county_lender_signals(con)
    return results


def main() -> None:
    print("=== Fraud & Risk Scoring Engine — v7 Ultra ===")
    con = get_connection()
    try:
        results = run_full_scoring(con)
    finally:
        con.close()

    print(f"✓ acs_dictionary: {results['acs_dictionary']}")
    print(f"✓ county_scores: {results['county_scores']}")
    print(f"✓ lender_profiles: {results['lender_profiles']}")
    print(f"✓ county_lender_signals: {results['county_lender_signals']}")


if __name__ == "__main__":
    main()
