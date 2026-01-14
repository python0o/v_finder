
from __future__ import annotations

from typing import Tuple

import duckdb
import pandas as pd


def _exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
    return con.execute(q, [name]).fetchone()[0] > 0


def build_normalization_report(
    con: duckdb.DuckDBPyConnection,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (summary_df, sample_df, outliers_df) for the profiles tab.

    This is intentionally robust: it works even if some tables are missing and
    degrades gracefully instead of throwing.
    """

    # Summary metrics
    summary_rows = []

    for table_name, label in [
        ("county_ref", "County reference rows"),
        ("ppp_clean", "PPP loan-level rows"),
        ("county_agg", "PPP county aggregates"),
        ("acs_county", "ACS county records"),
        ("county_scores", "Scored counties"),
    ]:
        if _exists(con, table_name):
            cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        else:
            cnt = 0
        summary_rows.append({"Metric": label, "Value": int(cnt)})

    summary_df = pd.DataFrame(summary_rows)

    # Simple sample view – show a few counties with joined PPP/ACS if available
    sample_df = pd.DataFrame()
    if _exists(con, "county_ref"):
        q = """
            SELECT
                r.GEOID,
                r.STUSPS,
                r.NAME,
                COALESCE(a.loan_count, 0)      AS loan_count,
                COALESCE(a.loan_total, 0)      AS loan_total,
                COALESCE(acs.Total_Pop, NULL)  AS population,
                COALESCE(acs.Median_Income, NULL) AS median_income,
                COALESCE(acs.Poverty_Rate, NULL)  AS poverty_rate
            FROM county_ref r
            LEFT JOIN county_agg a USING (GEOID)
            LEFT JOIN acs_county acs USING (GEOID)
            LIMIT 25
        """
        sample_df = con.execute(q).fetchdf()

    # Outliers – if scoring exists, surface the top-N by risk_score
    outliers_df = pd.DataFrame()
    if _exists(con, "county_scores"):
        q = """
            SELECT
                GEOID,
                STUSPS,
                NAME,
                risk_score,
                risk_rank,
                loan_per_capita,
                count_per_1k,
                poverty_rate,
                unemp_rate
            FROM county_scores
            ORDER BY risk_score DESC
            LIMIT 50
        """
        outliers_df = con.execute(q).fetchdf()

    return summary_df, sample_df, outliers_df
