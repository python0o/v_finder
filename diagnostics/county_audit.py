"""
County Alignment Audit — FINAL (typed)
"""

from __future__ import annotations
import duckdb
import pandas as pd


def run_county_alignment_audit(con):
    q_summary = """
        SELECT
            COUNT(*) AS total_clean,
            SUM(CASE WHEN matched = 1 THEN 1 ELSE 0 END) AS matched,
            100.0 * SUM(CASE WHEN matched = 1 THEN 1 ELSE 0 END) / COUNT(*) AS match_pct
        FROM (
            SELECT
                p.borrowerstate_u,
                p.county_norm,
                CASE WHEN r.GEOID IS NOT NULL THEN 1 ELSE 0 END AS matched
            FROM ppp_clean p
            LEFT JOIN county_ref r
              ON p.borrowerstate_u = r.STUSPS
             AND p.county_norm = r.NAME_NORM
        )
    """

    summary = con.execute(q_summary).fetchdf()

    q_unmatched = """
        SELECT
            borrowerstate_u,
            county_norm,
            COUNT(*) AS occurrences
        FROM ppp_clean p
        LEFT JOIN county_ref r
          ON p.borrowerstate_u = r.STUSPS
         AND p.county_norm = r.NAME_NORM
        WHERE r.GEOID IS NULL
        GROUP BY 1,2
        ORDER BY occurrences DESC
    """

    unmatched = con.execute(q_unmatched).fetchdf()

    return summary, unmatched
