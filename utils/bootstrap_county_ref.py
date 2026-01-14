# utils/bootstrap_county_ref.py
#
# Convenience "auto-repair" entry point for county_ref.
#
# 1) Rebuild county_ref from data/geo/county_2025.csv
# 2) Run a lightweight PPP → county_ref join sanity check (if ppp_clean exists)

from __future__ import annotations

import os
import sys

import duckdb
import pandas as pd

from .county_ref_rebuild import rebuild_county_ref, ROOT, DB_PATH


def _safe_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame | str:
    try:
        return con.execute(sql).fetchdf()
    except Exception as exc:
        return f"ERROR: {exc}"


def _run_join_sanity(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== PPP → county_ref join sanity ===")

    # Check if ppp_clean exists
    exists_df = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'ppp_clean'
    """).fetchdf()

    if exists_df.empty:
        print("ppp_clean does not exist yet – skipping join sanity.")
        return

    res = _safe_query(
        con,
        """
        SELECT
            p.borrowerstate_u,
            p.county_norm,
            r.STUSPS,
            r.NAME_NORM,
            COUNT(*) AS rows_joined
        FROM ppp_clean p
        LEFT JOIN county_ref r
            ON p.borrowerstate_u = r.STUSPS
           AND p.county_norm     = r.NAME_NORM
        GROUP BY 1,2,3,4
        ORDER BY rows_joined DESC
        LIMIT 20
        """,
    )

    print("\nTop 20 PPP → county_ref join groups:")
    print(res if isinstance(res, str) else res.to_string(index=False))


def main() -> None:
    print("=== bootstrap_county_ref ===")
    print(f"Project root: {ROOT}")
    print(f"DB path     : {DB_PATH}")

    # Step 1: Rebuild county_ref from CSV
    rebuild_county_ref()

    # Step 2: Run basic join sanity if PPP is present
    con = duckdb.connect(DB_PATH)
    _run_join_sanity(con)
    con.close()

    print("\nbootstrap_county_ref COMPLETE.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
