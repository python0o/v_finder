import duckdb
import pandas as pd
import os

DB_PATH = "data/db/v_finder.duckdb"
PR_CENTROIDS = "data/geo/pr_centroids.csv"


def main():
    print("=== Puerto Rico County Patch v4 ===")

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DuckDB not found at {DB_PATH}")

    if not os.path.exists(PR_CENTROIDS):
        raise FileNotFoundError(f"PR centroid file not found at {PR_CENTROIDS}")

    con = duckdb.connect(DB_PATH)

    # Ensure county_ref exists
    try:
        existing = con.execute(
            "SELECT GEOID, STUSPS, NAME, latitude, longitude FROM county_ref"
        ).df()
    except duckdb.CatalogException as exc:
        raise RuntimeError(
            "county_ref table does not exist. Run rebuild_county_ref_base.py first."
        ) from exc

    print(f"Existing county_ref rows: {len(existing)}")

    # Load PR centroids from CSV
    pr_df = pd.read_csv(PR_CENTROIDS, dtype=str)

    required_cols = ["GEOID", "NAME", "STUSPS", "INTPTLAT", "INTPTLONG"]
    for col in required_cols:
        if col not in pr_df.columns:
            raise ValueError(f"PR centroid file missing required column: {col}")

    # Normalize GEOID
    pr_df["GEOID"] = pr_df["GEOID"].astype(str).str.zfill(5)
    pr_df["NAME"] = pr_df["NAME"].astype(str)
    pr_df["STUSPS"] = "PR"

    # Determine which PR GEOIDs are missing
    existing_ids = set(existing["GEOID"].astype(str))
    pr_missing = pr_df[~pr_df["GEOID"].isin(existing_ids)].copy()

    print(f"Total PR rows in file: {len(pr_df)}")
    print(f"Missing PR GEOIDs to insert: {len(pr_missing)}")

    if pr_missing.empty:
        print("No new PR counties to add. Patch is effectively a no-op.")
        return

    # Register the missing PR rows as a DuckDB view
    con.register("pr_missing_df", pr_missing)

    # Insert into county_ref with proper casting into latitude/longitude
    con.execute(
        """
        INSERT INTO county_ref (GEOID, STUSPS, NAME, latitude, longitude)
        SELECT
            GEOID,
            STUSPS,
            NAME,
            CAST(INTPTLAT AS DOUBLE),
            CAST(INTPTLONG AS DOUBLE)
        FROM pr_missing_df
        """
    )

    new_count = con.execute("SELECT COUNT(*) FROM county_ref").fetchone()[0]
    print(f"county_ref patched successfully. New row count: {new_count}")


if __name__ == "__main__":
    main()
