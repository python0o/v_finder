import duckdb
import pandas as pd

DB = "data/db/v_finder.duckdb"
CENT_FILE = "data/geo/county_full.csv"  # This file contains GEOID, STUSPS, NAME, latitude, longitude


def main():
    print("=== Rebuilding BASE county_ref (no PR yet) ===")

    con = duckdb.connect(DB)

    # Load centroid file
    df = pd.read_csv(CENT_FILE, dtype=str)

    required = ["GEOID", "STUSPS", "NAME", "latitude", "longitude"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column in centroid file: {col}")

    # Standardize GEOID
    df["GEOID"] = df["GEOID"].astype(str).str.zfill(5)

    # Write DataFrame to an in-memory DuckDB relation
    con.register("tmp_df", df)

    # Drop old table
    con.execute("DROP TABLE IF EXISTS county_ref")

    # Create new table with explicit schema
    con.execute(
        """
        CREATE TABLE county_ref (
            GEOID VARCHAR,
            STUSPS VARCHAR,
            NAME VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE
        );
        """
    )

    # Insert from registered DF
    con.execute(
        """
        INSERT INTO county_ref
        SELECT
            GEOID,
            STUSPS,
            NAME,
            CAST(latitude AS DOUBLE),
            CAST(longitude AS DOUBLE)
        FROM tmp_df
        """
    )

    count = con.execute("SELECT COUNT(*) FROM county_ref").fetchone()[0]
    print(f"county_ref base rebuilt successfully. Rows: {count}")


if __name__ == "__main__":
    main()
