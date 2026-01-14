# rebuild_county_ref.py
import duckdb
import pandas as pd
import os

DB = r"C:\scripts\v_finder\data\db\v_finder.duckdb"
SRC = r"C:\scripts\v_finder\data\reference\county_ref_master.csv"

print(f"Using DB: {DB}")
print(f"Loading:  {SRC}")

# Load CSV
df = pd.read_csv(SRC, dtype=str)

# Enforce required columns
required = ["GEOID", "STATEFP", "COUNTYFP", "STUSPS", "NAME", "NAME_NORM", "INTPTLAT", "INTPTLONG", "ALAND"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise RuntimeError(f"Missing required columns: {missing}")

# Convert numeric
for col in ["INTPTLAT", "INTPTLONG", "ALAND"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

con = duckdb.connect(DB)

print("Dropping old county_ref…")
con.execute("DROP TABLE IF EXISTS county_ref")

print("Registering df_view…")
con.register("df_view", df)

print("Creating county_ref…")
con.execute("""
    CREATE TABLE county_ref AS
    SELECT
        GEOID,
        STATEFP,
        COUNTYFP,
        STUSPS,
        NAME,
        NAME_NORM,
        INTPTLAT::DOUBLE,
        INTPTLONG::DOUBLE,
        ALAND::DOUBLE
    FROM df_view
""")

print("\n=== Verification ===")
print(con.execute("PRAGMA show_tables").fetchdf())
print(con.execute("SELECT * FROM county_ref LIMIT 10").fetchdf())
print(con.execute("SELECT COUNT(*) FROM county_ref").fetchdf())

print("\nCounty_ref rebuild COMPLETE.")
