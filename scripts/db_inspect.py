import duckdb
import pandas as pd

DB_PATH = 'data/v_finder.duckdb'  # Adjust if needed

def inspect_database():
    con = duckdb.connect(DB_PATH, read_only=True)  # Read-only to avoid locks

    # List all tables
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in database:")
    for table in tables:
        print(f"- {table[0]}")

    # For each table, show schema, row count, and sample rows
    for table in tables:
        table_name = table[0]
        print(f"\n--- {table_name} ---")
        
        # Schema
        schema = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
        print("Schema:")
        print(schema[['name', 'type', 'notnull']])
        
        # Row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Row count: {row_count}")

        # Sample rows (top 5)
        if row_count > 0:
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
            print("Sample rows:")
            print(sample)
        else:
            print("No rows in table.")

    con.close()

if __name__ == '__main__':
    inspect_database()