import os
import time
import duckdb
import sys

if len(sys.argv) < 2:
    print("Usage: python db_watch.py path/to/v_finder.duckdb")
    sys.exit(1)

db_path = sys.argv[1]

def get_file_size(path):
    if os.path.exists(path):
        return os.path.getsize(path)
    return 0

con = duckdb.connect(db_path, read_only=True)  # Read-only to avoid locks

while True:
    size = get_file_size(db_path)
    print(f"[{time.strftime('%H:%M:%S')}] DB size: {size / 1024:.2f} KB")
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"Tables: {tables}")
        if ('ppp_clean',) in tables:
            rows = con.execute("SELECT COUNT(*) FROM ppp_clean").fetchone()[0]
            print(f"ppp_clean rows: {rows}")
    except Exception as e:
        print(f"Error querying DB: {str(e)}")
    time.sleep(5)  # Poll every 5s