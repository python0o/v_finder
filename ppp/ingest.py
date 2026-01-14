# ppp/ingest.py
#
# Robust PPP ingest:
# - Normalizes column names
# - Builds borrowerstate_u and county_norm
# - Rebuilds ppp_clean schema on first successful chunk
# - Works both from app (ingest_ppp_directory) and as a CLI script.
#
# Expected schema for ppp_clean (55 columns):
#   loannumber, dateapproved, sbaofficecode, processingmethod, borrowername,
#   borroweraddress, borrowercity, borrowerstate, borrowerzip, loanstatusdate,
#   loanstatus, term, sbaguarantypercentage, initialapprovalamount,
#   currentapprovalamount, undisbursedamount, franchisename,
#   servicinglenderlocationid, servicinglendername, servicinglenderaddress,
#   servicinglendercity, servicinglenderstate, servicinglenderzip,
#   ruralurbanindicator, hubzoneindicator, lmiindicator,
#   businessagedescription, projectcity, projectcountyname, projectstate,
#   projectzip, cd, jobsreported, naicscode, race, ethnicity,
#   utilitiesproceed, payrollproceed, mortgageinterestproceed, rentproceed,
#   refinanceeidlproceed, healthcareproceed, debtinterestproceed,
#   businesstype, originatinglenderlocationid, originatinglender,
#   originatinglendercity, originatinglenderstate, gender, veteran, nonprofit,
#   forgivenessamount, forgivenessdate, borrowerstate_u, county_norm

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator, Tuple

import duckdb
import pandas as pd


PPP_EXPECTED_COLS = [
    "loannumber",
    "dateapproved",
    "sbaofficecode",
    "processingmethod",
    "borrowername",
    "borroweraddress",
    "borrowercity",
    "borrowerstate",
    "borrowerzip",
    "loanstatusdate",
    "loanstatus",
    "term",
    "sbaguarantypercentage",
    "initialapprovalamount",
    "currentapprovalamount",
    "undisbursedamount",
    "franchisename",
    "servicinglenderlocationid",
    "servicinglendername",
    "servicinglenderaddress",
    "servicinglendercity",
    "servicinglenderstate",
    "servicinglenderzip",
    "ruralurbanindicator",
    "hubzoneindicator",
    "lmiindicator",
    "businessagedescription",
    "projectcity",
    "projectcountyname",
    "projectstate",
    "projectzip",
    "cd",
    "jobsreported",
    "naicscode",
    "race",
    "ethnicity",
    "utilitiesproceed",
    "payrollproceed",
    "mortgageinterestproceed",
    "rentproceed",
    "refinanceeidlproceed",
    "healthcareproceed",
    "debtinterestproceed",
    "businesstype",
    "originatinglenderlocationid",
    "originatinglender",
    "originatinglendercity",
    "originatinglenderstate",
    "gender",
    "veteran",
    "nonprofit",
    "forgivenessamount",
    "forgivenessdate",
    "borrowerstate_u",
    "county_norm",
]


def _normalize_column_name(col: str) -> str:
    # Lowercase and strip non-alphanumerics.
    # e.g. "LoanNumber" -> "loannumber"
    #      "UTILITIES_PROCEED" -> "utilitiesproceed"
    import re

    s = col.strip().lower()
    s = re.sub(r"[^0-9a-z]+", "", s)
    return s


def _normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a raw PPP DataFrame chunk into the canonical schema."""

    # 1) Normalize column names
    df = df.copy()
    df.columns = [_normalize_column_name(c) for c in df.columns]

    # 2) Ensure required base columns exist (fill missing with empty string)
    for col in PPP_EXPECTED_COLS:
        if col not in df.columns and col not in ("borrowerstate_u", "county_norm"):
            df[col] = ""

    # 3) Build borrowerstate_u
    if "borrowerstate" in df.columns:
        df["borrowerstate_u"] = (
            df["borrowerstate"].fillna("").astype(str).str.strip().str.upper()
        )
    else:
        df["borrowerstate_u"] = ""

    # 4) Build county_norm from projectcountyname
    import re

    if "projectcountyname" in df.columns:
        c = df["projectcountyname"].fillna("").astype(str).str.upper().str.strip()

        # Remove common suffixes
        for suffix in [" COUNTY", " PARISH", " BOROUGH", " MUNICIPALITY", " CITY"]:
            c = c.str.replace(suffix, "", regex=False)

        # Remove non-letter/space
        c = c.str.replace(r"[^A-Z ]", "", regex=True)
        c = c.str.replace(r"\s+", " ", regex=True).str.strip()

        df["county_norm"] = c
    else:
        df["county_norm"] = ""

    # 5) Reorder columns to PPP_EXPECTED_COLS and drop extras
    missing_core = [c for c in PPP_EXPECTED_COLS if c not in df.columns]
    if missing_core:
        # Safety: if we somehow still miss core columns, create them.
        for col in missing_core:
            df[col] = ""

    df = df[PPP_EXPECTED_COLS]
    return df


def _create_or_replace_log_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ppp_ingest_log (
            file_path   VARCHAR,
            chunk_index INTEGER,
            row_count   BIGINT,
            status      VARCHAR,
            error       VARCHAR
        )
        """
    )


def _append_log(
    con: duckdb.DuckDBPyConnection,
    file_path: str,
    chunk_index: int,
    row_count: int,
    status: str,
    error: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO ppp_ingest_log (file_path, chunk_index, row_count, status, error)
        VALUES (?, ?, ?, ?, ?)
        """,
        [file_path, chunk_index, row_count, status, error],
    )


def ingest_ppp_directory(
    con: duckdb.DuckDBPyConnection,
    ppp_dir: str,
    force: bool = False,
    chunk_rows: int = 200_000,
) -> Generator[Tuple[float, str], None, None]:
    """
    Ingest all PPP CSVs in ppp_dir into ppp_clean.

    Yields (progress_fraction, message) for UI / logging.
    """

    ppp_path = Path(ppp_dir)
    if not ppp_path.exists() or not ppp_path.is_dir():
        msg = f"PPP directory not found: {ppp_dir}"
        yield 0.0, msg
        return

    files = sorted(
        [
            f
            for f in ppp_path.iterdir()
            if f.is_file() and f.suffix.lower() == ".csv"
        ]
    )

    if not files:
        yield 0.0, f"No PPP CSV files found in {ppp_dir}"
        return

    # Optionally reset ppp_clean if force=True
    if force:
        con.execute("DROP TABLE IF EXISTS ppp_clean")
        con.execute("DROP TABLE IF EXISTS ppp_ingest_log")

    _create_or_replace_log_table(con)

    total_files = len(files)
    total_rows_inserted = 0
    first_chunk_global = True

    for file_idx, path in enumerate(files, start=1):
        file_str = str(path)
        yield file_idx / total_files, f"Scanning PPP file {file_idx}/{total_files}: {file_str}"

        try:
            chunk_iter = pd.read_csv(
                file_str,
                dtype=str,
                encoding="latin-1",
                chunksize=chunk_rows,
            )
        except Exception as e:
            _append_log(con, file_str, -1, 0, "READ_ERROR", str(e))
            yield file_idx / total_files, f"ERROR reading {file_str}: {e}"
            continue

        chunk_index = 0

        for raw_chunk in chunk_iter:
            chunk_index += 1
            if raw_chunk is None or raw_chunk.empty:
                continue

            try:
                chunk = _normalize_chunk(raw_chunk)
            except Exception as e:
                _append_log(
                    con,
                    file_str,
                    chunk_index,
                    len(raw_chunk),
                    "NORMALIZE_ERROR",
                    str(e),
                )
                yield file_idx / total_files, f"ERROR normalizing chunk {chunk_index} of {file_str}: {e}"
                continue

            # Register chunk as a DuckDB view
            con.register("ppp_chunk", chunk)

            try:
                if first_chunk_global:
                    # Drop any broken schema that may still exist
                    con.execute("DROP TABLE IF EXISTS ppp_clean")
                    con.execute("CREATE TABLE ppp_clean AS SELECT * FROM ppp_chunk")
                    first_chunk_global = False
                else:
                    # Append
                    con.execute("INSERT INTO ppp_clean SELECT * FROM ppp_chunk")

                _append_log(
                    con,
                    file_str,
                    chunk_index,
                    len(chunk),
                    "OK",
                    None,
                )

                total_rows_inserted += len(chunk)
                yield file_idx / total_files, (
                    f"Ingested chunk {chunk_index} from {file_str} "
                    f"(rows={len(chunk)}, total_rows={total_rows_inserted})"
                )

            except Exception as e:
                _append_log(
                    con,
                    file_str,
                    chunk_index,
                    len(chunk),
                    "WRITE_ERROR",
                    str(e),
                )
                yield file_idx / total_files, (
                    f"ERROR writing chunk {chunk_index} of {file_str}: {e}"
                )

            finally:
                # Always unregister to avoid stale views
                try:
                    con.unregister("ppp_chunk")
                except Exception:
                    pass

    if first_chunk_global:
        # Nothing ever got created
        yield 1.0, (
            "PPP ingest completed, but no valid rows were inserted into ppp_clean "
            "(all chunks failed or were empty). Check PPP column mappings."
        )
    else:
        yield 1.0, f"PPP ingest completed successfully. Total rows inserted: {total_rows_inserted}"


# ---------------------------------------------------------------------------
# CLI Entry Point (optional)
# ---------------------------------------------------------------------------

def _default_db_path() -> str:
    base = Path(__file__).resolve().parents[1]  # ...\v_finder
    return str(base / "data" / "db" / "v_finder.duckdb")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PPP ingest into DuckDB")
    parser.add_argument(
        "--db",
        default=_default_db_path(),
        help="Path to DuckDB file (default: data/db/v_finder.duckdb)",
    )
    parser.add_argument(
        "--ppp_dir",
        default=str(Path(__file__).resolve().parents[1] / "data" / "ppp"),
        help="Directory containing PPP CSV files",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop and recreate ppp_clean and ppp_ingest_log before ingest",
    )
    args = parser.parse_args()

    print(f"Using DB: {args.db}")
    print(f"PPP dir: {args.ppp_dir}")
    con_cli = duckdb.connect(args.db)

    for pct, msg in ingest_ppp_directory(con_cli, args.ppp_dir, force=args.force):
        print(f"[{pct:0.2%}] {msg}")
