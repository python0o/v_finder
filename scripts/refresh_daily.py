from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import duckdb

from ppp.ingest import ingest_ppp_directory
from acs.enrich import refresh_acs_county, ACS_DEFAULT_YEAR
from fraud.score import score_counties
from geo.loader import ensure_county_ref


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    db_path = os.environ.get(
        "VFINDER_DUCKDB_PATH",
        str(base_dir / "v_finder.duckdb"),
    )

    log_dir = base_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"daily_refresh_{datetime.utcnow().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        filename=str(log_file),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("=== V_FINDER Daily Refresh Start ===")
    logging.info("DB path: %s", db_path)

    con = duckdb.connect(db_path)

    try:
        county_ref_csv = str(base_dir / "data" / "geo" / "county_full.csv")
        ensure_county_ref(con, county_ref_csv)
        logging.info("county_ref ready.")

        ppp_dir = str(base_dir / "data" / "ppp")
        for pct, msg in ingest_ppp_directory(
            ppp_dir=ppp_dir,
            con=con,
            county_ref_csv=county_ref_csv,
            force=True,
            reingest_mode="full",
        ):
            logging.info("[PPP %3d%%] %s", pct, msg)

        acs_res = refresh_acs_county(con, year=int(ACS_DEFAULT_YEAR))
        logging.info("ACS refresh result: %s", acs_res)

        score_res = score_counties(con)
        logging.info("Score result: %s", score_res)

        logging.info("=== V_FINDER Daily Refresh Complete ===")
    except Exception as exc:
        logging.exception("Refresh failed: %s", exc)
    finally:
        con.close()


if __name__ == "__main__":
    main()
