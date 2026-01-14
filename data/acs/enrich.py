"""
ACS Enrichment – Universal-Compatible Version
---------------------------------------------

This module fetches ACS 5-year county-level data and writes it to
table `acs_county` with a STABLE, CANONICAL schema:

    GEOID               TEXT (state+county FIPS)
    NAME                TEXT (Census county name)
    state               TEXT (2-digit FIPS string)
    county              TEXT (3-digit FIPS string)
    Total_Pop           DOUBLE
    Median_Income       DOUBLE
    Poverty_Count       DOUBLE
    Poverty_Universe    DOUBLE
    Poverty_Rate        DOUBLE
    Unemployed          DOUBLE
    Labor_Force         DOUBLE
    Unemployment_Rate   DOUBLE

It uses a SINGLE national query:

    GET:  NAME, <ACS_VARS...>
    FOR:  county:*

No 'state' in GET (avoids the 400 "unknown variable 'state'").

If `data/acs_cache/acs_<year>.csv` exists, it uses the cached file
instead of calling the API.

Signature is compatible with past calls:

    refresh_acs_county(con, year=2022, api_key="...", use_cache=True)

Extra kwargs are safely ignored.
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional

import duckdb
import pandas as pd
import requests

# Default year if UI does not specify
ACS_DEFAULT_YEAR = 2022

# Canonical ACS vars → column names
ACS_VARS = {
    "B01003_001E": "Total_Pop",
    "B19013_001E": "Median_Income",
    "B17001_002E": "Poverty_Count",
    "B17001_001E": "Poverty_Universe",
    "B23025_005E": "Unemployed",
    "B23025_003E": "Labor_Force",
}

ACS_ENDPOINT_TMPL = "https://api.census.gov/data/{year}/acs/acs5"


# ---------------------------------------------------------------------------
# Paths / cache helpers
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ACS_CACHE_DIR = os.path.join(BASE_DIR, "data", "acs_cache")


def _ensure_cache_dir() -> None:
    os.makedirs(ACS_CACHE_DIR, exist_ok=True)


def _cache_path(year: int) -> str:
    return os.path.join(ACS_CACHE_DIR, f"acs_{year}.csv")


# ---------------------------------------------------------------------------
# Core fetch logic – NATIONAL QUERY (no per-state loops)
# ---------------------------------------------------------------------------

def _fetch_acs_from_api(year: int, api_key: Optional[str]) -> pd.DataFrame:
    """
    Fetch ACS county-level data via a single national query.

    Uses:
        GET:  NAME,<ACS_VARS>
        FOR:  county:*
        KEY:  api_key (if provided)

    Returns a DataFrame with columns:
        NAME, <ACS VAR KEYS>, state, county
    """
    url = ACS_ENDPOINT_TMPL.format(year=year)

    get_vars = ["NAME"] + list(ACS_VARS.keys())
    params = {
        "get": ",".join(get_vars),
        "for": "county:*",
    }
    if api_key:
        params["key"] = api_key

    resp = requests.get(url, params=params, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(f"ACS API error ({year}): {exc}") from exc

    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"ACS API returned non-JSON response for {year}") from exc

    if not data or len(data) < 2:
        raise RuntimeError(f"ACS API returned no data for {year}")

    df = pd.DataFrame(data[1:], columns=data[0])
    return df


def _canonicalize_acs_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw ACS DF into canonical schema with stable names.
    Expects columns: NAME, <ACS VAR KEYS>, state, county.
    """
    # Ensure required geography fields exist
    for col in ("NAME", "state", "county"):
        if col not in df.columns:
            raise RuntimeError(f"ACS response missing expected column: {col}")

    # Build GEOID
    df["GEOID"] = df["state"].astype(str) + df["county"].astype(str)

    # Rename ACS numeric fields
    rename_map = ACS_VARS.copy()
    df = df.rename(columns=rename_map)

    # Ensure all canonical numeric fields exist even if missing from rename
    for col in rename_map.values():
        if col not in df.columns:
            df[col] = None

    # Extra derived metrics
    df["Poverty_Rate"] = pd.to_numeric(
        df.get("Poverty_Count"), errors="coerce"
    ) / pd.to_numeric(df.get("Poverty_Universe"), errors="coerce")

    df["Unemployment_Rate"] = pd.to_numeric(
        df.get("Unemployed"), errors="coerce"
    ) / pd.to_numeric(df.get("Labor_Force"), errors="coerce")

    # Coerce numerics
    numeric_cols = [
        "Total_Pop",
        "Median_Income",
        "Poverty_Count",
        "Poverty_Universe",
        "Poverty_Rate",
        "Unemployed",
        "Labor_Force",
        "Unemployment_Rate",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep only canonical columns we care about
    keep_cols = [
        "GEOID",
        "NAME",
        "state",
        "county",
        "Total_Pop",
        "Median_Income",
        "Poverty_Count",
        "Poverty_Universe",
        "Poverty_Rate",
        "Unemployed",
        "Labor_Force",
        "Unemployment_Rate",
    ]
    df = df[keep_cols]

    # Drop obvious duplicates on GEOID (keep first)
    df = df.drop_duplicates(subset=["GEOID"])

    return df


def _load_from_cache(year: int) -> Optional[pd.DataFrame]:
    _ensure_cache_dir()
    path = _cache_path(year)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, dtype=str)
            return df
        except Exception:
            return None
    return None


def _save_to_cache(year: int, df: pd.DataFrame) -> None:
    _ensure_cache_dir()
    path = _cache_path(year)
    df.to_csv(path, index=False)


# ---------------------------------------------------------------------------
# Public API: refresh_acs_county
# ---------------------------------------------------------------------------

def refresh_acs_county(
    con: duckdb.DuckDBPyConnection,
    year: int,
    api_key: Optional[str] = None,
    use_cache: bool = True,
    **_: Any,
) -> Dict[str, Any]:
    """
    Refresh ACS county-level table.

    Args:
        con: DuckDB connection.
        year: ACS 5-year year (e.g., 2019–2023).
        api_key: Census API key. If None, uses env CENSUS_API_KEY if set.
        use_cache: If True, use/maintain data/acs_cache/acs_<year>.csv.

    Returns:
        dict with:
            rows: number of rows in acs_county
            year: the year fetched
            source: "cache" or "api"
            error: optional error string
    """
    result: Dict[str, Any] = {"rows": 0, "year": year, "source": None}

    # Resolve API key
    api_key = (api_key or os.environ.get("CENSUS_API_KEY") or "").strip() or None

    # Try cache first
    df_raw: Optional[pd.DataFrame] = None
    used_cache = False
    if use_cache:
        df_cached = _load_from_cache(year)
        if df_cached is not None and not df_cached.empty:
            df_raw = df_cached
            used_cache = True

    # If no cache or empty -> hit API
    if df_raw is None:
        try:
            df_raw = _fetch_acs_from_api(year, api_key)
        except Exception as exc:  # noqa: BLE001
            result["error"] = str(exc)
            return result

    # Canonicalize
    try:
        df = _canonicalize_acs_df(df_raw)
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"ACS canonicalization error: {exc}"
        return result

    if df.empty:
        result["error"] = f"ACS produced empty canonical DF for {year}"
        return result

    # Save cache if from API
    if not used_cache and use_cache:
        try:
            _save_to_cache(year, df)
        except Exception:
            # Non-fatal
            pass

    # Write to DuckDB
    con.execute("DROP TABLE IF EXISTS acs_county")
    con.register("_acs", df)
    con.execute("CREATE TABLE acs_county AS SELECT * FROM _acs")
    con.unregister("_acs")

    rowcount = con.execute("SELECT COUNT(*) FROM acs_county").fetchone()[0]

    result["rows"] = int(rowcount)
    result["source"] = "cache" if used_cache else "api"
    return result
