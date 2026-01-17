from __future__ import annotations

from pathlib import Path
from typing import Tuple

import duckdb
import streamlit as st

from ui.mission_control import render_mission_control_page
from ui.county_profile import render_county_profile_page
from ui.lender_profile import render_lender_profile_page
from ui.network_graph import render_network_graph
from ui.compare_counties import render_compare_counties_page
from ui.fraud_simulator import render_fraud_simulator_page
from map.deck import render_nationwide_map

try:
    from health.panel import render_health_panel
    HAS_HEALTH_PANEL = True
except Exception:
    render_health_panel = None
    HAS_HEALTH_PANEL = False


def _is_streamlit_cloud() -> bool:
    """Heuristic detection for Streamlit Community Cloud."""
    return bool(
        st.secrets.get("STREAMLIT_CLOUD", False)
        or Path("/mount/src").exists()
        or Path("/home/sandbox").exists()
    )


def _table_exists(con: duckdb.DuckDBPyConnection, table: str, schema: str | None = None) -> bool:
    """Return True if a table exists.

    If schema is provided (e.g., 'analytics' or 'core'), the check is restricted
    to that schema.

    Parameters
    ----------
    table : table name without schema
    schema: optional schema name
    """
    if schema:
        q = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
        """
        return bool(con.execute(q, [schema, table]).fetchone()[0])
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE lower(table_name) = lower(?)
    """
    return bool(con.execute(q, [table]).fetchone()[0])


def _db_paths() -> Tuple[Path, Path, Path]:
    """Return (monolithic, core, analytics) paths."""
    base = Path("data") / "db"
    return (base / "v_finder.duckdb", base / "core.duckdb", base / "analytics.duckdb")


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open DuckDB connection and ATTACH split DBs if present."""
    mono, core, analytics = _db_paths()
    mono.parent.mkdir(parents=True, exist_ok=True)

    def _enable_schema_compat(c: duckdb.DuckDBPyConnection) -> None:
        """Make schema resolution deterministic in both local + Streamlit Cloud.

        The monolithic v_finder.duckdb stores tables under schemas (analytics/core).
        Many UI modules still reference unqualified names (e.g., county_scores).
        We enforce a search_path AND create compatibility views in main.
        """
        try:
            c.execute("SET search_path TO analytics, core, main;")
        except Exception:
            pass

        # Create main-schema compatibility views (best-effort; never fail startup)
        view_map = {
            "county_scores": "analytics.county_scores",
            "county_agg": "analytics.county_agg",
            "county_lender_signals": "analytics.county_lender_signals",
            "lender_profiles": "analytics.lender_profiles",
            "acs_county": "core.acs_county",
            "county_ref": "core.county_ref",
        }
        for v, target in view_map.items():
            try:
                c.execute(f"CREATE OR REPLACE VIEW {v} AS SELECT * FROM {target}")
            except Exception:
                # Target may not exist in some dev shells; ignore.
                continue

    if mono.exists():
        con = duckdb.connect(str(mono), read_only=read_only)
        # If split DBs also exist, prefer them for speed/clarity
        if core.exists():
            con.execute(f"ATTACH '{core.as_posix()}' AS core")
        if analytics.exists():
            con.execute(f"ATTACH '{analytics.as_posix()}' AS analytics")
        _enable_schema_compat(con)
        return con

    # No monolithic DB: create an in-memory shell and attach available DBs
    con = duckdb.connect(":memory:")
    if core.exists():
        con.execute(f"ATTACH '{core.as_posix()}' AS core")
    if analytics.exists():
        con.execute(f"ATTACH '{analytics.as_posix()}' AS analytics")
    _enable_schema_compat(con)
    return con


def _attach_ppp_shard_for_state(con: duckdb.DuckDBPyConnection, state: str) -> Tuple[bool, str]:
    """Attach per-state PPP shard DB if present.

    Attaches as schema name 'ppp'.
    Expected file pattern: data/db/ppp/ppp_<ST>.duckdb
    """
    st_code = (state or "").upper().strip()
    if len(st_code) != 2:
        return False, "Select a 2-letter state to attach PPP shard."

    shard = Path("data") / "db" / "ppp" / f"ppp_{st_code}.duckdb"
    if not shard.exists():
        return False, f"PPP shard not found for {st_code} (expected {shard})."

    # Detach first if already attached
    try:
        con.execute("DETACH ppp")
    except Exception:
        pass

    con.execute(f"ATTACH '{shard.as_posix()}' AS ppp")
    return True, f"PPP shard attached: {shard.name}"


def _render_status_badge(con: duckdb.DuckDBPyConnection) -> None:
    cloud = _is_streamlit_cloud()
    mode = "Cloud" if cloud else "Local"

    # Determine if we are split or monolithic
    mono, core, analytics = _db_paths()
    split = (core.exists() or analytics.exists()) and not mono.exists()
    layout = "Split DB" if split else "Monolithic DB"

    ppp_attached = bool(st.session_state.get("vf_ppp_attached", False))

    left, right = st.columns([0.65, 0.35])
    with left:
        st.caption(f"Mode: {mode} | Storage: {layout}")
    with right:
        if ppp_attached:
            st.success("PPP: ATTACHED")
        else:
            st.info("PPP: OFF")


def _init_state() -> None:
    st.session_state.setdefault("vf_state", "")
    st.session_state.setdefault("vf_ppp_attached", False)
    st.session_state.setdefault("vf_ppp_auto_attach", True)


def _available_states(con: duckdb.DuckDBPyConnection) -> list[str]:
    # Prefer analytics.county_scores if attached, else main.county_scores
    for schema in ("analytics", "main"):
        try:
            if _table_exists(con, 'county_scores', schema=schema):
                df = con.execute(f"SELECT DISTINCT STUSPS FROM {schema}.county_scores ORDER BY STUSPS").fetchdf()
                return [str(x) for x in df["STUSPS"].dropna().tolist()]
        except Exception:
            continue
    return []


def _sidebar_nav(con: duckdb.DuckDBPyConnection) -> str:
    st.sidebar.title("V_FINDER")

    states = _available_states(con)
    default_ix = 0
    if states and st.session_state.get("vf_state") in states:
        default_ix = states.index(st.session_state["vf_state"])

    if states:
        st.session_state["vf_state"] = st.sidebar.selectbox(
            "Active state",
            states,
            index=default_ix,
            help="Used for default fast views and optional PPP shard attachment.",
        )
    else:
        st.session_state["vf_state"] = st.sidebar.text_input("Active state (2-letter)", value=st.session_state.get("vf_state", ""))

    st.session_state["vf_ppp_auto_attach"] = st.sidebar.checkbox(
        "Auto-attach PPP shard (if present)",
        value=bool(st.session_state.get("vf_ppp_auto_attach", True)),
    )

    pages = [
        "Mission Control",
        "Nationwide Map",
        "County Profile",
        "Lender Profile",
        "Lender Network",
        "Compare Counties",
        "Fraud Simulator",
    ]
    if HAS_HEALTH_PANEL:
        pages.append("Health Panel")

    return st.sidebar.radio("Navigate", pages, index=0)


def main() -> None:
    st.set_page_config(page_title="V_FINDER", layout="wide", initial_sidebar_state="expanded")
    _init_state()

    con = get_connection(read_only=False)
    try:
        page = _sidebar_nav(con)

        # PPP shard attach (optional)
        if st.session_state.get("vf_ppp_auto_attach", True):
            ok, _msg = _attach_ppp_shard_for_state(con, st.session_state.get("vf_state", ""))
            st.session_state["vf_ppp_attached"] = bool(ok)
        else:
            st.session_state["vf_ppp_attached"] = False

        _render_status_badge(con)

        if page == "Mission Control":
            render_mission_control_page(con)
        elif page == "Nationwide Map":
            render_nationwide_map(con, default_state=st.session_state.get("vf_state", ""))
        elif page == "County Profile":
            render_county_profile_page(con)
        elif page == "Lender Profile":
            render_lender_profile_page(con)
        elif page == "Lender Network":
            render_network_graph(con)
        elif page == "Compare Counties":
            render_compare_counties_page(con)
        elif page == "Fraud Simulator":
            render_fraud_simulator_page(con)
        elif page == "Health Panel" and HAS_HEALTH_PANEL:
            render_health_panel(con)  # type: ignore[misc]
        else:
            st.error("Unknown page.")

    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
