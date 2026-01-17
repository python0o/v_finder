from __future__ import annotations

import os
from pathlib import Path
import duckdb
import streamlit as st

# =============================================================================
# Environment + Cloud Detection (SAFE)
# =============================================================================

def _is_streamlit_cloud() -> bool:
    """
    Detect Streamlit Community Cloud safely.
    st.secrets throws if no secrets.toml exists, so wrap everything.
    """
    try:
        if bool(st.secrets.get("STREAMLIT_CLOUD", False)):
            return True
    except Exception:
        pass

    # Filesystem heuristics used by Streamlit Cloud
    if Path("/mount/src").exists():
        return True
    if Path("/home/sandbox").exists():
        return True

    return False


# =============================================================================
# DuckDB Bootstrap
# =============================================================================

def _open_duckdb() -> duckdb.DuckDBPyConnection:
    """
    Open DuckDB in read-only mode for analytics.
    """
    # Default DB path (repo-relative)
    default_db = Path("data/db/v_finder.duckdb")

    # Allow override via env var (Cloud-safe)
    db_path = Path(os.environ.get("V_FINDER_DB", default_db))

    if not db_path.exists():
        st.error(f"DuckDB file not found: {db_path}")
        st.stop()

    return duckdb.connect(str(db_path), read_only=True)


def _ensure_compat_views(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure unqualified compatibility views exist for hybrid deployments.

    This allows UI code to reference:
      - county_scores
      - lender_profiles
      - county_lender_signals
      - county_ref

    even when the physical tables live in:
      - analytics.*
      - core.*
    """

    def has_table(schema: str, name: str) -> bool:
        try:
            return bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE lower(table_schema) = lower(?)
                      AND lower(table_name) = lower(?)
                    """,
                    [schema, name],
                ).fetchone()[0]
            )
        except Exception:
            return False

    def has_any(name: str) -> bool:
        try:
            return bool(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE lower(table_name) = lower(?)
                    """,
                    [name],
                ).fetchone()[0]
            )
        except Exception:
            return False

    mappings = [
        ("county_scores", "analytics"),
        ("lender_profiles", "analytics"),
        ("county_lender_signals", "analytics"),
        ("county_ref", "core"),
    ]

    for table, schema in mappings:
        # If already accessible unqualified, do nothing
        if has_any(table):
            continue

        # If schema-qualified table exists, create compat view
        if has_table(schema, table):
            try:
                con.execute(
                    f"CREATE VIEW {table} AS SELECT * FROM {schema}.{table}"
                )
            except Exception:
                # View may already exist or be created concurrently; ignore
                pass


# =============================================================================
# UI Imports (AFTER BOOTSTRAP)
# =============================================================================

def _import_pages():
    """
    Import UI modules only after DuckDB + compat views are ready.
    """
    from ui.dashboard import render_dashboard_page
    from ui.mission_control import render_mission_control_page
    from ui.outliers import load_outliers  # noqa: F401 (used indirectly)
    from ui.lender_profile import render_lender_profile_page
    from ui.network_graph import render_network_graph
    from map.deck import render_nationwide_map
    from health.panel import render_health_panel

    return {
        "Dashboard": render_dashboard_page,
        "Mission Control": render_mission_control_page,
        "Nationwide Map": render_nationwide_map,
        "Lender Profile": render_lender_profile_page,
        "Lender Network": render_network_graph,
        "System Health": render_health_panel,
    }


# =============================================================================
# Status Badge
# =============================================================================

def _render_status_badge(con: duckdb.DuckDBPyConnection) -> None:
    cloud = _is_streamlit_cloud()

    cols = st.columns(4)
    cols[0].metric("Mode", "Cloud" if cloud else "Local")
    cols[1].metric("DuckDB", "Connected")
    cols[2].metric("PPP Source", "Capella")
    cols[3].metric("Architecture", "Hybrid")

    st.markdown("---")


# =============================================================================
# Main App
# =============================================================================

def main() -> None:
    st.set_page_config(
        page_title="V_FINDER",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # -------------------------------------------------------------------------
    # Open DB + bootstrap
    # -------------------------------------------------------------------------
    con = _open_duckdb()
    _ensure_compat_views(con)

    # -------------------------------------------------------------------------
    # Header
    # -------------------------------------------------------------------------
    st.title("V_FINDER")
    st.caption("PPP + ACS County-Level Fraud & Risk Analytics (Hybrid Architecture)")
    _render_status_badge(con)

    # -------------------------------------------------------------------------
    # Navigation
    # -------------------------------------------------------------------------
    pages = _import_pages()

    with st.sidebar:
        st.subheader("Navigation")
        page = st.radio("Go to", list(pages.keys()))

    # -------------------------------------------------------------------------
    # Render selected page
    # -------------------------------------------------------------------------
    try:
        pages[page](con)
    except Exception as e:
        st.error("Unhandled application error")
        st.exception(e)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    main()
