"""
app.py — FINAL LAUNCH BUILD (UI MODES ENABLED)

V_FINDER Fraud Intelligence Platform

Guarantees:
- Safe module-based routing (no named imports from legacy UI files)
- Basic / Enhanced / Vivid UI modes
- Health panel isolated and non-blocking
- No deprecated Streamlit APIs
- No walrus operators
- Single cached DuckDB connection
"""
import os
import streamlit as st

DB_PATH = "data/db/v_finder.duckdb"

def ensure_database():
    if os.path.exists(DB_PATH):
        return True

    st.error("Database not found.")
    st.info(
        "Upload the V_FINDER DuckDB file to initialize the application.\n\n"
        "This is a one-time setup step."
    )

    uploaded = st.file_uploader(
        "Upload v_finder.duckdb",
        type=["duckdb"],
        accept_multiple_files=False
    )

    if uploaded is not None:
        os.makedirs("data/db", exist_ok=True)
        with open(DB_PATH, "wb") as f:
            f.write(uploaded.read())

        st.success("Database uploaded successfully.")
        st.warning("Please reboot the app using the menu in the top right.")
        st.stop()

    st.stop()

# Call this before anything else
ensure_database()

from __future__ import annotations

import duckdb
import streamlit as st

# ---------------------------------------------------------------------
# SAFE module imports (no named imports from legacy files)
# ---------------------------------------------------------------------
import ui.dashboard as dashboard
import ui.status as status

from ui.mission_control import render_mission_control_page
from ui.county_profile import render_county_profile_page
from ui.compare_counties import render_compare_counties_page
from ui.network_graph import render_network_graph
from ui.fraud_simulator import render_fraud_simulator_page
from ui.lender_profile import render_lender_profile_page

from health.panel import render_health_panel
from ui.theme import apply_theme


# ---------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="V_FINDER — Fraud Intelligence",
    page_icon="🕵️",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = "data/db/v_finder.duckdb"


# ---------------------------------------------------------------------
# Database connection (single, cached)
# ---------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


# ---------------------------------------------------------------------
# Safe render wrappers for legacy modules
# ---------------------------------------------------------------------
def _safe_render_module(mod, title: str, con: duckdb.DuckDBPyConnection) -> None:
    if hasattr(mod, "render_dashboard"):
        mod.render_dashboard(con)
    elif hasattr(mod, "render_dashboard_page"):
        mod.render_dashboard_page(con)
    elif hasattr(mod, "render_status"):
        mod.render_status(con)
    elif hasattr(mod, "render_status_page"):
        mod.render_status_page(con)
    elif hasattr(mod, "render"):
        mod.render(con)
    elif hasattr(mod, "main"):
        mod.main(con)
    else:
        st.title(title)
        st.info(
            f"{title} module loaded, but no render entrypoint was found. "
            "Mission Control is the primary operational interface."
        )


def render_dashboard_safe(con: duckdb.DuckDBPyConnection) -> None:
    _safe_render_module(dashboard, "Dashboard", con)


def render_status_safe(con: duckdb.DuckDBPyConnection) -> None:
    _safe_render_module(status, "System Status", con)


# ---------------------------------------------------------------------
# Navigation registry (single source of truth)
# ---------------------------------------------------------------------
NAV_PAGES = {
    "Dashboard": render_dashboard_safe,
    "Mission Control": render_mission_control_page,
    "County Profile": render_county_profile_page,
    "Compare Counties": render_compare_counties_page,
    "Lender Network": render_network_graph,
    "Lender Profile": render_lender_profile_page,
    "Fraud Simulator": render_fraud_simulator_page,
    "System Status": render_status_safe,
}

DEFAULT_PAGE = "Mission Control"


# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------
def render_sidebar():
    st.sidebar.title("V_FINDER")

    # -----------------------------
    # UI MODE (THEME)
    # -----------------------------
    st.sidebar.markdown("### Display Mode")

    ui_mode = st.sidebar.selectbox(
        "UI Theme",
        ["Basic", "Enhanced", "Vivid"],
        index=1,
        help="Basic = audit-safe · Enhanced = balanced · Vivid = investigative/demo",
    )

    st.session_state["vf_ui_mode"] = ui_mode

    st.sidebar.markdown("---")

    # -----------------------------
    # Navigation
    # -----------------------------
    if "vf_nav_target" not in st.session_state:
        st.session_state["vf_nav_target"] = DEFAULT_PAGE

    page = st.sidebar.radio(
        "Navigation",
        list(NAV_PAGES.keys()),
        index=list(NAV_PAGES.keys()).index(
            st.session_state.get("vf_nav_target", DEFAULT_PAGE)
        ),
    )

    if page != st.session_state.get("vf_nav_target"):
        st.session_state["vf_nav_target"] = page

    st.sidebar.markdown("---")

    # -----------------------------
    # Health Panel Toggle
    # -----------------------------
    show_health = st.sidebar.toggle(
        "Show System Health",
        value=True,
        help="Pipeline, data freshness, and integrity checks",
    )

    return page, show_health


# ---------------------------------------------------------------------
# Main application loop
# ---------------------------------------------------------------------
def main() -> None:
    con = get_connection()

    page, show_health = render_sidebar()

    # -----------------------------
    # Apply UI theme BEFORE rendering pages
    # -----------------------------
    apply_theme(st.session_state.get("vf_ui_mode", "Enhanced"))

    # -----------------------------
    # Health panel (isolated)
    # -----------------------------
    if show_health:
        with st.expander("System Health", expanded=False):
            try:
                render_health_panel(con)
            except Exception as e:
                st.error(f"Health panel error: {e}")

    # -----------------------------
    # Guard against invalid routing
    # -----------------------------
    if page not in NAV_PAGES:
        st.warning("Invalid navigation state detected. Resetting to default.")
        st.session_state["vf_nav_target"] = DEFAULT_PAGE
        st.rerun()

    # -----------------------------
    # Render selected page
    # -----------------------------
    try:
        NAV_PAGES[page](con)
    except Exception as e:
        st.error("Unhandled application error")
        st.exception(e)


# ---------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
