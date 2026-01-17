"""
map/deck.py — Nationwide County Map (Hybrid Safe)

Responsibilities:
- Render nationwide county-level risk heatmap
- DuckDB analytics only (no PPP dependency)
- Works with split or monolithic DB via compat views
- Public entry point: render_nationwide_map(con, default_state)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
import pydeck as pdk


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        return bool(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower(?)",
                [name],
            ).fetchone()[0]
        )
    except Exception:
        return False


def _load_county_map_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load county-level lat/lon + risk metrics for mapping.
    Uses compat views created in app.py.
    """
    if not _table_exists(con, "county_scores"):
        return pd.DataFrame()

    # county_ref provides lat/lon + names
    join_ref = _table_exists(con, "county_ref")

    sql = f"""
        SELECT
            cs.GEOID,
            cs.risk_score,
            cr.NAME,
            cr.STUSPS,
            cr.lat,
            cr.lon
        FROM county_scores cs
        {"LEFT JOIN county_ref cr ON cs.GEOID = cr.GEOID" if join_ref else ""}
        WHERE cr.lat IS NOT NULL AND cr.lon IS NOT NULL
    """

    try:
        df = con.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()

    # enforce numeric
    for c in ["lat", "lon", "risk_score"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.dropna(subset=["lat", "lon"])


# ---------------------------------------------------------------------
# Public render function (THIS IS WHAT app.py IMPORTS)
# ---------------------------------------------------------------------
def render_nationwide_map(
    con: duckdb.DuckDBPyConnection,
    default_state: str | None = None,
) -> None:
    """
    Render a nationwide county risk map.

    Parameters
    ----------
    con : DuckDBPyConnection
        Active analytics connection
    default_state : str | None
        Optional 2-letter state code for initial focus
    """
    st.title("Nationwide Risk Map")
    st.caption("County-level fraud and risk heatmap (analytics-only)")

    df = _load_county_map_frame(con)
    if df.empty:
        st.warning("No county map data available.")
        return

    # Optional state filter
    if default_state and "STUSPS" in df.columns:
        df = df[df["STUSPS"] == default_state]

    # Color scale by risk
    max_risk = float(df["risk_score"].max()) if "risk_score" in df.columns else 1.0
    df["risk_norm"] = df["risk_score"] / max(max_risk, 1e-6)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[lon, lat]",
        get_radius=1500,
        get_fill_color="[255 * risk_norm, 50, 50, 160]",
        pickable=True,
    )

    view = pdk.ViewState(
        latitude=float(df["lat"].mean()),
        longitude=float(df["lon"].mean()),
        zoom=4,
        pitch=0,
    )

    tooltip = {
        "html": "<b>{NAME}</b><br/>Risk: {risk_score}",
        "style": {"backgroundColor": "black", "color": "white"},
    }

    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view,
            tooltip=tooltip,
        ),
        use_container_width=True,
    )
