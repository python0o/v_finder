"""
map/deck.py — v8 (Lat/Lon + Modern Visuals)

Nationwide county-level map using deck.gl via pydeck.

- Uses county_ref.latitude / county_ref.longitude
- Colors & elevation derived from county_scores (risk + PPP intensity)
"""

from typing import Optional

import math

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st


# =====================================================================
# Data loader
# =====================================================================


def load_map_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Loads a DataFrame joining county_scores with county_ref.

    Expected schemas:
        county_scores: GEOID, STUSPS, NAME, risk_score, risk_tier,
                       ppp_loan_count, ppp_per_capita, Total_Pop
        county_ref:    GEOID, STUSPS, NAME, latitude, longitude
    """

    def exists(tbl: str) -> bool:
        return (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [tbl.lower()],
            ).fetchone()[0]
            == 1
        )

    if not exists("county_scores"):
        return pd.DataFrame()

    if not exists("county_ref"):
        return pd.DataFrame()

    df = con.execute(
        """
        SELECT
            s.GEOID,
            s.STUSPS,
            s.NAME,
            s.risk_score,
            s.risk_tier,
            s.ppp_loan_count,
            s.ppp_per_capita,
            s.Total_Pop,
            r.latitude AS lat,
            r.longitude AS lon
        FROM county_scores s
        LEFT JOIN county_ref r
            ON s.GEOID = r.GEOID
        WHERE r.latitude IS NOT NULL
          AND r.longitude IS NOT NULL
        """
    ).fetchdf()

    return df


# =====================================================================
# Layer builders
# =====================================================================


def _compute_color(df: pd.DataFrame, color_metric: str) -> pd.DataFrame:
    v = df[color_metric].fillna(0)
    vmax = v.max() if v.max() > 0 else 1.0
    norm = (v / vmax).clip(0, 1)

    df = df.copy()
    df["color_r"] = (norm * 255).astype(int)
    df["color_g"] = (120 - norm * 120).clip(0, 120).astype(int)
    df["color_b"] = (255 - norm * 255).astype(int)
    df["color_a"] = 180

    df["color"] = df[["color_r", "color_g", "color_b", "color_a"]].values.tolist()
    return df


def _compute_radius(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    pop = df["Total_Pop"].fillna(0).astype(float) + 1.0

    def _rad(x: float) -> float:
        lnx = math.log(x) if x > 0 else 0.0
        # 6k–24k band scaled by population
        return 6000.0 + min(18000.0, 2000.0 * lnx)

    df["radius"] = pop.apply(_rad)
    return df


def make_scatter_layer(df: pd.DataFrame, color_metric: str) -> pdk.Layer:
    df = _compute_color(df, color_metric)
    df = _compute_radius(df)

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
    )


def make_heat_layer(df: pd.DataFrame, weight_metric: str) -> pdk.Layer:
    return pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position=["lon", "lat"],
        get_weight=weight_metric,
        radiusPixels=45,
        aggregation="MEAN",
    )


def make_hex_layer(df: pd.DataFrame, elev_metric: str) -> pdk.Layer:
    df = df.copy()
    elev = df[elev_metric].fillna(0)
    emax = elev.max() if elev.max() > 0 else 1.0
    df["elev"] = (elev / emax) * 3000.0

    return pdk.Layer(
        "HexagonLayer",
        data=df,
        get_position=["lon", "lat"],
        get_elevation="elev",
        elevation_scale=1,
        extruded=True,
        radius=16000,
    )


# =====================================================================
# Public entrypoint
# =====================================================================


def render_county_map(con: duckdb.DuckDBPyConnection) -> None:
    """
    Draws a deck.gl map inside Streamlit.
    """
    df = load_map_data(con)

    if df is None or df.empty:
        st.warning("No map data available. Build county_ref + county_scores first.")
        return

    st.caption(f"{len(df):,} counties available for visualization")

    # Controls
    col1, col2 = st.columns([2, 2])

    with col1:
        mode = st.selectbox(
            "Visualization Mode",
            ["Risk Scatter", "PPP Heatmap", "Risk Hex"],
            index=0,
        )

    with col2:
        base_style = st.selectbox(
            "Base Map Style",
            [
                "mapbox://styles/mapbox/dark-v11",
                "mapbox://styles/mapbox/light-v11",
                "mapbox://styles/mapbox/outdoors-v12",
                "mapbox://styles/mapbox/satellite-streets-v12",
            ],
            index=0,
        )

    if mode == "Risk Scatter":
        color_metric = st.selectbox(
            "Color by",
            ["risk_score", "ppp_per_capita", "ppp_loan_count"],
            index=0,
        )
        layer = make_scatter_layer(df, color_metric)

    elif mode == "PPP Heatmap":
        weight_metric = st.selectbox(
            "Heat by",
            ["ppp_per_capita", "ppp_loan_count"],
            index=0,
        )
        layer = make_heat_layer(df, weight_metric)

    else:  # Risk Hex
        elev_metric = st.selectbox(
            "Extrude by",
            ["risk_score", "ppp_per_capita"],
            index=0,
        )
        layer = make_hex_layer(df, elev_metric)

    view_state = pdk.ViewState(
        latitude=39.5,
        longitude=-98.35,
        zoom=4.2,
        pitch=40,
    )

    tooltip = {
        "html": """
        <b>{NAME}</b> ({STUSPS})<br/>
        GEOID: {GEOID}<br/>
        Risk Score: {risk_score}<br/>
        Tier: {risk_tier}<br/>
        PPP Loans: {ppp_loan_count}<br/>
        PPP $ / Resident: {ppp_per_capita}
        """,
        "style": {"color": "white"},
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style=base_style,
    )

    st.pydeck_chart(deck)
