from __future__ import annotations

"""
Map Renderer — PPP + ACS Overlays (SomoliScan++ Edition)
--------------------------------------------------------

Features:
    - PPP Loan Intensity mode (tier-colored)
    - Population overlay mode
    - Median Income overlay mode
    - Poverty Rate overlay mode
    - Uses county centroids (INTPTLAT / INTPTLONG)
"""

import duckdb
import pandas as pd
import streamlit as st
import pydeck as pdk


def _exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [name],
        ).fetchone()[0]
        == 1
    )


def render_map(con: duckdb.DuckDBPyConnection) -> None:
    if not _exists(con, "county_ref"):
        st.warning("Map disabled: county_ref not loaded.")
        return

    # Join PPP, ACS, scores into a single geo table
con.execute("DROP TABLE IF EXISTS county_scores")

con.execute(
    """
    CREATE TABLE county_scores AS
    SELECT
        r.GEOID,
        r.STUSPS,
        r.NAME,

        -- PPP columns (may be NULL if no PPP for county)
        COALESCE(a.loan_count, 0)                       AS loan_count,
        COALESCE(a.loan_total, 0)                       AS loan_total,

        -- ACS columns (ensure present for map)
        COALESCE(acs.population,        0)              AS population,
        COALESCE(acs.median_income,     0)              AS median_income,
        COALESCE(acs.poverty_rate,      0)              AS poverty_rate,
        COALESCE(acs.unemployment_rate, 0)              AS unemployment_rate,

        -- Quality flags
        CASE WHEN acs.GEOID IS NULL THEN 0 ELSE 1 END   AS acs_present,
        CASE WHEN a.GEOID   IS NULL THEN 0 ELSE 1 END   AS ppp_present,

        -- Risk score (simple composite)
        (
            COALESCE(a.loan_count, 0) * 0.4 +
            COALESCE(a.loan_total, 0) * 0.3 +
            (1 - COALESCE(acs.poverty_rate, 0)) * 0.2 +
            (1 - COALESCE(acs.unemployment_rate, 0)) * 0.1
        ) AS score,

        -- Tier class
        CASE
            WHEN (
                COALESCE(a.loan_count,0) +
                COALESCE(a.loan_total,0)
            ) > 0 THEN
                CASE
                    WHEN score >= 75 THEN 'HIGH'
                    WHEN score >= 50 THEN 'MEDIUM'
                    ELSE 'LOW'
                END
            ELSE 'NO DATA'
        END AS tier,

        CURRENT_TIMESTAMP AS asof_ts

    FROM county_ref r
    LEFT JOIN county_agg a
        ON r.GEOID = a.GEOID
    LEFT JOIN acs_county acs
        ON r.GEOID = acs.GEOID
    """
)


    if df.empty:
        st.warning("Map disabled: no counties with geo + data.")
        return

    # Basic cleaning
    df["Total_Pop"] = df["Total_Pop"].astype("float64")
    df["Median_Income"] = df["Median_Income"].astype("float64")
    df["Poverty_Rate"] = df["Poverty_Rate"].astype("float64")

    # ------------------------------------------------------------------
    # Mode selector
    # ------------------------------------------------------------------
    st.subheader("PPP & ACS Map")

    mode = st.radio(
        "Color mode",
        [
            "PPP intensity (risk score)",
            "Population",
            "Median income",
            "Poverty rate",
        ],
        index=0,
        horizontal=True,
    )

    # ------------------------------------------------------------------
    # Color computation per mode
    # ------------------------------------------------------------------
    def _color_scale(series: pd.Series, invert: bool = False):
        """Simple 3-stop scale: low, mid, high → RGB."""
        s = series.fillna(0.0)
        if s.empty:
            return [[180, 180, 180]] * len(s)

        q10 = float(s.quantile(0.10))
        q50 = float(s.quantile(0.50))
        q90 = float(s.quantile(0.90))

        colors = []
        for v in s:
            if v <= q10:
                c = (0, 120, 255)    # low
            elif v <= q50:
                c = (80, 200, 120)   # mid
            else:
                c = (230, 80, 60)    # high

            if invert:
                c = (c[2], c[1], c[0])

            colors.append(list(c))
        return colors

    if mode == "PPP intensity (risk score)":
        # Use tier-like intensity based on risk_score; fallback to loan_total
        series = df["risk_score"]
        if series.max() == 0:
            series = df["loan_total"]
        df["color"] = _color_scale(series, invert=False)

    elif mode == "Population":
        df["color"] = _color_scale(df["Total_Pop"], invert=False)

    elif mode == "Median income":
        df["color"] = _color_scale(df["Median_Income"], invert=False)

    else:  # Poverty rate
        df["color"] = _color_scale(df["Poverty_Rate"], invert=False)

    # Dot size driven by loan_count (capped for readability)
    df["size"] = (df["loan_count"].astype("float64") ** 0.7).clip(lower=5, upper=90)

    # ------------------------------------------------------------------
    # Deck.gl layer
    # ------------------------------------------------------------------
    layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        pickable=True,
        opacity=0.35,
        stroked=False,
        radius_scale=500,
        radius_min_pixels=3,
        radius_max_pixels=60,
        get_position=["INTPTLONG", "INTPTLAT"],
        get_radius="size",
        get_fill_color="color",
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=39.5,
        longitude=-98.5,
        zoom=3.6,
        pitch=25,
    )

    tooltip = {
        "html": """
        <b>{NAME}, {STUSPS}</b><br/>
        Loans: {loan_count}<br/>
        Total: {loan_total}<br/>
        Risk score: {risk_score}<br/>
        Population: {Total_Pop}<br/>
        Median income: {Median_Income}<br/>
        Poverty rate: {Poverty_Rate}%
        """,
        "style": {"font-size": "13px"},
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/dark-v11",
    )

    st.pydeck_chart(deck)
    st.caption("Circle size = loan_count; color = selected PPP/ACS measure.")
