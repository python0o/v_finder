from __future__ import annotations
import io, os, zipfile
from pathlib import Path
import pandas as pd
import geopandas as gpd
import requests
import folium
from shapely.geometry import Point
from streamlit_folium import st_folium
import streamlit as st

from config import TIGER_COUNTY_ZIP_URL, TIGER_COUNTY_CACHE_ZIP, TIGER_COUNTY_CACHE_DIR

def _ensure_tiger_counties() -> Path:
    TIGER_COUNTY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not TIGER_COUNTY_CACHE_ZIP.exists():
        r = requests.get(TIGER_COUNTY_ZIP_URL, timeout=180)
        r.raise_for_status()
        TIGER_COUNTY_CACHE_ZIP.write_bytes(r.content)
    # extract if needed
    shp = TIGER_COUNTY_CACHE_DIR / "tl_2024_us_county.shp"
    if not shp.exists():
        with zipfile.ZipFile(TIGER_COUNTY_CACHE_ZIP, "r") as z:
            z.extractall(TIGER_COUNTY_CACHE_DIR)
    return shp

@st.cache_data(show_spinner=False)
def load_county_geoms() -> gpd.GeoDataFrame:
    shp = _ensure_tiger_counties()
    gdf = gpd.read_file(shp)
    # GEOID as 5-char string
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(5)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4269", allow_override=True)
    # Reproject to WGS84
    gdf = gdf.to_crs("EPSG:4326")
    # Simplify for web map speed
    gdf["geometry"] = gdf.geometry.simplify(0.01, preserve_topology=True)
    return gdf[["GEOID","NAME","STATEFP","geometry"]].copy()

def render_county_map(fraud_df: pd.DataFrame | None, selected_geoid_key: str = "selected_geoid") -> None:
    gdf = load_county_geoms()

    merged = gdf.merge(
        fraud_df[["geoid","fraud_score"]] if fraud_df is not None and not fraud_df.empty else pd.DataFrame(columns=["geoid","fraud_score"]),
        left_on="GEOID",
        right_on="geoid",
        how="left"
    )
    merged["fraud_score"] = pd.to_numeric(merged["fraud_score"], errors="coerce").fillna(0)

    m = folium.Map(location=[39.5, -98.35], zoom_start=4, tiles="CartoDB positron", prefer_canvas=True)

    folium.Choropleth(
        geo_data=merged,
        data=merged,
        columns=["GEOID","fraud_score"],
        key_on="feature.properties.GEOID",
        fill_color="Reds",
        fill_opacity=0.65,
        line_opacity=0.10,
        name="Fraud Score"
    ).add_to(m)

    folium.LayerControl().add_to(m)

    st.caption("Click a county to select it. Selection is used across tabs.")
    state = st_folium(m, height=650, width=1100, returned_objects=[])

    if state and state.get("last_clicked"):
        lat = state["last_clicked"]["lat"]
        lng = state["last_clicked"]["lng"]
        pt = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
        hit = gpd.sjoin(pt, gdf[["GEOID","geometry"]], how="left", predicate="within")
        if not hit.empty and pd.notna(hit.iloc[0].get("GEOID")):
            st.session_state[selected_geoid_key] = str(hit.iloc[0]["GEOID"])
