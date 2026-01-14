import os
import geopandas as gpd
import streamlit as st

@st.cache_resource(show_spinner=False)
def load_counties(path: str) -> gpd.GeoDataFrame:
    path = os.path.abspath(path)
    if not os.path.exists(path):
        # Return empty gdf with expected columns
        return gpd.GeoDataFrame({"GEOID": [], "NAME": []}, geometry=[], crs="EPSG:4326")

    gdf = gpd.read_file(path)

    # Normalize GEOID column
    if "GEOID" not in gdf.columns:
        for alt in ("GEOID10", "GEOID20", "geoid", "FIPS", "COUNTYFP", "COUNTYFP20"):
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "GEOID"})
                break

    # If separate STATEFP/COUNTYFP exist, build GEOID
    if "GEOID" not in gdf.columns and "STATEFP" in gdf.columns and "COUNTYFP" in gdf.columns:
        gdf["GEOID"] = gdf["STATEFP"].astype(str).str.zfill(2) + gdf["COUNTYFP"].astype(str).str.zfill(3)

    # NAME
    if "NAME" not in gdf.columns:
        for alt in ("NAMELSAD", "NAME10", "NAME20", "County", "county"):
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "NAME"})
                break
        if "NAME" not in gdf.columns:
            gdf["NAME"] = ""

    gdf["GEOID"] = gdf["GEOID"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(5).str[-5:]

    # Ensure WGS84
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        gdf = gdf.to_crs("EPSG:4326")

    # Build spatial index once
    _ = gdf.sindex
    return gdf
