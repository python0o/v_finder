from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from census import Census

ACS_FIELDS = [
    "NAME",
    "B01003_001E",  # total pop
    "B02001_002E", "B02001_003E", "B02001_004E", "B02001_005E", "B02001_006E", "B02001_007E", "B02001_008E",
    "B03003_003E",  # hispanic/latino
    "B19013_001E",  # median income
    "B25077_001E",  # median home value
    "B23025_005E",  # unemployed
    "B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"
]

def fetch_acs_county_all_states(census_key: str, year: int = 2022) -> pd.DataFrame:
    c = Census(census_key)
    rows = []
    for state in range(1, 57):  # includes territories; we will filter later
        sf = str(state).zfill(2)
        try:
            data = c.acs5.state_county(ACS_FIELDS, sf, "*", year=year)
            if data:
                rows.extend(data)
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(columns={
        "B01003_001E": "total_population",
        "B02001_002E": "white",
        "B02001_003E": "black",
        "B02001_004E": "native_american",
        "B02001_005E": "asian",
        "B02001_006E": "pacific_islander",
        "B02001_007E": "other_race",
        "B02001_008E": "two_or_more",
        "B03003_003E": "hispanic_latino",
        "B19013_001E": "median_income",
        "B25077_001E": "median_home_value",
        "B23025_005E": "unemployed",
        "B15003_022E": "bachelor",
        "B15003_023E": "master",
        "B15003_024E": "professional",
        "B15003_025E": "doctorate",
    })

    for col in ["total_population","white","black","native_american","asian","pacific_islander","other_race","two_or_more","hispanic_latino","median_income","median_home_value","unemployed","bachelor","master","professional","doctorate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["college_educated"] = df[["bachelor","master","professional","doctorate"]].fillna(0).sum(axis=1)
    df["geoid"] = df["state"].astype(str).str.zfill(2) + df["county"].astype(str).str.zfill(3)
    df["acs_year"] = year
    df["last_refresh_utc"] = datetime.now(timezone.utc).replace(tzinfo=None)

    keep = ["geoid","NAME","total_population","white","black","asian","native_american","pacific_islander","other_race","two_or_more","hispanic_latino","median_income","median_home_value","unemployed","college_educated","acs_year","last_refresh_utc"]
    out = df[keep].copy().rename(columns={"NAME":"name"})
    return out
