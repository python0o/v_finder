import requests
import pandas as pd
import os

# =====================================================
# CONFIG
# =====================================================
YEAR = 2023
API_KEY = "a662f4c242a3f8b4b8d8735cf81139fccff5ed3e"

OUTPUT_PATH = r"C:\scripts\v_finder\data\reference\acs_county_master.csv"
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# =====================================================
# ACS API HELPERS
# =====================================================
def fetch_acs(table, variables):
    base = f"https://api.census.gov/data/{YEAR}/acs/acs5"
    params = {
        "get": ",".join(variables),
        "for": "county:*",
        "key": API_KEY
    }
    r = requests.get(base, params=params)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

# =====================================================
# VARIABLE SETS
# =====================================================

# Population (B01001)
POP_VARS = [
    "NAME",
    "B01001_001E",   # Total Pop
]

# Median Income (B19013)
INC_VARS = [
    "B19013_001E",  # Median household income
]

# Poverty (B17001)
POV_VARS = [
    "B17001_001E",  # universe
    "B17001_002E",  # below poverty
]

# Employment (B23025)
EMP_VARS = [
    "B23025_003E",  # labor force
    "B23025_005E",  # unemployed
]

# =====================================================
# FETCH
# =====================================================
print("\n=== Fetching ACS 2023 5-year data from Census API ===")

print("• Population (B01001)…")
pop = fetch_acs("B01001", POP_VARS)

print("• Income (B19013)…")
inc = fetch_acs("B19013", INC_VARS)

print("• Poverty (B17001)…")
pov = fetch_acs("B17001", POV_VARS)

print("• Employment (B23025)…")
emp = fetch_acs("B23025", EMP_VARS)

# =====================================================
# MERGE
# =====================================================

df = pop.copy()
df["GEOID"] = df["state"] + df["county"]

df = df.merge(inc, on=["state", "county"])
df = df.merge(pov, on=["state", "county"])
df = df.merge(emp, on=["state", "county"])

# =====================================================
# CLEAN + RENAME
# =====================================================
df = df.rename(columns={
    "NAME": "NAME",
    "B01001_001E": "Total_Pop",
    "B19013_001E": "Median_Income",
    "B17001_001E": "Poverty_Universe",
    "B17001_002E": "Poverty_Count",
    "B23025_003E": "Labor_Force",
    "B23025_005E": "Unemployed",
})

# convert numerics
for col in [
    "Total_Pop", "Median_Income",
    "Poverty_Universe", "Poverty_Count",
    "Labor_Force", "Unemployed"
]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["Poverty_Rate"] = df["Poverty_Count"] / df["Poverty_Universe"]
df["Unemployment_Rate"] = df["Unemployed"] / df["Labor_Force"]

df["state"] = df["state"].astype(str)
df["county"] = df["county"].astype(str)

# =====================================================
# SAVE FILE
# =====================================================
df.to_csv(OUTPUT_PATH, index=False)

print("\n=== DONE — ACS MASTER CREATED ===")
print(f"Output: {OUTPUT_PATH}")
print(f"Rows: {len(df):,}")
print("\nPreview:")
print(df.head())
