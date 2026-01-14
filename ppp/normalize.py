import re
import pandas as pd
from typing import Dict, Optional, Tuple

def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        return ""
    return s

def safe_float(x, default: float = 0.0) -> float:
    if x is None:
        return default
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return default
    s = safe_str(x).strip()
    if s == "" or s.upper() in {"NA", "N/A", "NULL", "NONE"}:
        return default
    s = s.replace(",", "").replace("$", "")
    try:
        return float(s)
    except Exception:
        return default

def norm_county_name(name: str) -> str:
    s = safe_str(name).strip().lower()
    s = re.sub(r"\bcounty\b", "", s)
    s = re.sub(r"\bparish\b", "", s)
    s = re.sub(r"\bcensus area\b", "", s)
    s = re.sub(r"\bborough\b", "", s)
    s = s.replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

STATE_TO_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10","DC":"11","FL":"12",
    "GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20","KY":"21",
    "LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30",
    "NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","OH":"39",
    "OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46","TN":"47","TX":"48","UT":"49",
    "VT":"50","VA":"51","WA":"53","WV":"54","WI":"55","WY":"56","PR":"72","AS":"60","GU":"66",
    "MP":"69","VI":"78"
}

def build_geoid(state_abbr: Optional[pd.Series], county_fips: Optional[pd.Series]) -> pd.Series:
    if state_abbr is None or county_fips is None:
        return pd.Series([None] * 0)
    s = state_abbr.astype(str).str.upper().str.strip()
    sf = s.map(STATE_TO_FIPS).fillna("")
    cf = county_fips.astype(str).str.replace(r"\D", "", regex=True).str.zfill(3).str[-3:]
    geoid = (sf + cf)
    geoid = geoid.where(geoid.str.len() == 5, None)
    return geoid

# ---------------------------------------------------------
# Column normalization for PPP ingest
# ---------------------------------------------------------
from typing import Dict, Tuple

def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Normalize PPP CSV column names into a canonical, safe set.

    Rules:
      - Lowercase
      - Strip leading/trailing whitespace
      - Remove all non-alphanumeric characters
      - e.g. "Loan Number" -> "loannumber"
           "BorrowerState" -> "borrowerstate"
           "Project County Name" -> "projectcountyname"

    Returns:
      (normalized_df, mapping) where:
        normalized_df.columns are the new names
        mapping is {new_name: original_name}
    """
    original_cols = list(df.columns)
    new_cols: list[str] = []
    mapping: Dict[str, str] = {}
    used: set[str] = set()

    for col in original_cols:
        # Safe string
        s = safe_str(col)
        s = s.strip().lower()
        # Keep only [0-9a-z]
        s = re.sub(r"[^0-9a-z]+", "", s)
        if not s:
            s = "col"

        base = s
        candidate = base
        i = 2
        while candidate in used:
            candidate = f"{base}_{i}"
            i += 1

        new_cols.append(candidate)
        mapping[candidate] = col
        used.add(candidate)

    df_norm = df.copy()
    df_norm.columns = new_cols
    return df_norm, mapping
