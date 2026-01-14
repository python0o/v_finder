from __future__ import annotations
import re

_SUFFIXES = [
    r"\s+COUNTY$",
    r"\s+PARISH$",
    r"\s+BOROUGH$",
    r"\s+CENSUS\s+AREA$",
    r"\s+MUNICIPALITY$",
    r"\s+CITY\s+AND\s+BOROUGH$",
    r"\s+DISTRICT$",
]

def norm_county_name(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    for pat in _SUFFIXES:
        s = re.sub(pat, "", s)
    s = s.replace(".", "")
    return s.strip()