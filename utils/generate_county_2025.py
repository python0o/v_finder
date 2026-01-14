from __future__ import annotations

import argparse
import pandas as pd
from pathlib import Path
from utils.normalize import norm_county_name

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Output CSV path")
    p.add_argument("--gazetteer-txt", default=None, help="Path to Census Gazetteer .txt (tab-delimited)")
    return p.parse_args()

def main():
    args = parse_args()
    out_path = Path(args.out)

    if not args.gazetteer_txt:
        raise SystemExit("Provide --gazetteer-txt pointing to the Gazetteer .txt you downloaded.")

    txt_path = Path(args.gazetteer_txt)
    if not txt_path.exists():
        raise FileNotFoundError(str(txt_path))

    # Gazetteer .txt is typically tab-delimited with header row
    df = pd.read_csv(txt_path, sep="\t", dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]

    # Common column names in Gazetteer:
    # GEOID, NAME, USPS (state abbrev) or sometimes "USPS"
    # We'll normalize a bit.
    col_geoid = "GEOID"
    col_name = "NAME" if "NAME" in df.columns else None
    col_usps = "USPS" if "USPS" in df.columns else ("STUSPS" if "STUSPS" in df.columns else None)

    missing = [c for c in [col_geoid, col_name, col_usps] if c is None or c not in df.columns]
    if missing:
        raise RuntimeError(f"Gazetteer missing columns after normalization: {set(['GEOID','NAME','USPS']) - set(df.columns)}")

    out = pd.DataFrame({
        "GEOID": df[col_geoid].astype(str).str.zfill(5),
        "STUSPS": df[col_usps].astype(str).str.strip().str.upper(),
        "NAME": df[col_name].astype(str).str.strip(),
    })
    out["STATEFP"] = out["GEOID"].str[:2]
    out["NAME_NORM"] = out["NAME"].map(norm_county_name)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out):,} counties -> {out_path}")

if __name__ == "__main__":
    main()
