from __future__ import annotations

import duckdb
import pandas as pd

"""
Normalization Report + Outlier Diagnostics
------------------------------------------
Adds:
    • BEFORE → AFTER county normalization
    • PPP + ACS metrics attached
    • Outlier scoring:
        - PPP loan_total z-score
        - loan_per_capita z-score
        - poverty rate deviation
    • Outlier flag matrix
"""

def _exists(con, name):
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0]
        == 1
    )


def build_normalization_report(con, sample_limit=500):
    for tbl in ("ppp_clean", "county_ref"):
        if not _exists(con, tbl):
            return (
                pd.DataFrame({"error": [f"Missing table: {tbl}"]}),
                pd.DataFrame(),
                pd.DataFrame(),
            )

    # Core BEFORE → AFTER sample
    base_df = con.execute(
        """
        SELECT DISTINCT
            p.borrowerstate_u AS borrower_state,
            p.ProjectCountyName AS county_raw,
            p.county_norm AS county_norm,
            r.NAME AS matched_name,
            r.NAME_NORM AS matched_norm,
            r.GEOID AS geoid
        FROM ppp_clean p
        LEFT JOIN county_ref r
          ON p.borrowerstate_u = r.STUSPS
         AND p.county_norm = r.NAME_NORM
        """
    ).fetchdf()

    base_df["matched"] = base_df["geoid"].notna()

    # Join PPP + ACS if present
    if _exists(con, "county_agg"):
        agg = con.execute(
            "SELECT GEOID, loan_count, loan_total FROM county_agg"
        ).fetchdf()
        base_df = base_df.merge(agg, on="GEOID", how="left")

    if _exists(con, "acs_county"):
        acs = con.execute(
            "SELECT GEOID, Total_Pop, Median_Income, Poverty_Rate FROM acs_county"
        ).fetchdf()
        base_df = base_df.merge(acs, on="GEOID", how="left")

    # Derived PPP/ACS metrics
    base_df["loan_total_per_capita"] = (
        base_df["loan_total"] / base_df["Total_Pop"]
    ).replace([float("inf")], pd.NA)
    base_df["loans_per_1k"] = (
        base_df["loan_count"] / base_df["Total_Pop"] * 1000.0
    ).replace([float("inf")], pd.NA)

    # Summary
    total = len(base_df)
    matched = int(base_df["matched"].sum())
    summary_df = pd.DataFrame(
        {
            "Total county strings": [total],
            "Matched county_ref": [matched],
            "Unmatched": [total - matched],
            "Match %": [round(matched / total * 100.0, 2) if total else 0],
        }
    )

    # --- OUTLIER DIAGNOSTICS -----------------------------------------
    df2 = base_df.copy()
    numeric_cols = ["loan_total", "loan_total_per_capita", "loans_per_1k", "Poverty_Rate"]

    for col in numeric_cols:
        if col in df2:
            s = pd.to_numeric(df2[col], errors="coerce")
            df2[col + "_z"] = (s - s.mean()) / (s.std() if s.std() else 1)

    # Outlier rules: any |z| >= 2.5
    df2["outlier_flag"] = (
        (df2.filter(regex="_z$").abs() >= 2.5).any(axis=1)
    ).astype(int)

    outliers_df = df2[df2["outlier_flag"] == 1].copy()
    outliers_df = outliers_df[
        [
            "borrower_state",
            "county_raw",
            "county_norm",
            "matched",
            "matched_name",
            "geoid",
            "loan_total",
            "loan_total_z",
            "loan_total_per_capita",
            "loan_total_per_capita_z",
            "loans_per_1k",
            "loans_per_1k_z",
            "Poverty_Rate",
            "Poverty_Rate_z",
        ]
    ]

    # Sampling (match your UI)
    unmatched_df = base_df[~base_df["matched"]]
    matched_df = base_df[base_df["matched"]]

    sample_unmatched = unmatched_df.head(sample_limit // 2)
    sample_matched = matched_df.head(sample_limit - len(sample_unmatched))
    sample_df = pd.concat([sample_unmatched, sample_matched], ignore_index=True)

    return summary_df, sample_df, outliers_df
