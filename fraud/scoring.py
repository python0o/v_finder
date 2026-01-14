import pandas as pd
import numpy as np

def _zscore(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if sd == 0 or np.isnan(sd):
        return x * 0.0
    return (x - mu) / sd

def compute_fraud_table(con) -> pd.DataFrame:
    # county_stats may have null population/minority_share
    df = con.execute("""
        SELECT
            GEOID,
            NAME,
            population,
            minority_share,
            loans,
            loan_total,
            forgiven_total
        FROM county_stats
        ORDER BY loan_total DESC
        LIMIT 5000
    """).df()

    # per-capita
    pop = pd.to_numeric(df["population"], errors="coerce")
    loan_total = pd.to_numeric(df["loan_total"], errors="coerce").fillna(0.0)
    df["dollars_per_capita"] = np.where((pop > 0), loan_total / pop, np.nan)

    # Base risk = zscore(percap) + zscore(loan_total) (weights can be surfaced later)
    base = 0.6 * _zscore(df["dollars_per_capita"]) + 0.4 * _zscore(df["loan_total"])

    # Ethnicity-weighting: amplify where minority_share is higher (neutral when null)
    ms = pd.to_numeric(df["minority_share"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    beta = 0.35  # conservative multiplier
    df["fraud_score"] = base * (1.0 + beta * ms)

    # Normalize to 0..100 for dashboard readability
    lo, hi = df["fraud_score"].min(skipna=True), df["fraud_score"].max(skipna=True)
    if pd.notna(lo) and pd.notna(hi) and hi != lo:
        df["fraud_score_0_100"] = (df["fraud_score"] - lo) / (hi - lo) * 100.0
    else:
        df["fraud_score_0_100"] = 0.0

    df = df.sort_values("fraud_score_0_100", ascending=False)
    return df
