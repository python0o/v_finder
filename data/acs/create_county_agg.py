import duckdb

DB = "data/db/v_finder.duckdb"

def create_county_agg():
    con = duckdb.connect(DB)
    print("Building county_agg using PPP + ACS + county_ref...")

    con.execute("DROP TABLE IF EXISTS county_agg")

    con.execute("""
        CREATE TABLE county_agg AS
        WITH p AS (
            SELECT
                county_norm,
                COUNT(*)::BIGINT AS ppp_loan_count,
                SUM(CAST(currentapprovalamount AS DOUBLE)) AS ppp_current_total
            FROM ppp_clean
            GROUP BY county_norm
        ),
        a AS (
            SELECT
                GEOID,
                CAST(Total_Pop AS BIGINT) AS Total_Pop,
                CAST(Median_Income AS DOUBLE) AS Median_Income,
                CAST(Poverty_Rate AS DOUBLE) AS Poverty_Rate,
                CAST(Unemployment_Rate AS DOUBLE) AS Unemployment_Rate
            FROM acs_county
        )
        SELECT
            r.GEOID,
            r.STUSPS,
            r.NAME AS NAME,
            a.Total_Pop,
            a.Median_Income,
            a.Poverty_Rate,
            a.Unemployment_Rate,
            COALESCE(p.ppp_loan_count,0) AS ppp_loan_count,
            COALESCE(p.ppp_current_total,0.0) AS ppp_current_total,
            CASE
                WHEN a.Total_Pop > 0 THEN COALESCE(p.ppp_current_total,0)/a.Total_Pop
                ELSE 0
            END AS ppp_per_capita
        FROM county_ref r
        LEFT JOIN a ON a.GEOID = r.GEOID
        LEFT JOIN p ON LOWER(p.county_norm) = LOWER(r.NAME)
    """)

    print("county_agg built successfully.")


if __name__ == "__main__":
    create_county_agg()
