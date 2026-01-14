import duckdb

DB = "data/db/v_finder.duckdb"

def create_county_scores():
    con = duckdb.connect(DB)

    print("Building county_scores...")

    con.execute("DROP TABLE IF EXISTS county_scores")

    con.execute("""
        CREATE TABLE county_scores AS
        SELECT
            GEOID,
            STUSPS,
            NAME,
            Total_Pop,
            Poverty_Rate,
            Unemployment_Rate,
            ppp_loan_count,
            ppp_current_total,
            ppp_per_capita,
            -- basic fraud risk scoring model
            (
                (Poverty_Rate * 0.3) +
                (Unemployment_Rate * 0.3) +
                (ppp_per_capita * 0.4)
            ) AS fraud_score
        FROM county_agg
    """)

    print("county_scores built successfully.")


if __name__ == "__main__":
    create_county_scores()
