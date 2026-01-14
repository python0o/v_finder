def ensure_ppp_tables(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS ppp_clean(
        GEOID VARCHAR,
        State VARCHAR,
        City VARCHAR,
        Business_Name VARCHAR,
        Borrower_Address VARCHAR,
        ZIP VARCHAR,
        Date_Approved DATE,
        Loan_Amount DOUBLE,
        Forgiven_Amount DOUBLE,
        Latitude DOUBLE,
        Longitude DOUBLE,
        source_file VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS ppp_agg_by_geoid(
        GEOID VARCHAR PRIMARY KEY,
        loans BIGINT,
        loan_total DOUBLE,
        forgiven_total DOUBLE
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS county_stats(
        GEOID VARCHAR PRIMARY KEY,
        NAME VARCHAR,
        population BIGINT,
        minority_share DOUBLE,
        loans BIGINT,
        loan_total DOUBLE,
        forgiven_total DOUBLE
    );
    """)
