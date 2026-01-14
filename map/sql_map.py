# map/sql_map.py
#
# Centralized SQL for Deck.gl map rendering (PPP + ACS + County Reference)

MAP_FRAME_QUERY = """
WITH ppp AS (
    SELECT
        county_norm,
        borrowerstate_u,
        COUNT(*)::INT AS loan_count
    FROM ppp_clean
    WHERE county_norm IS NOT NULL
      AND borrowerstate_u IS NOT NULL
    GROUP BY 1,2
),

county AS (
    SELECT
        GEOID,
        STATEFP,
        COUNTYFP,
        STUSPS,
        NAME,
        NAME_NORM,
        INTPTLAT AS lat,
        INTPTLONG AS lon,
        ALAND
    FROM county_ref
),

acs AS (
    SELECT
        GEOID,
        Total_Pop,
        Median_Income,
        Poverty_Rate,
        Unemployment_Rate
    FROM acs_county
)

SELECT
    -- Identity
    c.GEOID,
    c.STUSPS,
    c.NAME,
    c.NAME_NORM,

    -- Centroids
    c.lat,
    c.lon,

    -- ACS
    a.Total_Pop,
    a.Median_Income,
    a.Poverty_Rate,
    a.Unemployment_Rate,

    -- PPP
    p.loan_count,

    -- Derived fields
    CASE WHEN a.Total_Pop > 0
         THEN p.loan_count / a.Total_Pop::DOUBLE
         ELSE NULL END AS loans_per_capita

FROM county AS c
LEFT JOIN acs AS a
    ON a.GEOID = c.GEOID
LEFT JOIN ppp AS p
    ON upper(trim(c.NAME_NORM)) = upper(trim(p.county_norm))
   AND upper(trim(c.STUSPS)) = upper(trim(p.borrowerstate_u))
WHERE c.lat IS NOT NULL
  AND c.lon IS NOT NULL
ORDER BY p.loan_count DESC NULLS LAST;
"""
