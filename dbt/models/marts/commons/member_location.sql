WITH geo_census_tract AS (

        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_new_york`
        UNION ALL
        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_north_carolina`
        UNION ALL
        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_connecticut`
        UNION ALL
        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_massachusetts`
        UNION ALL
        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_delaware`
        UNION ALL
        SELECT * FROM `bigquery-public-data.geo_census_tracts.census_tracts_new_jersey`
        ),

location_cte AS (
        SELECT 
        patientId,
        street1,
        street2,
        zip,
        state,
        city,
        addressDescription,
        lat,
        long
        FROM {{ref('member_info')}}
        ),
        
matched_addresses AS (
    SELECT 
    location_cte.*,
    county_fips_code,
    tract_ce,
    geo_id as bq_census_geo_id,
    tract_geom
    FROM location_cte CROSS JOIN geo_census_tract gct
    WHERE ST_CONTAINS(
                    gct.tract_geom,
                    ST_GeogPoint(location_cte.long, location_cte.lat)
                )
    )
            
SELECT * FROM matched_addresses 
    UNION ALL 
SELECT location_cte.*,
        NULL as county_fips_code,
        NULL as tract_ce,
        NULL as bq_census_geo_id,
        NULL as tract_geom
FROM location_cte
WHERE patientId NOT IN (SELECT patientId FROM matched_addresses)