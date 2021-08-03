WITH

member_address_with_geopoint as (
    SELECT * FROM
    {{ ref('member_address_with_geopoint') }}
),
air_quality_last_updated as (
    SELECT max(date_local) as maxDate
    FROM {{ source('epa_historical_air_quality', 'pm25_frm_daily_summary') }}
    WHERE sample_duration='24 HOUR'
),
air_data as (
    SELECT CONCAT(state_code, county_code, site_num, parameter_code, poc) as id, *, 
    (SELECT maxDate FROM air_quality_last_updated) as dateLastUpdated
    FROM {{ source('epa_historical_air_quality', 'pm25_frm_daily_summary') }} 
    WHERE date_local <= (SELECT maxDate FROM air_quality_last_updated)
    AND date_local > (SELECT DATE_SUB(maxDate, INTERVAL 3 MONTH) FROM air_quality_last_updated)
    AND sample_duration='24 HOUR'
),
nearest_stations as (
    SELECT AS VALUE ARRAY_AGG(STRUCT<id_a STRING, id_b STRING>(a.id, b.id) 
        ORDER BY ST_DISTANCE(a.point, b.point) LIMIT 1)[OFFSET(0)] 
    FROM (SELECT patientId as id, geopoint as point FROM member_address_with_geopoint) a
    CROSS JOIN (SELECT id, ST_GEOGPOINT(longitude, latitude) point FROM air_data) b 
    GROUP BY a.id
    ORDER BY a.id ASC
),
merged as (
    SELECT DISTINCT --there are dupes
    ma.patientId, ma.addressId, aq.* except(id)
    FROM member_address_with_geopoint ma
    LEFT JOIN nearest_stations ne
    ON ma.patientId = ne.id_a
    LEFT JOIN air_data aq
    ON ne.id_b = aq.id
)

SELECT
patientId, addressId,
state_code as stateCode, county_code as countyCode, site_num as siteNum, parameter_code as parameterCode,
avg(aqi) as epa_meanAQI,
avg(arithmetic_mean) as epa_meanPM25,
CASE
    WHEN avg(aqi) < 51 THEN "Good (green)"
    WHEN avg(aqi) <101 THEN "Moderate (yellow)"
    WHEN avg(aqi) <151 THEN "Unhealthy for sensitive groups (orange)"
    WHEN avg(aqi) <201 THEN "Unhealthy (red)"
    WHEN avg(aqi) <301 THEN "Very unhealthy (purple)"
    WHEN avg(aqi) <501 THEN "Hazardous (maroon)"
    ELSE "unexpected data"
  END AS epa_airQualityRating, 
  --source: https://www.airnow.gov/aqi/aqi-basics/
  CASE
    WHEN avg(arithmetic_mean) < 13 THEN "Good (green)"
    WHEN avg(arithmetic_mean) < 36 THEN "Moderate (yellow)"
    WHEN avg(arithmetic_mean) < 56 THEN "Unhealthy for sensitive groups (orange)"
    WHEN avg(arithmetic_mean) <151 THEN "Unhealthy (red)"
    WHEN avg(arithmetic_mean) <251 THEN "Very unhealthy (purple)"
    WHEN avg(arithmetic_mean) <501 THEN "Hazardous (maroon)"
    ELSE "unexpected data"
  END AS epa_pm25Rating,
  -- source: https://en.wikipedia.org/wiki/Air_quality_index#Computing_the_AQI
dateLastUpdated as epa_dateLastMeasurement,
EXTRACT(YEAR from dateLastUpdated) as epa_yearLastUpdated
FROM merged
GROUP BY patientId, addressId, stateCode, countyCode, siteNum, parameterCode, epa_dateLastMeasurement, epa_yearLastUpdated
