WITH

member_address_with_geopoint as (

  select
    patientId,
    addressId,
    geopoint
  from {{ ref('member_address_with_geopoint') }}
),

us_blockgroups_national as (

  select
    geo_id,
    blockgroup_geom
  from {{ source('geo_census_blockgroups', 'us_blockgroups_national') }}
),

blockgroup_2018_5yr as (

  select *
  from {{ source('census_bureau_acs', 'blockgroup_2018_5yr') }}
),

area_deprivation_index as (

  select *
  from {{ source('broadstreet_adi', 'area_deprivation_index_by_census_block_group') }}
),

area_deprivation_index_latest as (

   {{ select_latest('area_deprivation_index', 'year') }}
),

final as (

  SELECT
    mawg.patientId,
    mawg.addressId,
    acs.geo_id as geoId,

    -- Income / Rent
    acs.median_income as acs_incomeHouseholdMedianValue,
    acs.income_per_capita as acs_incomePerPersonValue,
    acs.percent_income_spent_on_rent as acs_incomeOnRentPercent,
    SAFE_DIVIDE(acs.rent_over_50_percent, acs.occupied_housing_units) as acs_rentOver50PctIncomePercent,
    SAFE_DIVIDE(acs.rent_under_10_percent, acs.occupied_housing_units) as acs_rentUnder10PctIncomePercent,
    acs.median_rent as acs_rentMedianValue,
    -- Housing / Households
    SAFE_DIVIDE(acs.family_households, acs.households) * 100 as acs_familyHouseholdPercent,
    SAFE_DIVIDE(acs.occupied_housing_units, acs.housing_units) * 100 as acs_housingUnitsOccupiedPercent,
    SAFE_DIVIDE(acs.housing_units_renter_occupied, acs.occupied_housing_units) * 100 as acs_housingUnitsRenterOccupiedPercent,
    SAFE_DIVIDE(acs.owner_occupied_housing_units, acs.occupied_housing_units) * 100 as acs_housingUnitsOwnerOccupiedPercent,
    -- Employment
    SAFE_DIVIDE(acs.unemployed_pop, acs.civilian_labor_force) * 100 as acs_unemployedPercent,
    -- Race/ethnicity
    SAFE_DIVIDE(acs.white_pop, acs.total_pop) * 100 as acs_raceWhiteAlonePercent,
    SAFE_DIVIDE(acs.black_pop, acs.total_pop) * 100 as acs_raceBlackAlonePercent,
    SAFE_DIVIDE(acs.asian_pop, acs.total_pop) * 100 as acs_raceAsianAlonePercent,
    SAFE_DIVIDE(acs.amerindian_pop, acs.total_pop) * 100 as acs_raceAmerIndianAlonePercent,
    SAFE_DIVIDE(acs.other_race_pop, acs.total_pop) * 100 as acs_raceOtherRaceAlonePercent,
    SAFE_DIVIDE(acs.two_or_more_races_pop, acs.total_pop) * 100 as acs_raceMultiRacialPercent,
    SAFE_DIVIDE(acs.hispanic_pop, acs.total_pop) * 100 as acs_ethnHispanicPercent,
    SAFE_DIVIDE(acs.not_hispanic_pop, acs.total_pop) * 100 as acs_ethnNotHispanicPercent,
    -- Gender
    SAFE_DIVIDE(acs.female_pop, acs.total_pop) * 100 as acs_genderFemalePercent,
    SAFE_DIVIDE(acs.male_pop, acs.total_pop) * 100 as acs_genderMalePercent,
    -- Age
    acs.median_age as acs_ageMedianValue,
    -- Education
    SAFE_DIVIDE(acs.high_school_diploma, acs.pop_25_years_over) * 100 as acs_eduHighSchoolDiplomaOnlyPercent,
    SAFE_DIVIDE(acs.associates_degree, acs.pop_25_years_over) * 100 as acs_eduAssociatesDegreeOnlyPercent,
    SAFE_DIVIDE(acs.bachelors_degree, acs.pop_25_years_over) * 100 as acs_eduBachelorsDegreeOnlyPercent,
    SAFE_DIVIDE(acs.masters_degree, acs.pop_25_years_over) * 100 as acs_eduMastersDegreeOnlyPercent,
    -- Work / Commute
    SAFE_DIVIDE(acs.commuters_by_public_transportation, acs.commuters_16_over) * 100 as acs_commutePublicTransitPercent,
    SAFE_DIVIDE(acs.commute_less_10_mins, acs.commuters_16_over) * 100 as acs_commuteUpTo10MinsPercent,
    SAFE_DIVIDE(acs.commute_60_more_mins, acs.commuters_16_over) * 100 as acs_commuteMoreThan60MinsPercent,
    SAFE_DIVIDE(acs.aggregate_travel_time_to_work, acs.commuters_16_over) as acs_averageCommuteTimePerWorker,
    -- ACS-Derived Measures
    adi.area_deprivation_index_percent as adi_areaDeprivationIndexPercent,
    -- Metadata
    2018 as acs_yearLastUpdated,
    adi.year as adi_yearLastUpdated
  FROM member_address_with_geopoint mawg
  JOIN us_blockgroups_national geo
    ON ST_CONTAINS(geo.blockgroup_geom, mawg.geopoint)
  LEFT JOIN blockgroup_2018_5yr acs
    ON geo.geo_id = acs.geo_id
  LEFT JOIN area_deprivation_index_latest adi
    ON geo.geo_id = SUBSTR(adi.geo_id, 8, 12)
)

SELECT * FROM final
