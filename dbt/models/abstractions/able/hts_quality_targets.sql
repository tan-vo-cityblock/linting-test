{{
  config(
    materialized='table'
  ) 
}}

SELECT * FROM {{ source ('hub_team_success','hts_quality_targets') }}