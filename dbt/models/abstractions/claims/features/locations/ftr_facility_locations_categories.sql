
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      claimId,
      typeOfBill

    from {{ ref('abs_facility_flat') }}

),

flags as (

    select distinct

        claimId,

        case
          when (typeOfBill like '013%' or typeOfBill like '014%') then 'hospital'
          when typeOfBill like '081%' then 'nonhospital_hospice'
          when typeOfBill like '082%' then 'hospital_hospice'
          when typeOfBill like '083%' then 'asc'
          when typeOfBill like '021%' then 'snf'
          when typeOfBill like '072%' then 'dialysis'
          else 'other'
          end
        as outpatientLocationCategory

    from base

)

select * from flags
