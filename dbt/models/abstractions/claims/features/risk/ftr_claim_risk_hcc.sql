

{{
  config(
    materialized='table'
  )
}}

with facility as (

    select

        claimId,
        partnerMemberId as memberId,
        principalDiagnosisCode as diagnosis

    from {{ ref('abs_facility_flat') }}

    where claimLineStatus = 'Paid'

),

professional as (

    select

        claimId,
        partnerMemberId as memberId,
        principalDiagnosisCode as diagnosis

    from {{ ref('abs_professional_flat') }}

    where claimLineStatus = 'Paid'

),

data as (

    select * from facility

    union all

    select * from professional

),

mapped as (

    {{ map_hcc_categories(
      diagnosis_column='diagnosis',
      table_name='data',
      index_columns=['claimId'],
      group_by=True
    ) }}

)

select * from mapped
