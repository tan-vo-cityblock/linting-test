
with risk_area_group_acuities as (

  select 
    patientId,
    riskAreaGroupSlug as slug,
    acuity as acuityScore,
    {{ convert_acuity_score_to_description('acuity') }} as acuityDescription

  from {{ source('commons', 'patient_risk_area_group_acuity') }}

),

risk_area_groups as (

  select
    slug,
    shortTitle as domainName
  
  from {{ source('commons', 'builder_risk_area_group') }}

),

final as (

  select
    a.patientId,
    g.domainName,
    a.acuityScore,
    a.acuityDescription

  from risk_area_group_acuities a

  left join risk_area_groups g
  using (slug)

)

select * from final
