
{% macro bp_level(

  slug,
  vitals_code,
  vitals_code_2,
  severe_min,
  severe_min_2,
  moderate_min,
  moderate_min_2,
  mild_min,
  mild_min_2,
  stable_cpt,
  stable_cpt_1,
  stable_cpt_2,
  day_min=14,
  period="1 year",
  subperiod="4 month",
  vitals_table=ref('vitals'),
  cpt_table=ref('abs_procedures')

	)
%}

{{ construct_combined_bp(

    vitals_code, 
    vitals_code_2, 
    severe_min, 
    severe_min_2, 
    moderate_min, 
    moderate_min_2, 
    mild_min, 
    mild_min_2,
    stable_cpt,
    stable_cpt_1,
    stable_cpt_2,
    period,
    vitals_table,
    cpt_table

    ) 
}}

following_readings as (

  select
    av.patientId,
    av.timestamp,
    av1.timestamp as timestamp1,
    av1.acuity < av.acuity as isLowerAcuity
  from all_vitals av
  left join all_vitals av1
  on 
    av.patientId = av1.patientId and
    av.timestamp < av1.timestamp

),
    
stable_or_higher_readings as (

  select 
    patientId, 
    timestamp, 
    count(timestamp1) as numStableOrHigher
  from following_readings
  where isLowerAcuity is false
  group by patientId, timestamp

),

lower_readings as (

  select 
    patientId, 
    timestamp, 
    count(timestamp1) as numLower, 
    date_diff(date(max(timestamp1)), date(min(timestamp1)), day) as dateDiffLower
  from following_readings
  where isLowerAcuity is true
  group by patientId, timestamp

),

final_readings as (

  select 
    av.patientId, 
    min(av.timestamp) as timestamp
  from all_vitals av
  left join stable_or_higher_readings sohr
  using (patientId, timestamp)
  left join lower_readings lr
  using (patientId, timestamp)
  where 
    sohr.numStableOrHigher is null and
    (lr.numLower is null or lr.dateDiffLower < {{ day_min }})
  group by patientId

),

cf_status as (

  select 
    patientId, 
    case 
      when acuity = 4 then 'severe'
      when acuity = 3 then 'moderate'
      when acuity = 2 then 'mild'
      when acuity = 1 then 'stable'
    end as value
  from all_vitals
  inner join final_readings
  using (patientId, timestamp)

),

final as (

    {{ computed_field(slug=slug, type='bp_level', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
