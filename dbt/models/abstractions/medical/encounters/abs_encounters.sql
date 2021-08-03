{%- set model_list = ['stg_encounters_claims', 'stg_encounters_hie'] -%}

with encounters as (

  {% for model in model_list %}

    select 
      patientId, 
      date, 
      ed, 
      acsEd, 
      obs, 
      inpatient,
      acsInpatient,
      bhInpatient,
      resultsInInpatient

    from {{ ref(model) }}

    {% if not loop.last %}

      union distinct

    {% endif %}

  {% endfor %}

),

final as (

  select 
    {{ dbt_utils.surrogate_key(['patientId', 'date', 'ed', 'acsEd', 'obs', 'inpatient', 'acsInpatient', 'bhInpatient', 'resultsInInpatient']) }} as id,
    *

  from encounters

)

select * from final
