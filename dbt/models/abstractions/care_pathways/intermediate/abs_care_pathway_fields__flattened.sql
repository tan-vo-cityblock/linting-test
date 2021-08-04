with input_fields as (

  select * from {{ ref('src_care_pathway_input_fields') }}

),

flattened_fields as (

  select distinct
    cf.patientId,
    i.pathwaySlug,
    cf.fieldSlug,
    cf.fieldType,
    i.fieldLevel,
    er.id as evaluatedResourceId,
    er.key as evaluatedResourceKey,

    case
      when cf.fieldType = 'boolean_evidence'
        {# convert 'cf_field_name' to 'field-name', in order to join on `fieldSlug` in later models #}
        then replace(right(er.model, char_length(er.model) - 3), '_', '-')
      else er.model
    end as evaluatedResourceModel

  from {{ ref('all_computed_fields') }} cf
  left join unnest(evaluatedResource) er
  
  inner join input_fields i
  using (fieldSlug)

  where cf.fieldValue = 'true'

),

final as (

  select
    {{ dbt_utils.surrogate_key(['patientId', 'pathwaySlug', 'fieldSlug', 'fieldLevel', 'evaluatedResourceId']) }} as id,
    *

  from flattened_fields

)

select * from final
