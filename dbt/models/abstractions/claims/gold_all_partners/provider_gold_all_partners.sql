
{{
  config(
    udf = '''
CREATE TEMPORARY FUNCTION toTrimmedTitleCase(str STRING)
          RETURNS STRING
          LANGUAGE js AS """
            if (str == null){
              return null;
            }
            return str.trim().replace(/^\\\\.\\\\s+/, '').replace(
                /\\\\w\\\\S*/g,
                function(txt) {
                    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
                }
            );
          """;
  '''
  )
}}

{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}

with gold_provider as (

    {% for source_name in payer_list %} 

            select 
                id, 
                name,
                npi,
                phone,
                email,
                address1,
                address2,
                city,
                state,
                zip
            from (
                  -- grabbing only 1 provider name per provider id defaulting to the latest effectiveFrom record
                  select
                    providerIdentifier.id, 
                    name,
                    npi,
                    locations.clinic.phone,
                    locations.clinic.email,
                    locations.clinic.address1,
                    locations.clinic.address2,
                    locations.clinic.city,
                    locations.clinic.state,
                    locations.clinic.zip,
                    row_number() over (partition by providerIdentifier.id order by dateEffective.from desc) as rownum
                  from {{ source(source_name, 'Provider') }} , unnest(locations) as locations
                  where name is not null
                 )
            where rownum = 1

        {% if not loop.last -%} union distinct {%- endif %}

    {% endfor %}

),

final as (

  select
    id,
    max(toTrimmedTitleCase(name)) as name,
    max(npi) as npi,
    max(phone) as phone,
    max(email) as email,
    max(address1) as address1,
    max(address2) as address2,
    max(city) as city,
    max(state) as state,
    max(zip) as zip
  from gold_provider

  group by id

)

select * from final
