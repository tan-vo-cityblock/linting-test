
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
        qr.*,
        m.patientId

    from {{ source('qreviews', 'qreviews_results') }} as qr

    left join {{ ref('src_member') }} as m
      on qr.recipient.uid = safe_cast(m.cbhId as string)

),

renamed as (

    select
        *

    from base

),

flagged as (

    select
        *,

        case
          -- these are the two nps questions we've used
          when question.id in (799, 993) then true
          else false
          end
        as isNpsQuestion

    from renamed

)

select * from flagged
