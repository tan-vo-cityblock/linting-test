
{{ config(materialized = 'ephemeral') }}

with ranked_note_text as (

  select
    eventGroupId,
    templateSlug,
    text,
    rank() over(partition by eventGroupId order by createdAt desc) as rnk
  from {{ source('commons', 'note') }}

)

select
  eventGroupId,
  templateSlug,
  text
from ranked_note_text
where rnk = 1
