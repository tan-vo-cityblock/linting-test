select distinct
  {{ dbt_utils.surrogate_key(['body', 'type', 'createdAt']) }} as id,
  body,
  type,
  createdAt

from {{ source('commons', 'event') }}
