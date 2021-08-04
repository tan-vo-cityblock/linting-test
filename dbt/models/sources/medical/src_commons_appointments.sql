select 
    * 
    
from {{ source('commons', 'appointment') }}

where deletedAt is null
  and startAt >= '2017-01-01'
