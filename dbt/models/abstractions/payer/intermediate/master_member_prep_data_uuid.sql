
-- to save on memory usage, sorting the prep data in a separate query
-- result from the prep work sql
with result as (

  select 
    *,
    GENERATE_UUID() as rowid

 from {{ ref('master_member_prep_data') }}
)

select * from result