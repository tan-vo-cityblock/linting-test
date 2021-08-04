select *
from {{ source('cotiviti', 'output_member_data') }}
where IS_TARGET_POPULATION
