select *
from {{ source('member_index', 'member_datasource_identifier') }}
where deletedAt is null
