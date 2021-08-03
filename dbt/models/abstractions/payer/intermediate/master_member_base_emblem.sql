with row_nums as (
    select 
        patientId, 
        spanFromDate, 
        effectiveFromDate, 
        memberId,
        rowHash,
        ROW_NUMBER() OVER(ORDER BY patientId, spanFromDate, effectiveFromDate, memberId) AS RowNum
    from {{ ref('master_member_base_emblem_no_row_num') }}
),

base as (
    select * from {{ ref('master_member_base_emblem_no_row_num') }}
)

select 
    base.*,
    row_nums.rowNum,
from base
left join row_nums using(patientId, spanFromDate, effectiveFromDate, memberId, rowHash)
