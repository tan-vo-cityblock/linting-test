
-- doing all the ordering here, after table is already constructed in separate upstream queries
-- added a distinct here bc pavel noticed there  were some unexplained  dupes. need to resolve this
with mmt as (
    select distinct * 
    from  {{ ref('master_member_prep_data_unsorted2') }}
)

select 
    mmt.*,
    hhh.medicaidEndDate,
    hhh.harpStatus,
    hhh.healthHomeStatus,
    hhh.reportDate
from mmt
-- join health home statuses (owned by aalap)
left join {{ ref('health_home_harp_medicaid_end_dates') }} hhh
    on mmt.medicaidId = hhh.medicaid_id