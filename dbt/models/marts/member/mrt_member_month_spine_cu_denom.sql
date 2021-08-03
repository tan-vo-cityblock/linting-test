select 
    * 

from {{ ref('mrt_member_month_spine_cu_denom_all') }} 

where isCityblockMemberMonth is TRUE
