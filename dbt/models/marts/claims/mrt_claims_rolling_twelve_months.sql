select *
from {{ ref('mrt_claims_last_three_years_paid') }}
where {{ filter_for_lagged_window() }}
