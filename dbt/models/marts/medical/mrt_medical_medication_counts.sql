select
	memberIdentifier as patientId,
	count(distinct ndc) as ndcCount
  
from {{ ref('abs_medications') }}
where
	memberIdentifierField = 'patientId' and
	{{ filter_for_lagged_window(earlier_date="dateFilled") }}
group by 1
