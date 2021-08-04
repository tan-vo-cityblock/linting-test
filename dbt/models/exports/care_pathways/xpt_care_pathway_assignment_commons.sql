
select *

from {{ ref('mrt_care_pathway_assignment') }}

where pathwaySlug != 'base-care-model'
