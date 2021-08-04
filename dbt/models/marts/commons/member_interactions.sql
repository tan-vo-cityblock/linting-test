{{- config(materialized='view') -}}

select *
from {{ ref('member_interactions_all') }}
where eventSource in ('commons_progress_notes', 'elation_notes', 'epic_notes')
