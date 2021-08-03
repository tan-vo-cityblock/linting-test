{{- config(materialized='view') -}}

select * from {{ ref('stg_commons_outreach_notes') }}
union all
select * from {{ ref('stg_commons_progress_notes') }}
