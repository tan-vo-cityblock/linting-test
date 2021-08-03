{{ config(materialized = 'view') }}

select * replace(
  nullif(
    replace(multi_ccs_lvl_2, '.', ''),
    ' '
  ) as multi_ccs_lvl_2
)

from {{ source('codesets', 'ccs_dx_icd10cm') }}
