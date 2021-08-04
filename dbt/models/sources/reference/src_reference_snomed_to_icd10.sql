select
  cast(referencedComponentId as string) as snomedCode,
  regexp_replace(mapTarget, r'[.]', '') as mapTarget

from {{ source('code_maps', 'snomed_to_icd10') }}
