--Use procedures to identify flu/pneumonia shots, one line per member

WITH all_proc_codes AS (

  SELECT
    memberIdentifier as patientId,
    procedureCode as procCode,
    procedureCodeset as procCodeSet,
    sourceId as claimId,
    serviceDateFrom as fromDate,
    serviceDateTo as toDate,
    reporting_date
  FROM {{ ref('abs_procedures') }}
--we want to make sure all createdAt dates are between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
  inner join {{ ref('cm_del_reporting_dates_ytd') }}
    on serviceDateFrom <= reporting_date
  INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
    on  date(serviceDateFrom) >= delegation_at
    and memberIdentifier = dd.patientid
  WHERE memberIdentifierField = 'patientId' AND
    sourceType = 'claim'

),


procedures as (
SELECT DISTINCT
  proc.*,
   cpt.DESCRIPTION as procCodeDesc,
   cpt2.concept_name as procCodeDesc2,
   (proc.procCode in ("90653", "90656", "90662", "90674", "90682", "90685",
                               "90686", "90687", "90688", "90756", "Q2035", "90689")
                  ) flu_shot,
   (procCode in ("90732", "90670", "90669")
                  ) pneumonia_shot
  FROM all_proc_codes proc
  LEFT JOIN {{ source('codesets', 'hcpcs') }} cpt
  ON proc.procCode = cpt.HCPCS
  LEFT JOIN {{ source('codesets', 'hcpcs_full') }} cpt2
  ON proc.procCode = cpt2.concept_code

  WHERE procCode in ('90653', '90656', '90662', '90674', '90682', '90685',
    '90686', '90687', '90688', '90756', 'Q2035', '90689', '90732', '90670', '90669')
    AND procCodeSet like '%CPT%'
    AND patientId is not null
),

flu_shot as (
--https://github.com/cityblock/mixer/blob/e593c3a2049b3e945ca84b0e7c978fd4246dadf8/containers/cm_delegation_reports/step2_transform.R#L242
    select
        patientId,
        max(flu_shot) as flu_shot_received, --max takes the trues over the falses
        min(case when flu_shot is true then fromDate else null end) as flu_shot_received_date
    FROM procedures
    where date_sub(reporting_date, interval 1 year) < fromDate
    group by 1

),

pneumonia_shot as (
    select
        patientId,
        max(pneumonia_shot) as pneumonia_shot_received,
        min(case when pneumonia_shot is true then fromDate else null end) as pneumonia_shot_received_date
    FROM procedures
    group by 1
)

select
patientId,
coalesce(flu_shot_received,false) as flu_shot_received,
flu_shot_received_date,
coalesce(pneumonia_shot_received,false) as pneumonia_shot_received,
pneumonia_shot_received_date
from pneumonia_shot
left join flu_shot using (patientId)
