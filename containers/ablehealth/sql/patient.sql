SELECT
    pic.id as external_id,
    p.firstName as first_name,
    p.lastName as last_name,
    p.dateOfBirth as date_of_birth,

    case
      when mi.gender = "male" then "m"
      when mi.gender = "female" then "f"
      else ""
      end
    as sex,

    NULL as dual_eligible_status,
    0 as institutional_status,

    case
      when p.medicaidId is not null then 1
      else 0
      end
    as medicaid_status,

    0 as disabled_status,
    NULL as special_risk_category

FROM `cbh-db-mirror-prod.member_index_mirror.member` as pic

LEFT JOIN `cbh-db-mirror-prod.commons_mirror.patient` as p
  on pic.id = p.id

left join `cityblock-analytics.mrt_commons.member_info` as mi
  on pic.id = mi.patientId

where pic.deletedAt is null
