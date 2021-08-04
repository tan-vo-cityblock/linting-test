(

with prof_slct as (
  select
      patient.patientId,
      prof.patient.externalId,
      claim.ATT_PROV,
      claim.BILL_PROV,
      prof.claim.SERVICINGPROVLOCSUFFIX,
      prof.claim.BILLINGPROVLOCATIONSUFFIX,
      claim.PROC_CODE,
      prov.provider.PROV_FNAME,
      prov.provider.PROV_LNAME,
      prov.provider.PROV_NPI,
      claim.POS,
      pos_desc_lo,
      claim.FROM_DATE as occuredAt,
      array_agg(distinct claim.claim_id) ids,
--       array_agg(
--           struct(claim.claim_id,
--                  claim.sv_line,
--                  claim.SV_STAT,
--                  claim.AMT_PAID,
--                  prof.claim.PAID_DATE) order by claim.claim_id, claim.sv_line, claim.SV_STAT) paid_statuses,
      'services' as verb
    from
      `emblem-data.silver_claims.professional` prof
      left join `emblem-data.silver_claims.providers` prov
--       on prof.claim.ATT_PROV = prov.provider.PROV_ID and prof.claim.SERVICINGPROVLOCSUFFIX = prov.provider.PROV_LOC
      on prof.claim.BILL_PROV = prov.provider.PROV_ID and prof.claim.BILLINGPROVLOCATIONSUFFIX = prov.provider.PROV_LOC
      left join `reference-data-199919.claims_descriptions.pos_codes_lo` pos
      on pos.pos_code_lo = prof.claim.POS
      where patient.patientId is not null
      group by
        patient.patientId,
        prof.patient.externalId,
        claim.ATT_PROV,
        claim.BILL_PROV,
        prof.claim.SERVICINGPROVLOCSUFFIX,
        prof.claim.BILLINGPROVLOCATIONSUFFIX,
        claim.PROC_CODE,
        prov.provider.PROV_FNAME,
        prov.provider.PROV_LNAME,
        prov.provider.PROV_NPI,
        prof.claim.POS,
        pos_desc_lo,
        claim.FROM_DATE
)

select
  STRUCT(
       ids,  -- Group in denied and paid claims with different claim ids
      'CLAIM_ID' as idField,
      'emblem-data' as project,
      'silver_claims' as dataset,
      'professional' as table
    ) as source,

  STRUCT(
     'BILL_PROV' as `type`,
     case when BILL_PROV = '' then null else BILL_PROV end as `key`,
     case when PROV_FNAME is not null and PROV_FNAME <> '' then concat(PROV_FNAME, ' ', PROV_LNAME) else PROV_LNAME end as `display`
  ) as subject,

  verb,

  STRUCT(
    'patientId' as `type`,
    patientId as `key`,
    STRING(NULL) as `display`
  ) as directObject,

  STRUCT(
    'setting' as `type`,
    pos_desc_lo as `key`
  ) as indirectObject,

  STRUCT(
    'market' as `type`,
    'New York City' as `key`
  ) as prepositionalObject,

  STRUCT(
    cast(occuredAt as TIMESTAMP) as createdAt,
    TIMESTAMP(NULL) as completedAt,
    TIMESTAMP(NULL) as manualEventAt,
    TIMESTAMP(NULL) as receivedAt
  ) as timestamp,

  [
      STRUCT(
        'CPT' as `type`,
        upper(PROC_CODE) as `key`
      )
  ] as purposes,

  [
    STRUCT(
      STRING(NULL) as `type`,
      STRING(NULL) as `key`
    )
  ]
  as outcomes

from prof_slct

)
