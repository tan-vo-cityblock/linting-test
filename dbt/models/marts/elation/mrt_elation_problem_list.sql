with abs_diagnoses as (

  select distinct
    sourceId as claimId,
    memberIdentifier as patientId,
    serviceDateTo as dateTo,
    serviceDateFrom as dateFrom,
    diagnosisCodeset as codeset,
    diagnosisCode as code
    
  from {{ ref('abs_diagnoses') }}
  where
    memberIdentifierField = 'patientId' and
    source in ('facility', 'professional') and
    serviceDateFrom > date_sub(date_trunc(current_date, year), interval 3 year)

),

member_info as (

    select
        m.id as memberId,
        mrn.mrn as elationId,
        mi.gender

    from {{ source('member_index', 'member') }} as m
    
    inner join {{ source('member_index', 'mrn') }} mrn 
      on mrn.id = m.mrnId

    left join {{ ref('member_info') }} as mi
      on mi.patientId = m.id

    where mrn.name = 'elation'
      and mrn.deletedAt is null

),

grouped as (

    select
        patientId,
        code,
        codeset,
        count(distinct claimId) as codeCount,
        min(dateFrom) as minDateFrom,
        max(dateFrom) as maxDateFrom

    from abs_diagnoses

    group by patientId, code, codeset


),

merged as (

    select distinct
        diag.*,
        hcc.hcc_v24 as hccCategory,
        cast(hcc.hcc_v24_is_chronic as string) as hccIsChronic,
        ccs.ccs_category as ccsCategory,

        case
          when ccs.ccs_category in
          ('5',
          '6',
          '9',
          '11',
          '12',
          '13',
          '14',
          '15',
          '16',
          '17',
          '18',
          '19',
          '20',
          '21',
          '22',
          '23',
          '24',
          '25',
          '26',
          '27',
          '28',
          '29',
          '30',
          '31',
          '32',
          '33',
          '34',
          '35',
          '36',
          '37',
          '38',
          '39',
          '40',
          '41',
          '42',
          '43',
          '44',
          '46',
          '48',
          '49',
          '50',
          '51',
          '53',
          '54',
          '55',
          '56',
          '57',
          '59',
          '60',
          '61',
          '62',
          '63',
          '79',
          '80',
          '81',
          '82',
          '83',
          '85',
          '86',
          '87',
          '88',
          '89',
          '96',
          '97',
          '98',
          '99',
          '100',
          '101',
          '103',
          '105',
          '106',
          '107',
          '108',
          '109',
          '110',
          '113',
          '114',
          '115',
          '116',
          '118',
          '127',
          '128',
          '129',
          '130',
          '131',
          '132',
          '136',
          '137',
          '138',
          '139',
          '140',
          '143',
          '144',
          '145',
          '146',
          '149',
          '151',
          '152',
          '153',
          '156',
          '157',
          '158',
          '160',
          '164',
          '169',
          '170',
          '171',
          '173',
          '174',
          '176',
          '177',
          '178',
          '179',
          '180',
          '181',
          '182',
          '183',
          '184',
          '186',
          '187',
          '188',
          '189',
          '190',
          '191',
          '192',
          '193',
          '194',
          '195',
          '199',
          '201',
          '202',
          '204',
          '206',
          '207',
          '208',
          '209',
          '210',
          '211',
          '212',
          '213',
          '214',
          '215',
          '216',
          '219',
          '220',
          '221',
          '222',
          '223',
          '224',
          '227',
          '237',
          '241',
          '242',
          '243',
          '248',
          '249',
          '650',
          '651',
          '652',
          '653',
          '654',
          '655',
          '656',
          '657',
          '658',
          '659',
          '660',
          '661',
          '662',
          '670') then true
          else false
          end
        as ccsChronic

    from grouped as diag

    left join {{ source('codesets', 'ccs_dx_icd10cm') }} as ccs
      on diag.code = ccs.icd_10_cm_code

    left join {{ source('codesets', 'hcc_2020') }} as hcc
      on diag.code = hcc.diagnosis_code

),

filtered as (

    select
        *

    from merged

    where ccsChronic = true
      or hccIsChronic = 'Yes'

)
,

flagged as (

    select
        *,
        countif(hccIsChronic ='Yes') over (partition by patientId, ccsCategory) as ccsHccCount,
        rank() over (partition by patientId, cast( hccCategory as string)  order by maxDateFrom desc) as hccDateRank,
        rank() over (partition by patientId, ccsCategory order by maxDateFrom desc) as ccsDateRank

    from filtered

),


problems as (
select coding.* ,
rank() over (partition by patientId, ccsCategory order by patientId,code desc) as finalranking,
from
(
    select distinct
        patientId,
        member_info.elationId,
        member_info.gender,
        code,
        hccCategory,
        hccDateRank,
        ccsChronic,
        ccsHccCount,
        ccsDateRank,
        ccsCategory

    from flagged

    left join member_info
      on flagged.patientid = member_info.memberId

    where
      (cast( hccCategory as string) is not null and hccDateRank = 1)
      or
      (ccsChronic = true and ccsHccCount = 0 and ccsDateRank = 1)
) coding
),



final AS 

(
select * from
(select patientId,        elationId        ,gender,        code, ccscategory,
case when BYTE_LENGTH(code) >3 then concat(SUBSTR(code,1,3),'.',SUBSTR(code,4,6))
else code end
as code_decimal,
hccCategory,
--ifnull(hcc_min, CAST(hccCategory as INT64) ) as newcol,
ROW_NUMBER() OVER(PARTITION BY PatientID, ifnull(hcc_min, CAST(hccCategory as INT64) ) ORDER BY   hccCategory ) AS rownum
from
(
select 
patientId        ,elationId,        gender,        code,        ccsCategory, hccCategory,        hcc1,        
min(hcc2) as hcc_min
from
(select
patientId,
elationId,        
gender,        
code,ccsCategory,        hccCategory, hcc1,
hcc2        
    from problems r
    left outer join 
    (select * from {{ source('codesets', 'hcc_hierarchies_xwalk') }}  where version = 'v24') h 
    on r.hccCategory = cast(h.hcc1 as string)
        where finalranking =1
    )
 group by
patientId        ,elationId,        gender,        code,        hccCategory,        ccsCategory,hcc1) hccs)
where (hccCategory is not null and rownum = 1) OR (hccCategory is null and rownum > 0)

)

select * from final