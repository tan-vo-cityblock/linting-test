
{{ config(tags = ['payer_list'] )}}

{%- set payer_list = var('payer_list') -%}

with member_date_of_birth as (

  select 
    memberId as patientId, 
    dateOfBirth as dob

  from {{ ref('src_member_demographics') }}

),

inpatient_claims_base as (

  {% for source_name in payer_list %}

    select
        facility.memberIdentifier.patientId,
        facility.claimId,
        facility.header.DRG.code as drg,
        facility.header.diagnoses,
        facility.header.procedures,
        facility.header.date.from

    from {{ source( source_name, 'Facility') }} as facility

    {% if not loop.last -%}union all {%- endif %}

    {% endfor %}

),

inpatient_claims as (

    select
        facility.patientId,
        facility.claimId,
        facility.drg,
        facility.diagnoses,
        facility.procedures,
        date_diff(facility.from, member_date_of_birth.dob, year) as age,
        costs.costSubCategory

    from inpatient_claims_base as facility

    left join {{ ref('ftr_facility_costs_categories') }} as costs
      on facility.claimId = costs.claimId

    left join member_date_of_birth
      on facility.patientId = member_date_of_birth.patientId

    where costs.costCategory = 'inpatient'
      and costs.costSubCategory in ('acute','psych')

),

-- create the diagnoses dataset to be used to create diagnoses level claim flags

inpatient_claims_diagnoses as (

    select
        patientId,
        claimId,
        drg,
        diagnoses.tier,
        diagnoses.code

    from inpatient_claims

    left join unnest(inpatient_claims.diagnoses) as diagnoses

),

-- create the procedures dataset with certain codeset flags to create claim level procedure flags

pcs_chf_icd10 as (

    select distinct icd10, true as chfPcsFlag

    from {{ source('code_maps','icd9ToIcd10Pcs') }}

    where (
      (substr(icd9,1,3) in ("361", "375", "377")) or
      (substr(icd9,1,4) in ("3601", "3602", "3605"))
    )

),

pcs_ent_icd10 as (

    select distinct icd10, true as entPcsFlag

    from {{ source('code_maps','icd9ToIcd10Pcs') }}

    where (substr(icd9,1,4) in ('2001'))

),

pcs_cellulitis_icd10 as (

    select distinct icd10, true as cellulitisPcsFlag

    from {{ source('code_maps','icd9ToIcd10Pcs') }}

    where (substr(icd9,1,3) in ('860'))

),

pcs_pelvic_icd10 as (

    select distinct icd10, true as pelvicPcsFlag

    from {{ source('code_maps','icd9ToIcd10Pcs') }}

    where (substr(icd9,1,3) in ('683','684','685','686','687','688'))

),

inpatient_claims_procedures as (

    select
        patientId,
        claimId,
        drg,
        procedures.tier,
        procedures.code,

        pcs_chf_icd10.chfPcsFlag,
        pcs_ent_icd10.entPcsFlag,
        pcs_cellulitis_icd10.cellulitisPcsFlag,
        pcs_pelvic_icd10.pelvicPcsFlag

    from inpatient_claims

    left join unnest(inpatient_claims.procedures) as procedures

    left join pcs_chf_icd10
      on procedures.code = pcs_chf_icd10.icd10

    left join pcs_ent_icd10
      on procedures.code = pcs_ent_icd10.icd10

    left join pcs_cellulitis_icd10
    on procedures.code = pcs_cellulitis_icd10.icd10

    left join pcs_pelvic_icd10
      on procedures.code = pcs_pelvic_icd10.icd10

),

diagnoses_flags as (

    select distinct
        claimId,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('428','I50','J81')) or
              (substr(code,1,4) in ('5184')) or
              (substr(code,1,5) in ('40201','40211','40291'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as chfDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('090','A50'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as congenitalSyphilisDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('033','037','045','390','391','A35','A37','A80','I00','I01')) or
              (substr(code,1,4) in ('3220','G000'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as immunizationRelatedPreventableDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('345','G40','G41'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as epilepsyDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (code like 'R56%' or code like '7803%')
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as convulsionDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('462','463','465','J02','J03','J06')) or
              (substr(code,1,4) in ('4721','J312'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as entDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('382','H66'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as entV2DiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('011','012','013','014','015','016','017','018','A17','A18','A19')) or
              (substr(code,1,4) in ('A150','A151','A152','A153','A157','A159','A160','A161','A162','A167','A169','A154','A155','A156','A158','A163','A164','A165','A168'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as tuberculosisDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('491','492','494','496','J41','J42','J43','J44','J47'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as copdDiagnosesAcs,

        case
          when countif(
            tier != 'principal'
            and (
              (substr(code,1,3) in ('491','492','494','496','J41','J42','J43','J44','J47'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as copdSecondaryDiagnosesAcs,

        case
          when countif(
            tier != 'principal'
            and (
              (substr(code,1,4) in ('2826','D570,D571,D572,D578'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as bacterialPneumExcludedDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('481','483','485','486','J13','J14','J16','J18')) or
              (substr(code,1,4) in ('4822','4823','4829','J153','J154','J157','J159'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as bacterialPneumDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('493','J45'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as asthmaDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('I11')) or
              (substr(code,1,4) in ('4010','4019','I100','I101')) or
              (substr(code,1,5) in ('40200','40210','40290'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as hypertensionDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('413','I20')) or
              (substr(code,1,4) in ('4111','4118','I240','I248','I249')) or
              (substr(code,1,5) in ('I2382'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as anginaDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('681','682','683','686','L03','L04','L08','L88')) or
              (substr(code,1,4) in ('L444','L922','L980','L983'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as cellulitisDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,4) in ('2500','2501','2502','2503','2508','2509','E101','E106','E107','E109','E110','E111','E116','E117','E119','E130','E131','E136','E137','E139','E140','E141','E146','E147','E149'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as diabetesDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,4) in ('2512','E160','E161','E162'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as hypoglycemiaDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,4) in ('K522','K528','K529','5589'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as gastroenteritisDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('590','N10','N11','N12')) or
              (substr(code,1,4) in ('5990','5999','N136','N151','N158','N159','N160','N161','N162','N163','N164','N165','N369','N390','N399')) or
              (substr(code,1,5) in ('N2883','N2884','N2885'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as kidneyDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('E86')) or
              (substr(code,1,4) in ('2765'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as dehydrationDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,4) in ('2801','2808','2809','D501','D508','D509'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as anemiaDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('260','261','262','E40','E41','E42','E43')) or
              (substr(code,1,4) in ('2680','2681','E550','E643'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as nutritionDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('R62')) or
              (substr(code,1,4) in ('7834'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as thriveDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('614','N70','N73')) or
              (substr(code,1,4) in ('N994'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as pelvicDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (substr(code,1,3) in ('521','522','523','525','528','K02','K03','K04','K05','K06','K08','K12','K13')) or
              (substr(code,1,4) in ('K098','K099'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as dentalDiagnosesAcs,

        case
          when countif(
            tier = 'principal' and
            drg in ('056','057')
            and (
              (substr(code,1,4) in ('G300','G301', 'G308', 'G309', 'G311','G312', 'G319')) or
              (substr(code,1,5) in ('G3101','G3109', 'G3181', 'G3182','G3183','G3184','G3189','G319'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as alzheimerDiagnosesAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (code like 'J20%') or
              (code like '4660%')
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as acuteBronchitisDiagnosesAcs

    from inpatient_claims_diagnoses

),

procedures_flags as (

    select distinct
        claimId,

        case
          when countif(
            tier = 'principal'
            and (
              (code in ('0SRC0J9','0SRD0J9','0SRB04A','0SR904A','0SRS0J9','0SRB02A','8E0Y0CZ','0SR90JZ','0SR903Z',
                '0SR902A','0SR90JA','0SRB03A','0SRR0J9','0SRB0JZ','0SRB049','0SR902Z','0SPS0JZ','0SRB04Z','0SR904Z','0SRB0JA',
                '0QU70JZ','0SRT0J9','0SR90J9','0SRC069','0SUA09Z','0SRA019','0SRD0JA','0SR9049','0SWC0JZ','0SR903A','0SRR0JZ',
                '0SRB02Z','0SR906A','0SRS01Z','0SRS03A','0SRB01Z','0SRV0J9','0SRC0L9','0SR901A','0SPR0JZ','0SUD09C','0SRW0J9',
                '0SWB0JZ','0SWS0JZ','0SR906Z','0SRD0JZ','0SBD0ZX','0SRE0JA'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as hipkneeProceduresAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (code in ('0UT90ZZ','0UTC0ZZ','0UT90ZL','0UTC4ZZ','0UB90ZZ','0UT94ZZ','0UTC7ZZ','0UT97ZZ','8E0W4CZ','0UT9FZZ',
                        '0UB98ZZ','0UBC7ZX','0UBC0ZZ'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as hysterectomyProceduresAcs,

        case
          when countif(
            tier = 'principal'
            and (
              (code in ('0RB30ZZ','0RT30ZZ','0RG20A0','0RG2071','0RG2070','00NW0ZZ','0RG20K0','0RG4071','0PB30ZZ','01N10ZZ','0RG10AJ','0RG207J',
              '0RG1071','0RT50ZZ','0RB90ZZ','0RG40K1','0RG20K1','00NX0ZZ'))
              )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as spineProceduresAcs,

        case
          when countif(
            chfPcsFlag = true
            ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as chfExcludedProceduresAcs,

        case
          when countif(
            entPcsFlag = true
            ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as entExcludedProceduresAcs,

        case
          when countif(
            cellulitisPcsFlag = true
            ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as cellulitisProceduresAcs,

        case
          when countif(
            pelvicPcsFlag = true
            ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as pelvicExcludedProceduresAcs,

        count(code) over (PARTITION BY claimId) as countProcedures

    from inpatient_claims_procedures

),

flagged_claims as (

    select
        ic.claimId,

        case
          when
            chfDiagnosesAcs = true and
            chfExcludedProceduresAcs = false
            then true
          else false
          end
        as acsChf,

        case
          when
            congenitalSyphilisDiagnosesAcs = true and
            age = 0
            then true
          else false
          end
        as acsCongenitalSyphilis,

        case
          when
            immunizationRelatedPreventableDiagnosesAcs = true and
            age in (1, 2, 3, 4, 5)
            then true
          else false
          end
        as acsImmunizationRelatedPreventable,

        case
          when
            epilepsyDiagnosesAcs = true
            then true
          else false
          end
        as acsEpilepsy,

        case
          when
            convulsionDiagnosesAcs = true
            then true
          else false
          end
        as acsConvulsions,

        case
          when
            entDiagnosesAcs = true or
            (
              entV2DiagnosesAcs = true and
              entExcludedProceduresAcs = false
            )
            then true
          else false
          end
        as acsSevereEnt,

        case
          when
            tuberculosisDiagnosesAcs = true
            then true
          else false
          end
        as acsTuberculosis,

        case
          when
            copdDiagnosesAcs = true
            then true
          else false
          end
        as acsCopd,

        case
          when
            acuteBronchitisDiagnosesAcs = true
            then true
          else false
          end
        as acsAcuteBronchitis,

        case
          when
            bacterialPneumDiagnosesAcs = true and
            age > 0
            then true
          else false
          end
        as acsBacterialPneumonia,

        case
          when
            asthmaDiagnosesAcs = true
            then true
          else false
          end
        as acsAsthma,

        case
          when
            hypertensionDiagnosesAcs = true and
            chfExcludedProceduresAcs = false
            then true
          else false
          end
        as acsHypertension,

        case
          when
            anginaDiagnosesAcs = true and
            countProcedures = 0
            then true
          else false
          end
        as acsAngina,

        case
          when
            cellulitisDiagnosesAcs = true and
            cellulitisProceduresAcs = true and
            countProcedures = 0
            then true
          else false
          end
        as acsCellulitis,

        case
          when
            diabetesDiagnosesAcs = true
            then true
          else false
          end
        as acsDiabetes,

        case
          when
            hypoglycemiaDiagnosesAcs = true
            then true
          else false
          end
        as acsHypoglycemia,

        case
          when
            gastroenteritisDiagnosesAcs = true
            then true
          else false
          end
        as acsGastroenteritis,

        case
          when
            kidneyDiagnosesAcs = true
            then true
          else false
          end
        as acsKidney,

        case
          when
            dehydrationDiagnosesAcs = true
            then true
          else false
          end
        as acsDehydration,

        case
          when
            anemiaDiagnosesAcs = true and
            age <= 5
            then true
          else false
          end
        as acsAnemia,

        case
          when
            nutritionDiagnosesAcs = true
            then true
          else false
          end
        as acsNutritional,

        case
          when
            thriveDiagnosesAcs = true and
            age < 1
            then true
          else false
          end
        as acsFailToThrive,

        case
          when
            pelvicDiagnosesAcs = true and
            pelvicExcludedProceduresAcs = false
            then true
          else false
          end
        as acsPelvicInfla,

        case
          when
            dentalDiagnosesAcs = true
            then true
          else false
          end
        as acsDental,

        case
          when
            alzheimerDiagnosesAcs = true
            then true
          else false
          end
        as acsAlzheimer,


    from inpatient_claims as ic

    left join diagnoses_flags as df
      on ic.claimId = df.claimId

    left join procedures_flags as pf
      on ic.claimId = pf.claimId

)

select * from flagged_claims
