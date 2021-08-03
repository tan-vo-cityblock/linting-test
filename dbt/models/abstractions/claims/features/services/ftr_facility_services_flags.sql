
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      claimId,
      revenueCode,
      procedureCode,
      typeOfBill

    from {{ ref('abs_facility_flat') }}

),

flags as (

    select distinct
        claimId,

        case
          when countif(revenueCode in (
            '0450','0451','0452','0456','0459'
          )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as edServiceFlag,

        case
          when countif(revenueCode in (
            '0200', '0201', '0202', '0203', '0204', '0206', '0207',
            '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0219'
          )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as icuServiceFlag,

        case
          when countif(procedureCode in (
            'G0463','T1015','99210','99202','99203','99204','99205',
            '99211','99212', '99213','99214','99215'
          )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as clinicServiceFlag,

        case
          when countif(
            revenueCode like '042%' or
            revenueCode like '043%'
          ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as ptServiceFlag,

        case
          when countif((revenueCode in (
            '0760', '0761', '0762', '0769'
          )) or (procedureCode in (
            '99217', '99218', '99219', '99220', '99224', '99225',
            '99226', '99356', '99357', 'G0378', 'G0379'
          ))) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as obsServiceFlag,

        case
          when countif(procedureCode = 'H0020')
          over (PARTITION BY claimId) > 0 then true
          else false
          end
        as methadoneServiceFlag,

        case
          when countif(revenueCode like '019%')
          over (PARTITION BY claimId) > 0 then true
          else false
          end
        as subAcuteServiceFlag,

        case
          when countif((
            revenueCode like '082%' or
            revenueCode like '082%')
          ) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as dialysisServiceFlag,

        case
          when (
            typeOfBill like '031%' or
            typeOfBill like '032%' or
            typeOfBill like '033%' or
            typeOfBill like '034%')
            then true
          else false
          end
        as homeHealthServiceFlag,

        case
          when countif(procedureCode in (
            'A0427','A0429','A0434'
          )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as emergentAmbulanceServiceFlag

    from base

)

select * from flags
