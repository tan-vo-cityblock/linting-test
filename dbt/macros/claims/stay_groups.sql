
{% macro create_stay_groups(table,
                            startDate,
                            endDate,
                            transferStatusCode,
                            memberId='patientId',
                            claimId='claimId',
                            providerBillingId='providerBillingId',
                            dischargeStatus='dischargeStatus',
                            flag_overlap=False,
                            filter=None) %}


{% set uuid = unique_id() %}

with data{{ uuid }} as (

    select distinct
        {{ memberId }} as memberId,
        {{ claimId }} as claimId,
        {{ startDate }} as startDate,
        {{ endDate }} as endDate,
        {{ providerBillingId }} as providerBillingId,
        {{ dischargeStatus }} as dischargeStatus

    from {{ table }}

    {% if filter %}
    where {{ filter }}
    {% endif %}

),

base{{ uuid }} as (

    select
        claimId,
        memberId,
        startDate,
        endDate,
        providerBillingId,

        row_number() over (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as admissionNumber,

        lag(endDate, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as previousEndDate,

        lag(dischargeStatus, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as previousDischargeStatus,

        lag(providerBillingId, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as previousProviderBillingId

    from data{{ uuid }}

),

transfers{{ uuid }} as (

    select
        claimId,
        memberId,
        startDate,
        endDate,
        previousEndDate,

        case
          when (
            previousEndDate >= date_sub(startDate, INTERVAL 1 DAY) and
            previousDischargeStatus = '{{ transferStatusCode }}' and
            providerBillingId != previousProviderBillingId) then 'transfer'
          when (
            previousEndDate >= date_sub(startDate, INTERVAL 1 DAY) and
            previousDischargeStatus = '30' and
            providerBillingId = previousProviderBillingId) then 'partial'

          {% if flag_overlap %}
          when (
            previousEndDate >= date_sub(startDate, INTERVAL 1 DAY)) then 'overlap'
          {% endif %}

          else 'index'
          end
        as stayType

     from base{{ uuid }}

 ),

 groups{{ uuid }} as (

    select
        claimId,
        stayType,

        to_hex(
          md5(
            concat(
              cast(
                SUM(IF(stayType != 'index', 0, 1))
                OVER (
                  PARTITION BY memberId
                  ORDER BY startDate, endDate)
                  as string)
              , '{{ uuid }}'
              , memberId)
            )
          )
        AS stayGroup

    from transfers{{ uuid }}

)

select * from groups{{ uuid }}

{% endmacro %}
