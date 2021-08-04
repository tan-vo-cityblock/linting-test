
{% macro create_readmission_flag(table,
                                 startDate,
                                 endDate,
                                 memberId='patientId',
                                 claimId='claimId',
                                 stayGroup='stayGroup') %}

{% set uuid = unique_id() %}

with data{{ uuid }} as (

    select distinct
        {{ memberId }} as memberId,
        {{ claimId }} as claimId,
        {{ startDate }} as startDate,
        {{ endDate }} as endDate,
        {{ stayGroup }} as stayGroup

    from {{ table }}

),

base{{ uuid }} as (

    select
        claimId,
        memberId,
        startDate,
        endDate,
        stayGroup,

        lag(endDate, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as previousEndDate,

        lead(startDate, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as nextStartDate,

        lag(stayGroup, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as previousStayGroup,

        lead(stayGroup, 1) OVER (
          PARTITION BY memberId
          ORDER BY startDate, endDate)
        as nextStayGroup

    from data{{ uuid }}

),

final{{ uuid }} as (
-- need to do the downstream joins at the claimId level, but want to flag readmission true/false at the stayGroup level so that all claims in the stayGroup are tagged with the same boolean value
-- downstream count functions in looker count distinct admissionId aka stayGroupId
-- does this admit occur within 30d of the last admit?
    select
        claimId,
    max(
        case
          when (
            previousEndDate >= date_sub(startDate, INTERVAL 30 DAY) and
            previousStayGroup != stayGroup) then true
          else false
          end
        ) over(partition by stayGroup)
        as readmissionFlag,

-- did this admit result in a readmit within 30d?    
    max(
        case
          when (
            nextStartDate <= date_add(endDate, INTERVAL 30 DAY) and
            nextStayGroup != stayGroup) then true
          else false
          end
        ) over(partition by stayGroup)
        as resultsInReadmissionFlag

     from base{{ uuid }}

 )

 select * from final{{ uuid }}

{% endmacro %}
