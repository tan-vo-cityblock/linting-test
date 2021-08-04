with

{% if is_incremental() %}

  existing_cases as (

    select * from {{ this }}

  ),

  latest_received_date as (

    select max(receivedDate) as maxReceivedDate
    from existing_cases

  ),

{% endif %}

base as (

  select *,
    identifier.partner,
    memberIdentifier.patientId,
    facilityIdentifier.facilityName,

    coalesce(
      admitDate is not null and   
      placeOfService = 'Inpatient Hospital' and
      careType in ('Emergency', 'Urgent') and
      caseType = 'Concurrent',
      false
    ) as isConcurrentInpatientAuthorization

  from {{ ref('src_prior_auth_gold_all_partners') }}

  where
    memberIdentifier.patientId is not null and
    (statusReason not in ('Duplicate Entry', 'Date Entry Error') or statusReason is null)

),

member_partner as (

  select
    patientId,
    lower(split(partnerName, ' ')[safe_offset(0)]) as partner
    
  from {{ ref('src_member') }}
  where
    lower(partnerName) like 'emblem%' or
    lower(partnerName) like 'connecticare%'
  
),

authorization_version_ids as (

  select
    {{ dbt_utils.surrogate_key(
        ['patientId', 'authorizationId', 'admitDate', 'facilityName', 'isConcurrentInpatientAuthorization']
      ) 
    }} as id,

    * except (partner, facilityName)

  from base

  inner join member_partner
  using (patientId, partner)

),

ranked_version_records as (

  select * except (receivedDate),

    min(receivedDate) over(partition by id) as receivedDate,
    rank() over(partition by id order by receivedDate desc) as rnk

  from authorization_version_ids

),

latest_version_records as (

  select * except (rnk)
  from ranked_version_records
  where rnk = 1

    {% if is_incremental() %}

      and receivedDate > (select maxReceivedDate from latest_received_date)

    {% endif %}

),

flagged_authorization_versions as (

  select *,

    coalesce(
      admitDate >= date_sub(receivedDate, interval 3 day), false
    ) as wasReceivedWithinThreeDaysOfAdmit
  
  from latest_version_records

),

final as (

  select fav.*
  from flagged_authorization_versions fav

  {% if is_incremental() %}

    left join existing_cases ec
    using (id)

    where ec.id is null

  {% endif %}

)

select * from final
