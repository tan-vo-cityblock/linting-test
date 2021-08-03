with abs_prior_auth_emblem_cci_latest_version_dates as (

  select 

    {{ dbt_utils.surrogate_key(
        ['patientId', 'authorizationId']
      ) 
    }} as memberAuthorizationId,

    *

  from {{ ref('abs_prior_auth_emblem_cci_latest_version_dates') }}
  
),

ranked_member_authorizations as (

  select *,
    rank() over(partition by memberAuthorizationId order by receivedDate desc) as rnk

  from abs_prior_auth_emblem_cci_latest_version_dates

),

latest_member_authorizations as (

  select * except (rnk)
  from ranked_member_authorizations
  where rnk = 1

),

expanded_authorizations as (

  select distinct
    memberAuthorizationId,
    patientId,
    authorizationId,
    authorizationStatus,
    placeOfService,
    careType,
    caseType,
    admitDate,
    dischargeDate,
    serviceStartDate,
    serviceEndDate,
    receivedDate,
    isConcurrentInpatientAuthorization,
    wasReceivedWithinThreeDaysOfAdmit,
    facilityIdentifier.facilityNpi,
    facilityIdentifier.facilityName,

    {% for field in ['Address1', 'Address2', 'City', 'State', 'County', 'Zip', 'Country', 'Email', 'Phone'] %}

      facilityIdentifier.facilityAddress.{{ field }} as facility{{ field }},

    {% endfor %}

    diagnosis.*,

    {% for field in ['Code', 'Modifier', 'ServiceUnits', 'UnitType'] %}

      {% if field == 'Modifier' %}

        procedure{{ field }},

      {% else %}
    
        procedure.{{ field }} as procedure{{ field }},

      {% endif %}

    {% endfor %}

    {% for provider_type in ['servicing', 'requesting'] %}

      {% for field in ['ProviderId', 'PartnerProviderId', 'ProviderNpi', 'Specialty', 'ProviderName'] %}

        {% if field == 'PartnerProviderId' %}

          {{ provider_type }}Provider.{{ field }} as {{ provider_type }}ProviderPartnerId,

        {% elif field == 'Specialty' %}

          {{ provider_type }}Provider.{{ field }} as {{ provider_type }}Provider{{ field }},

        {% else %}

          {{ provider_type }}Provider.{{ field }} as {{ provider_type }}{{ field }},

        {% endif %}

      {% endfor %}

    {% endfor %}

    serviceType,
    serviceStatus,
    statusReason,
    requestDate,
    recordCreatedDate,
    recordUpdatedDate

  from latest_member_authorizations

  left join unnest(diagnosis) as diagnosis
  left join unnest(procedures) as procedure
  left join unnest(procedure.modifiers) as procedureModifier

),

final as (

  select
    {{ dbt_utils.surrogate_key(
        ['memberAuthorizationId', 'diagnosisCode', 'procedureCode', 'procedureServiceUnits']
      ) 
    }} as id,

    *

  from expanded_authorizations

)

select * from final
