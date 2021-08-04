WITH outreaches_by_hie AS
    (select
        distinct outreach.id outreach_id,
        outreach.patientid,
        attemptedat as outreach_date,
        outreach.userid as outreach_user_id,
        outreachAttemptUserName as outreach_attempt_username,
        outreach.modality as outreach_modality,
        hie.directobject.key,
        timestamp.receivedat as received_hie_date,
        indirectobject.key as hie_facility_type,
        hie.subject.display as hie_facility_name
    from {{ ref('hie_events') }} hie
    inner join {{ ref('outreach_attempt') }} outreach
        on hie.directobject.key = outreach.patientid
    where hie.directObject.type = 'patientId'
    and outreach.attemptedat > timestamp.receivedat
),

Rank_outreaches_after_hie_event AS (
    SELECT
        *,
        rank() over(partition by received_hie_date order by outreach_date) AS  post_hie_outreach_rank
    FROM outreaches_by_hie
),

final as (
    SELECT
        patientid,
        outreach_id,
        outreach_date,
        outreach_user_id,
        outreach_modality,
        outreach_attempt_username,
        received_hie_date as date_cityblock_receieved_hie_event,
        hie_facility_type,
        hie_facility_name,
        date_diff(outreach_date, received_hie_date, DAY) AS days_between_received_hie_date_and_outreach
    FROM Rank_outreaches_after_hie_event
    where post_hie_outreach_rank = 1
)

select *
from final
