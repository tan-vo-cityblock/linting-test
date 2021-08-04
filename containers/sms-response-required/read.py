"""Read input data."""

import pandas as pd


def query_db(project_id):
    sql_query = """
            WITH sms_msg AS (
              SELECT id,
                      userId as user_id,
                      patientId as patient_id,
                      contactNumber as contact_number,
                      COALESCE(providerCreatedAt, createdAt) as created_at,
                      body,
                      direction,
                      provider,
                      NULL as call_receiver,
                      NULL as duration,
                      NULL as call_status,
                      "SMS message" as type
              FROM `cityblock-analytics.mrt_communications.sms_message`
              ),

            sms_session AS (
              SELECT messageId,
                     sessionId as session_id
              FROM `cityblock-analytics.mrt_communications.sms_session_conversation`
            ),

            historical_member_state as (
              SELECT patientId as patient_id,
                     currentState as historical_state,
                     createdAt as state_valid_from,
                     coalesce(timestamp_sub(lead(createdAt) over (
                                              partition by patientId
                                              order by createdAt, updatedAt),
                                            interval 1 microsecond),
                              "9999-01-01") as state_valid_to
              FROM `cbh-db-mirror-prod.commons_mirror.patient_state`
            )

            SELECT sms_msg.*,
                   sms_session.session_id,
                   CASE when user.userPhone is not null then TRUE else FALSE END AS is_internal_contact_number,
                   coalesce(hms.historical_state, 'non_member') as historical_state,
                   CASE
                     when hms.historical_state in ('consented', 'enrolled') then 'cbh_engaged'
                     when hms.historical_state is null then 'non_member'
                     else 'pre_cbh_engaged'
                   END as coarse_historical_state
            FROM sms_msg
            LEFT JOIN sms_session
              ON sms_msg.id = sms_session.messageId
            LEFT JOIN `cityblock-analytics.mrt_commons.user` user
              on sms_msg.contact_number = user.userPhone
            LEFT JOIN historical_member_state hms
              on hms.patient_id = sms_msg.patient_id
                 and sms_msg.created_at between hms.state_valid_from and hms.state_valid_to
            """
    
    print('Ingesting SMS data')
    sms_msg_read = pd.read_gbq(query=sql_query, project_id=project_id, dialect="standard")
    sms_msg_all = sms_msg_read.sort_values(['contact_number','created_at']).reset_index(drop=True)

    return sms_msg_all
