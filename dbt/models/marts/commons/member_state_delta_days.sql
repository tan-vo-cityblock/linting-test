with member_state_delta_days as (

    select
        patientId,
        TIMESTAMP_DIFF(current_timestamp(), assignedAt, day) AS assignedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), contactAttemptedAt, day) AS contactAttemptedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), reachedAt, day) AS reachedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), interestedAt, day) AS interestedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), veryInterestedAt, day) AS veryInterestedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), consentedAt, day) AS consentedAt_to_currentDate,
        TIMESTAMP_DIFF(current_timestamp(), enrolledAt, day) AS enrolledAt_to_currentDate,

        TIMESTAMP_DIFF(assignedAt, attributedAt, day) AS attributedAt_to_assignedAt,
        TIMESTAMP_DIFF(contactAttemptedAt, assignedAt, day) AS assignedAt_to_contactAttemptedAt,
        TIMESTAMP_DIFF(reachedAt, contactAttemptedAt, day) AS contactAttemptedAt_to_reachedAt,
        TIMESTAMP_DIFF(interestedAt, reachedAt, day) AS reachedAt_to_interestedAt,
        TIMESTAMP_DIFF(veryInterestedAt, interestedAt, day) AS interestedAt_to_veryInterestedAt,
        TIMESTAMP_DIFF(consentedAt , veryInterestedAt, day) AS veryInterestedAt_to_consentedAt,
        TIMESTAMP_DIFF(enrolledAt, consentedAt, day) AS consentedAt_to_enrolledAt,

        TIMESTAMP_DIFF(consentedAt, assignedAt, day) AS assignedAt_to_consentedAt,
        TIMESTAMP_DIFF(enrolledAt, assignedAt, day) AS assignedAt_to_enrolledAt

    from {{ ref('member_states') }}

)

select * from member_state_delta_days