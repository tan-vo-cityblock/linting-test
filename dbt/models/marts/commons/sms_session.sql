SELECT
  td.session_id,
  sms.contactNumber,
  td.session_created_at,
  sms.direction,
  sms.patientId,
  sms.userId,
  td.is_internal_contact_number,
  sms.provider,
  td.historical_state,
  td.coarse_historical_state,
  td.possible_convo_end,
  td.tdiff_mins,
  td.adjusted_tdiff_mins,
  TIME_ADD(TIME "00:00:00", INTERVAL cast((td.tdiff_mins*60) as INT64) SECOND) as tdiff_hms,
  ARRAY_AGG(STRUCT(
      sms.id,
      COALESCE(sms.providerCreatedAt, sms.createdAt) AS createdAt,
      sms.body,
      prob__label__1,
      custom_pred_label)
  ORDER BY
    COALESCE(sms.providerCreatedAt, sms.createdAt)) as message
FROM
  {{ source('commons', 'sms_message' )}} sms
LEFT JOIN
  {{ source('mrt_commons_docker', 'sms_session_metadata')}} td
ON
  sms.id=td.id
WHERE td.session_id IS NOT NULL
GROUP BY
  td.session_id,
  sms.contactNumber,
  td.session_created_at,
  sms.direction,
  sms.patientId,
  sms.userId,
  td.is_internal_contact_number,
  sms.provider,
  td.historical_state,
  td.coarse_historical_state,
  td.possible_convo_end,
  td.tdiff_mins,
  td.adjusted_tdiff_mins,
  TIME_ADD(TIME "00:00:00", INTERVAL cast((td.tdiff_mins*60) as INT64) SECOND)
ORDER BY td.session_created_at