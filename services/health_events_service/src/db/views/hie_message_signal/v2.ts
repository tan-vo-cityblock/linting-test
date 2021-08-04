// update for each iteration
import {up as v1Down} from './v1';

export const up = `
DROP MATERIALIZED VIEW IF EXISTS hie_message_signal;

CREATE MATERIALIZED VIEW hie_message_signal AS (
  with hie_columns as (
    select
    hie_message."messageId",
    hie_message.payload::json -> 'Meta' -> 'Transmission' ->> 'ID' as "transmissionId",
    hie_message.payload::json -> 'Meta' ->> 'EventDateTime' as "receivedAt",
    hie_message."patientId",
    hie_message.payload::json -> 'Meta' -> 'Source' ->> 'Name' as "sourceName",
    hie_message."eventType" as "dataModel",
    (hie_message.payload::json -> 'Visit' ->> 'VisitDateTime')::timestamp with time zone as "eventDateTime",
    (hie_message.payload::json -> 'Visit' ->> 'VisitDateTime')::timestamp with time zone as "eventDateTimeInstant",
    (hie_message.payload::json -> 'Visit' ->> 'VisitDateTime')::date as "eventDateTimeDate",
    case
    	when hie_message.payload::json -> 'Meta' ->> 'EventType' = 'Discharge'
        then hie_message.payload::json -> 'Meta' ->> 'EventDateTime'
        else null
    end as "dischargeDateTimeRaw",
    coalesce(hie_message.payload::json -> 'Note' ->> 'FileContents', hie_message.payload::json -> 'Patient' ->> 'Notes') as "fullText",
    hie_message.payload::json -> 'Visit' ->> 'VisitNumber' as "visitId",
    case
    	when coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType')
    	in ('Arrival', 'LTC/Skilled Nursing Admit','OBS Admit')
    	then 'Admit'
        when coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType')
    	in ('LTC/Skilled Nursing Discharge', 'OBS Discharge','Discharge')
    	then 'Discharge'
    	when coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType')
    	in ('Transfer', 'Transfer to IP')
    	then 'Transfer'
        else coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value', hie_message.payload::json -> 'Meta' ->> 'EventType')
    end as "eventType",
    case
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('S')
        then 'Silent'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('N')
            or hie_message.payload::json -> 'Note' -> 'Components' ->> 'Value' in ('P')
            or hie_message.payload::json -> 'Visit' -> 'PatientClass' ->> 'Value' in ('Unknown')
        then 'Unknown'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('G')
        then 'General'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('T')
        	or coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType') in ('Transfer', 'Transfer to IP')
        then 'Transfer'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('O')
            or hie_message.payload::json -> 'Note' -> 'Components' -> 2 ->> 'Value' in ('O')
            or hie_message.payload::json -> 'Visit' ->> 'PatientClass' in ('Outpatient')
            or coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType') in ('Outpatient')
        then 'Outpatient'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('ED')
            or hie_message.payload::json -> 'Note' -> 'Components' -> 2 ->> 'Value' in ('ED')
            or hie_message.payload::json -> 'Visit' ->> 'PatientClass' in ('Emergency')
        then 'Emergency'
        when hie_message.payload::json -> 'Note' ->> 'DocumentType' in ('IA')
            or hie_message.payload::json -> 'Note' -> 'Components' -> 2 ->> 'Value' in ('IA')
            or hie_message.payload::json -> 'Visit' ->> 'PatientClass' in ('Inpatient')
        then 'Inpatient'
        when hie_message.payload::json -> 'Visit' ->> 'PatientClass' in ('Observation')
            or coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',
            hie_message.payload::json -> 'Meta' ->> 'EventType') like '%OBS%'
            then 'Observation'
        else 'Parse Error'
    end as "visitType",
    hie_message.payload::json -> 'Note' ->> 'DocumentType' as "documentType",
    case
       when coalesce(hie_message.payload::json -> 'Patient' -> 'Diagnoses' -> 0 ->> 'Code', hie_message.payload::json -> 'Note' -> 'Components' -> 3 ->> 'Value')
       in ('COVID-19 labOrder','ROUTINE','COVID-19 labResult','Cityblock CCD Push','chiefComplaint')
       then null
       when hie_message.payload::json -> 'Meta' -> 'Source' ->> 'Name' in ('PatientPing Source (p)','ACPNY ADT Source (p)')
       then replace(hie_message.payload::json -> 'Patient' -> 'Diagnoses' -> 0 ->> 'Code','.','')
       when hie_message.payload::json -> 'Meta' -> 'Source' ->> 'Name'  in ('Healthix HL7 Source (p)','CRISP [PROD] ADT Source (p)')
       then replace(hie_message.payload::json -> 'Note' -> 'Components' -> 3 ->> 'Value','.','')
       else null
    end as "code",
    case
       when hie_message.payload::json -> 'Patient' -> 'Diagnoses' -> 0 ->> 'Codeset' in ('ICD-10')
       then 'ICD-10-CM'
       else hie_message.payload::json -> 'Patient' -> 'Diagnoses' -> 0 ->> 'Codeset'
    end as "codeset",
    case
       when length(hie_message.payload::json -> 'Note' -> 'Provider' ->> 'ID') = 10
       then hie_message.payload::json -> 'Note' -> 'Provider' ->> 'ID'
       else null
    end as "providerNPI",
    initcap(hie_message.payload::json -> 'Note' -> 'Provider' ->> 'FirstName') as "providerFirstName",
    initcap(hie_message.payload::json -> 'Note' -> 'Provider' ->> 'LastName') as "providerLastName",
    hie_message.payload::json -> 'Note' -> 'Provider' ->> 'Credentials' as "providerCredentials",
    coalesce(hie_message.payload::json -> 'Note' ->> 'DocumentId',
        hie_message.payload::json -> 'Visit' -> 'Location' ->> 'Department',
        hie_message.payload::json -> 'Meta' ->> 'FacilityCode',
        hie_message.payload::json -> 'Visit' -> 'Extensions' -> 'admission-source' -> 'codeableConcept' ->> 'text') as "locationName",
    case
    	when hie_message.payload::json -> 'Visit' -> 'AttendingProvider' -> 'Location' ->> 'Type' is null
    	and coalesce(hie_message.payload::json -> 'Note' -> 'Components' -> 0 ->> 'Value',  hie_message.payload::json -> 'Meta' ->> 'EventType') like '%LTC/Skilled Nursing%'
    	then 'LTC/SNF'
    	else hie_message.payload::json -> 'Visit' -> 'AttendingProvider' -> 'Location' ->> 'Type'
    end as "facilityType"
    from hie_message
    where hie_message."eventType" in ('PatientAdmin.Discharge','PatientAdmin.Arrival','Notes.New','PatientAdmin.Transfer') --adding in transfers here
),

toc_flags as (

    select *,
      case
        when hie_columns."eventType"= 'Admit'
        and hie_columns."visitType" = 'Inpatient'
        then true
        else false
      end as "isInpatientAdmission",
      case
        when hie_columns."eventType"= 'Admit'
        and hie_columns."visitType" = 'Emergency'
        then true
        else false
      end as "isEdAdmission",
      case
        when hie_columns."eventType"= 'Admit'
        and hie_columns."visitType" = 'Inpatient'
        and date_part('day', current_timestamp - hie_columns."eventDateTime"::timestamp with time zone) between 3 and 20 --occurred within 3-20 days
        and lead(hie_columns."eventType") over (partition by hie_columns."patientId" order by hie_columns."eventDateTime") is null --and we don't see a subsequent ping
        then true
        else false
      end as "isCurrentlyHospitalized"
    from hie_columns
),

readmission as (

	select *,
      case
        when toc_flags."isInpatientAdmission" = TRUE --inpatient admit
        and date_part('day', toc_flags."eventDateTime"::timestamp with time zone - lag(toc_flags."eventDateTime"::timestamp with time zone, 1) over (partition by toc_flags."patientId"
        order by toc_flags."eventDateTime"::timestamp with time zone)) between 0 and 30 --within 30 days
        and lag(toc_flags."eventType", 1) over (partition by toc_flags."patientId" order by toc_flags."eventDateTime") = 'Discharge' --of discharge
        then true
        else false
      end as "isReadmission"
    from toc_flags

),

final as (

	select *
	from readmission
)

 select *
 from final

);

CREATE UNIQUE INDEX hie_message_id_index ON hie_message_signal("messageId");
`;

// update for each iteration
export const down = v1Down
