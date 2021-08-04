WITH vaccines AS (
    SELECT 
        JSON_EXTRACT_SCALAR(payload,'$.Meta.Message.ID') as messageID,
        JSON_EXTRACT_ARRAY(payload, '$.Immunizations.') as immunizations
    FROM {{ source('streaming','redox_messages') }}
    where (patient.source.name = 'acpny' or patient.source.name = 'ACPNY') and eventType = 'PatientQueryResponse'

),

 ancillary as (
SELECT 
    JSON_EXTRACT_SCALAR(payload,'$.Meta.Message.ID') as messageID,
    JSON_EXTRACT_SCALAR(payload,'$.Header.Patient.Identifiers.0.ID') as patientIdentifier,
    JSON_EXTRACT_SCALAR(payload,'$.Header.Patient.Demographics.FirstName') as patientFirstName,
    JSON_EXTRACT_SCALAR(payload,'$.Header.Patient.Demographics.LastName') as patientLastName,
    JSON_EXTRACT_SCALAR(payload,'$.Header.Patient.Demographics.DOB') as patientDOB
FROM {{ source('streaming','redox_messages') }}
    where (patient.source.name = 'acpny' or patient.source.name = 'ACPNY') and eventType = 'PatientQueryResponse'
)

SELECT  a.*, 
        JSON_EXTRACT_SCALAR(immunizations, "$.DateTime") dateTime,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.LotNumber") lotNumber,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.Manufacturer") manufacturer,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.CodySystemName") codeSystemName,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.CodeSystem") codeSystem,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.Name") name,
        JSON_EXTRACT_SCALAR(immunizations, "$.Product.Code") code,
        JSON_EXTRACT_SCALAR(immunizations, "$.IsPRN") isPRN,
        JSON_EXTRACT_SCALAR(immunizations, "$.Frequency.Unit") freqUnit,
        JSON_EXTRACT_SCALAR(immunizations, "$.Frequency.Period") freqPeriod,
        JSON_EXTRACT_SCALAR(immunizations, "$.Route.CodeSystemName") routeCodeSystemName,
        JSON_EXTRACT_SCALAR(immunizations, "$.Route.CodeSystem") routeCodeSystem,
        JSON_EXTRACT_SCALAR(immunizations, "$.Route.Name") routeName,
        JSON_EXTRACT_SCALAR(immunizations, "$.Route.Code") routeCode,
        JSON_EXTRACT_SCALAR(immunizations, "$.Rate.Units") rateUnits,
        JSON_EXTRACT_SCALAR(immunizations, "$.Rate.Quantity") rateQty,
        JSON_EXTRACT_SCALAR(immunizations, "$.Dose.Units") doseUnits,
        JSON_EXTRACT_SCALAR(immunizations, "$.Dose.Quantity") doseQty
FROM vaccines v, UNNEST(immunizations) immunizations
LEFT JOIN ancillary a
   ON v.messageID = a.messageID 