-- RUN COMMAND: ENCOUNTER_TRANSFORM_PROJECT=cityblock-data ENCOUNTER_TRANSFORM_DATASET=dev_medical CITYBLOCK_ANALYTICS=cityblock-analytics dbt run --model transforms.patient_encounters
{{ config(
  materialized = 'table',
  udf='''
CREATE TEMP FUNCTION makeIdentifier(ID STRING, IDType STRING)
RETURNS STRING
LANGUAGE js AS """
  if(!ID && !IDType){
        return null
    }else{
        return JSON.stringify({"id": ID, "idType": IDType})
    }
""";


CREATE TEMP FUNCTION makeProviderIdentifier(ID STRING, IDType STRING)
RETURNS ARRAY<STRUCT<id STRING, idType STRING>>
LANGUAGE js AS """
  if(!ID && !IDType){
        return []
    }else{
        return [{"id": ID, "idType": IDType}]
    }
""";

CREATE TEMP FUNCTION makeProviderObj(transformedProvider STRUCT<
                                        identifiers ARRAY<STRUCT<id STRING, idType STRING>>,
                                        firstName STRING,
                                        lastName STRING,
                                        credentials ARRAY<STRING>,
                                        address STRUCT<street1 STRING, street2 STRING, city STRING, state STRING, zip STRING>,
                                        phone STRING,
                                        role STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>
                                      >)
RETURNS STRING
LANGUAGE js AS """
  return JSON.stringify(transformedProvider)
""";

CREATE TEMP FUNCTION convertToProviderStructs(arr ARRAY<STRING>)
RETURNS ARRAY<STRUCT<
                                        identifiers ARRAY<STRUCT<id STRING, idType STRING>>,
                                        firstName STRING,
                                        lastName STRING,
                                        credentials ARRAY<STRING>,
                                        address STRUCT<street1 STRING, street2 STRING, city STRING, state STRING, zip STRING>,
                                        phone STRING,
                                        role STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>
                                      >>
LANGUAGE js AS """
  if(arr){
    return arr.map(strObj => JSON.parse(strObj))
  }else{
    return []
  }
""";


CREATE TEMP FUNCTION makeLocationObj(redoxLoc STRUCT<
Address STRUCT<StreetAddress STRING, City STRING, State STRING, Country STRING, ZIP STRING>,
Type STRUCT<Code STRING, CodeSystem STRING, CodeSystemName STRING, Name STRING>,
Name STRING
>)
RETURNS STRING
LANGUAGE js AS """
    let address = null
    const redoxAddress = redoxLoc.Address
    if (redoxAddress.StreetAddress || redoxAddress.City || redoxAddress.State || redoxAddress.ZIP){
        address = {
            "street1": redoxAddress.StreetAddress,
            "street2": null,
            "city": redoxAddress.City,
            "state": redoxAddress.State,
            "zip": redoxAddress.ZIP
        }
    }
    let locType = null
    const redoxLocType = redoxLoc.Type
    if(redoxLocType.Code || redoxLocType.CodeSystem || redoxLocType.CodeSystemName || redoxLocType.Name){
        locType = {
        "code": redoxLocType.Code,
        "codeSystem": redoxLocType.CodeSystem,
        "codeSystemName": redoxLocType.CodeSystemName,
        "name": redoxLocType.Name
        }
    }
    if(!address && !locType && !redoxLoc.Name){
        return null
    }else{
        return JSON.stringify({
            "address": address,
            "type": locType,
            "name": redoxLoc.Name
        })
    }
""";

CREATE TEMP FUNCTION convertToLocationStructs(arr ARRAY<STRING>)
RETURNS ARRAY<STRUCT<
                     address STRUCT<street1 STRING, street2 STRING, city STRING, state STRING, zip STRING>,
                     type STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>,
                     name STRING
                     >>
LANGUAGE js AS """
  if(arr){
    return arr.map(strObj => JSON.parse(strObj))
  }else{
    return []
  }
""";

CREATE TEMP FUNCTION makeEncounterCodeObj(redoxObj STRUCT<Code STRING, CodeSystem STRING, CodeSystemName STRING, Name STRING>)
RETURNS STRING
LANGUAGE js AS """
  if(!redoxObj.Code && !redoxObj.CodeSystem && !redoxObj.CodeSystemName && !redoxObj.Name){
        return null
    }else{
        return JSON.stringify ({
         "code": redoxObj.Code,
         "codeSystem": redoxObj.CodeSystem,
         "codeSystemName": redoxObj.CodeSystemName,
         "name": redoxObj.Name
         })
    }
""";

CREATE TEMP FUNCTION makeEncounterReason(source String, encounterType STRING, reasonForVisit ARRAY<STRUCT<Code STRING, CodeSystem STRING, CodeSystemName STRING, Name STRING>>)
RETURNS ARRAY<STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING, notes STRING>>
LANGUAGE js AS """
    function transformRedoxReason(reason){
        if(!reason.Code && !reason.CodeSystem && !reason.CodeSystemName && !reason.Name){
            return null;
         }else{
             return {
                "code": reason.Code,
                "codeSystem": reason.CodeSystem,
                "codeSystemName": reason.CodeSystemName,
                "name": reason.Name,
                "notes": null
              };
         }
    }
    function transformNonVisitElationReason(reason){
        if(!reason.Code && !reason.CodeSystem && !reason.CodeSystemName && !reason.Name){
            return null;
         }else{
             return {
                "code": reason.Code,
                "codeSystem": reason.CodeSystem,
                "codeSystemName": reason.CodeSystemName,
                "name": null,
                "notes": reason.Name
              };
         }
    }
    function transformVisitElationReason(reason){
        const oldReason = reason.Name;
        let newReason = {
            "code": null,
            "codeSystem": null,
            "codeSystemName": null,
            "name": null,
            "notes": null
        };
        const reasonReg1 = new RegExp("(?<=Reason:\\\\n)((?:(?!\\\\n).)*)", "g");
        const notesReg1 = new RegExp("Reason:\\\\n.*?(?:\\\\n\\\\n|$)", "g");
        const reasonReg2 = new RegExp("(?<=Category: Reason - Text: )((?:(?!\\\\n).)*)", "g");
        const notesReg2 = new RegExp("Category: Reason - Text: .*?(?:\\\\n|$)", "g");
        const reasonReg3 = new RegExp("(?<=Reason - )((?:(?!\\\\n).)*)", "g");
        const notesReg3 = new RegExp("Reason - .*?(?:\\\\n|$)", "g");
        const reason1 = oldReason.match(reasonReg1);
        const reason2 = oldReason.match(reasonReg2);
        const reason3 = oldReason.match(reasonReg3);
        const clearReason1 = oldReason.match(notesReg1);
        const clearReason2 = oldReason.match(notesReg2);
        const clearReason3 = oldReason.match(notesReg3);
        if (reason1){
            newReason.name = reason1;
            newReason.notes = oldReason.replace(clearReason1, ``);
            return newReason;
        }else if(reason2){
           newReason.name = reason2;
           newReason.notes = oldReason.replace(clearReason2, ``);
           return newReason;
        } else if(reason3) {
            newReason.name = reason3;
            newReason.notes = oldReason.replace(clearReason3, ``);
            return newReason;
        }else{
            newReason.notes = oldReason
            return newReason;
        }
    }
    const elation = "elation";
    const nonvisit = "nonvisit";
    let newReasons = [];
    let reasonsLength = reasonForVisit.length;
    let elationVisitNotes = "";
    for (let i = 0; i < reasonsLength; i++) {
        const reason = reasonForVisit[i]
        if(source != elation){
            const newReason = transformRedoxReason(reason)
            if (newReason){
                newReasons.push(newReason);
            }
        }
        else if(source == elation && encounterType == nonvisit){
            const newReason = transformNonVisitElationReason(reason);
            if (newReason){
                newReasons.push(newReason);
            }
        }
        else if(source == elation && encounterType != nonvisit){
            newReasons.push(transformVisitElationReason(reason));
        }
    }
    if(source == elation && encounterType != nonvisit && newReasons.length > 1){
        let reasonName = null;
        let reasonNotes = "";
        for (let i = 0; i < newReasons.length; i++) {
            const reason = newReasons[i];
            if(!reasonName){
                reasonName = reason.name;
            }
            if(reasonNotes == "" && reason.notes){
                reasonNotes = reason.notes;
            }else if(reasonNotes != "" && reason.notes){
                reasonNotes = reasonNotes + "\\\\n\\\\n" + reason.notes;
            }
        }
        return [{
            "code": null,
            "codeSystem": null,
            "codeSystemName": null,
            "name": reasonName,
            "notes": reasonNotes
        }];
    }else{
        return newReasons;
    }
""";

CREATE TEMP FUNCTION convertToIdStructs(arr ARRAY<STRING>)
RETURNS ARRAY<STRUCT<id STRING, idType STRING>>
LANGUAGE js AS """
  if(arr){
    return arr.map(strObj => JSON.parse(strObj))
  }else{
    return []
  }
""";

CREATE TEMP FUNCTION convertToObjStruct(obj STRING)
RETURNS STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>
LANGUAGE js AS """
  return JSON.parse(obj)
""";

CREATE TEMP FUNCTION convertToObjsStructs(arr ARRAY<STRING>)
RETURNS ARRAY<STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>>
LANGUAGE js AS """
  if(arr){
    return arr.map(strObj => JSON.parse(strObj))
  }else{
    return []
  }
""";

CREATE TEMP FUNCTION convertToReasonStructs(arr ARRAY<STRING>)
RETURNS ARRAY<STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING, notes STRING>>
LANGUAGE js AS """
  if(arr){
    return arr.map(strObj => JSON.parse(strObj))
  }else{
    return []
  }
""";

  ''',

  table_schema = '''
(
   messageId STRING NOT NULL,
   patient STRUCT<
     patientId STRING,
     externalId STRING NOT NULL,
     source STRUCT<
       name STRING NOT NULL,
       commonId STRING
     > NOT NULL
   > NOT NULL,
   encounter STRUCT<
     identifiers ARRAY<
       STRUCT<
         id STRING,
         idType STRING
       >
     >,
     type STRUCT<
       code STRING NOT NULL,
       codeSystem STRING,
       codeSystemName STRING,
       name STRING
     > NOT NULL,
     dateTime STRUCT<
       raw STRING NOT NULL,
       instant TIMESTAMP,
       `date` DATE
     >,
     endDateTime STRUCT<
       raw STRING NOT NULL,
       instant TIMESTAMP,
       `date` DATE
     >,
     providers ARRAY<
       STRUCT<
         identifiers ARRAY<
           STRUCT<
             id STRING,
             idType STRING
           >
         >,
         firstName STRING,
         lastName STRING,
         credentials ARRAY<STRING>,
         address STRUCT<
           street1 STRING,
           street2 STRING,
           city STRING,
           state STRING,
           zip STRING
         > NOT NULL,
         phone STRING,
         role STRUCT<
           code STRING,
           codeSystem STRING,
           codeSystemName STRING,
           name STRING
         >
       >
     >,
     locations ARRAY<
       STRUCT<
         address STRUCT<
           street1 STRING,
           street2 STRING,
           city STRING,
           state STRING,
           zip STRING
         >,
         type STRUCT<
           code STRING,
           codeSystem STRING,
           codeSystemName STRING,
           name STRING
         >,
         name STRING
       >
     >,
     diagnoses ARRAY<
       STRUCT<
         code STRING,
         codeSystem STRING,
         codeSystemName STRING,
         name STRING
       >
     >,
     reasons ARRAY<
       STRUCT<
         code STRING,
         codeSystem STRING,
         codeSystemName STRING,
         name STRING,
         notes STRING
       >
     >,
     draft BOOLEAN NOT NULL
   > NOT NULL
 )
  '''
)}}

WITH
  uuid_encounters AS (SELECT GENERATE_UUID() AS uuid, * FROM {{ source('medical', 'patient_encounters') }}),
  final_encounters AS (SELECT messageId,
          ANY_VALUE(patient) AS patient,
          STRUCT<
                 identifiers ARRAY<STRUCT<id STRING, idType STRING>>,
                 type STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>,
                 dateTime STRUCT<raw STRING, instant TIMESTAMP, dateT DATE>,
                 endDateTime STRUCT<raw STRING, instant TIMESTAMP, dateT DATE>,
                 providers ARRAY<STRUCT<
                                        identifiers ARRAY<STRUCT<id STRING, idType STRING>>,
                                        firstName STRING,
                                        lastName STRING,
                                        credentials ARRAY<STRING>,
                                        address STRUCT<street1 STRING, street2 STRING, city STRING, state STRING, zip STRING>,
                                        phone STRING,
                                        role STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>
                                >>,
                 locations ARRAY<STRUCT<
                                        address STRUCT<street1 STRING, street2 STRING, city STRING, state STRING, zip STRING>,
                                        type STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>,
                                        name STRING
                                        >>,
                 diagnoses ARRAY<STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING>>,
                 reasons ARRAY<STRUCT<code STRING, codeSystem STRING, codeSystemName STRING, name STRING, notes String>>,
                 draft BOOLEAN
                 >
                 (
                  ARRAY_CONCAT_AGG((SELECT convertToIdStructs(ARRAY_AGG(DISTINCT makeIdentifier(ID, IDType) IGNORE NULLS)) FROM UNNEST(encounter.Identifiers))),
                  ANY_VALUE(encounter.Type),
                  ANY_VALUE(encounter.DateTime),
                  ANY_VALUE(encounter.EndDateTime),
                  ARRAY_CONCAT_AGG((SELECT
                  convertToProviderStructs(ARRAY_AGG(DISTINCT makeProviderObj(
                            (makeProviderIdentifier(Provider.ID, Provider.IDType),
                            Provider.FirstName,
                            Provider.LastName,
                            ARRAY<STRING>[],
                            (Provider.Address.StreetAddress, CAST(NULL AS STRING), Provider.Address.City, Provider.Address.State, Provider.Address.ZIP),
                            Provider.PhoneNumber.Office,
                            convertToObjStruct(makeEncounterCodeObj(Provider.Role))))
                            IGNORE NULLS
                            ))
                            FROM UNNEST(encounter.Providers) AS Provider)),
                  ARRAY_CONCAT_AGG((SELECT convertToLocationStructs(ARRAY_AGG(DISTINCT makeLocationObj(Location) IGNORE NULLS)) FROM UNNEST(encounter.Locations) AS Location)),

                  ARRAY_CONCAT_AGG((SELECT convertToObjsStructs(ARRAY_AGG(DISTINCT makeEncounterCodeObj(Diagnosis) IGNORE NULLS)) FROM UNNEST(encounter.Diagnosis) AS Diagnosis)),
                  ARRAY_CONCAT_AGG(makeEncounterReason(patient.source.name, encounter.Type.Name, encounter.ReasonForVisit)),
                  ANY_VALUE(encounter.EndDateTime IS NULL AND patient.source.name = "elation")
                  ) AS encounter
          FROM uuid_encounters
               GROUP BY uuid, messageId
        )

SELECT * FROM final_encounters
