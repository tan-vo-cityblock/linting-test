WITH

mrn_table as (
  SELECT
    m.id AS memberId,
    mrn.name AS datasourceName,
    mrn.mrn AS externalId
  FROM
    `cbh-db-mirror-prod.member_index_mirror.member` m
  INNER JOIN
    `cbh-db-mirror-prod.member_index_mirror.mrn` mrn
  ON
    mrn.id = m.mrnId
),

visit_note_doc_tagged AS
    (SELECT visit_note.patient_id,
            visit_note.signed_by_user_id,
            visit_note.signed_time,
            visit_note.document_date,
            doc_tag.description,
            doc_tag.code_type,
            doc_tag.code
     FROM `cbh-db-mirror-prod.elation_mirror.visit_note_document_tag_latest` AS visit_note_doc_tag
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.visit_note_latest` AS visit_note ON visit_note.id = visit_note_doc_tag.visit_note_id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.document_tag_latest` AS doc_tag ON visit_note_doc_tag.document_tag_id = doc_tag.id
     WHERE visit_note.deletion_time IS NULL ),
     non_visit_note_doc_tagged AS
    (SELECT non_visit_note.patient_id,
            non_visit_note.signed_by_user_id,
            non_visit_note.signed_time,
            non_visit_note.document_date,
            doc_tag.description,
            doc_tag.code_type,
            doc_tag.code
     FROM `cbh-db-mirror-prod.elation_mirror.non_visit_note_document_tag_latest` AS non_visit_note_doc_tag
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.document_tag_latest` AS doc_tag ON non_visit_note_doc_tag.document_tag_id = doc_tag.id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.non_visit_note_latest` AS non_visit_note ON non_visit_note.id = non_visit_note_doc_tag.non_visit_note_id
     WHERE non_visit_note.deletion_time IS NULL),
     lab_doc_tagged AS
    (SELECT lab.patient_id,
            lab.signed_by_user_id,
            lab.signed_time,
            lab.document_date,
            doc_tag.description,
            doc_tag.code_type,
            doc_tag.code
     FROM `cbh-db-mirror-prod.elation_mirror.lab_report_document_tag_latest` AS lab_doc_tag
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.lab_report_latest` AS lab ON lab.id = lab_doc_tag.lab_report_id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.document_tag_latest` AS doc_tag ON lab_doc_tag.document_tag_id = doc_tag.id
     WHERE lab.deletion_time IS NULL ),
     bill_item_visit_note AS
    (SELECT bill.visit_note_id,
            bill_item.cpt,
            bill_item.id AS bill_item_id
     FROM `cbh-db-mirror-prod.elation_mirror.bill_item_latest` AS bill_item
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.bill_latest` AS bill ON bill_item.bill_id = bill.id
     WHERE bill_item.deletion_time IS NULL),
     doc_tag_code_descriptions AS
    (SELECT code,
            description,
            RANK() OVER(PARTITION BY code
                        ORDER BY LENGTH(description) DESC) AS pos
     FROM `cbh-db-mirror-prod.elation_mirror.document_tag_latest`),
     bill_cpt_tagged AS
    (SELECT visit_note.patient_id,
            visit_note.signed_by_user_id,
            visit_note.signed_time,
            visit_note.document_date,
            doc_tag.description AS description,
            ("CPT") AS code_type,
            bill_item_visit_note.cpt AS code
     FROM bill_item_visit_note
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.visit_note_latest` AS visit_note ON bill_item_visit_note.visit_note_id = visit_note.id
     LEFT JOIN
         (SELECT DISTINCT *
          FROM doc_tag_code_descriptions
          WHERE pos = 1) AS doc_tag ON bill_item_visit_note.cpt = doc_tag.code
     WHERE visit_note.deletion_time IS NULL ),
     bill_icd10_tagged AS
    (SELECT visit_note.patient_id,
            visit_note.signed_by_user_id,
            visit_note.signed_time,
            visit_note.document_date,
            icd10.description,
            ("ICD10") AS code_type,
            icd10.code
     FROM bill_item_visit_note
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.bill_item_dx_latest` AS bill_item_dx ON bill_item_dx.bill_item_id = bill_item_visit_note.bill_item_id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.icd10_latest` AS icd10 ON bill_item_dx.icd10_id = icd10.id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.visit_note_latest` AS visit_note ON bill_item_visit_note.visit_note_id = visit_note.id
     WHERE visit_note.deletion_time IS NULL ),
     bill_icd9_tagged AS
    (SELECT visit_note.patient_id,
            visit_note.signed_by_user_id,
            visit_note.signed_time,
            visit_note.document_date,
            icd9.description,
            ("ICD9") AS code_type,
            icd9.code
     FROM bill_item_visit_note
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.bill_item_dx_latest` AS bill_item_dx ON bill_item_dx.bill_item_id = bill_item_visit_note.bill_item_id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.icd9_latest` AS icd9 ON bill_item_dx.icd9_id = icd9.id
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.visit_note_latest` AS visit_note ON bill_item_visit_note.visit_note_id = visit_note.id
     WHERE visit_note.deletion_time IS NULL ),
     elation_all_tagged AS
    (SELECT *
     FROM visit_note_doc_tagged
     UNION ALL SELECT *
     FROM non_visit_note_doc_tagged
     UNION ALL SELECT *
     FROM lab_doc_tagged
     UNION ALL SELECT *
     FROM bill_cpt_tagged
     UNION ALL SELECT *
     FROM bill_icd10_tagged
     UNION ALL SELECT *
     FROM bill_icd9_tagged),
     elation_all_tagged_mapped_to_cbh AS
    (SELECT elation_all_tagged.*,
            commmons_user.id AS userId,
            members.memberId
     FROM elation_all_tagged
     LEFT JOIN `cbh-db-mirror-prod.elation_mirror.user_latest` AS elation_user ON signed_by_user_id = elation_user.id
     LEFT JOIN `cbh-db-mirror-prod.commons_mirror.user` AS commmons_user ON REGEXP_REPLACE(elation_user.email, r"\+.*@", "@") = commmons_user.email
     LEFT JOIN mrn_table AS memberS ON CAST(patient_id AS STRING) = members.externalId)
SELECT memberId,
       userId,
       document_date AS setOn,
       code_type AS codeType,
       code,
       description AS reason
FROM elation_all_tagged_mapped_to_cbh
WHERE signed_time IS NOT NULL
    AND memberId IS NOT NULL
    AND code IS NOT NULL
    AND TRIM(code) != ""
