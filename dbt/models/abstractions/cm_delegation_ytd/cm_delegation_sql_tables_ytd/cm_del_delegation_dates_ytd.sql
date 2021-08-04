with patient_list as (
    SELECT
        pt.id as patientid,
        partnerId,
        Partner_List,
        delegation_at
    FROM {{ source('commons', 'patient') }} pt
    LEFT JOIN  {{ source('commons', 'partner') }} pr
        ON pt.partnerId = pr.id
    INNER join {{ ref('delegation_dates') }} dd
        ON pr.name = dd.Partner_List
)
select * from patient_list
