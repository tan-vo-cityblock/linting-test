SELECT table_id,
    DATE(TIMESTAMP_MILLIS(creation_time)) AS creation_date,
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_date,
    row_count,
    size_bytes,
    CASE
        WHEN type = 1 THEN 'table'
        WHEN type = 2 THEN 'view'
        WHEN type = 3 THEN 'external'
        ELSE '?'
    END AS type,
    TIMESTAMP_MILLIS(creation_time) AS creation_time,
    TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,
    dataset_id,
    project_id
FROM {{ source('commons', '__TABLES__') }}
WHERE row_count >0
