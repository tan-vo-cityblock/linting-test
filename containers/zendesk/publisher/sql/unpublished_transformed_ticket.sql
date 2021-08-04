WITH joined_ticket_fields AS(
    SELECT
        ticket.id,
        ARRAY_AGG(
            STRUCT(
                ticket_field.id,
                ticket_field.title,
                ticket_field.type,
                ticket_field.description,
                ticket_field.agent_description,
                custom_field.value
            )
        ) AS custom_ticket_fields
    FROM
        `{zendesk_project_id}.{zendesk_dataset_id}.ticket_latest` AS ticket,
        UNNEST(ticket.custom_fields) AS custom_field
        LEFT JOIN `{zendesk_project_id}.{zendesk_dataset_id}.ticket_field_latest` AS ticket_field ON custom_field.id = ticket_field.id
    WHERE
        custom_field.value IS NOT NULL
    GROUP BY
        ticket.id
),
joined_ticket AS (
    SELECT
        ticket.*,
        joined_ticket_fields.custom_ticket_fields,
        ticket_form.display_name AS ticket_form_display_name,
        org.name AS organization_name,
        grp.name AS group_name,
        brand.name AS brand_name
    FROM
        `{zendesk_project_id}.{zendesk_dataset_id}.ticket_latest` AS ticket
        LEFT JOIN joined_ticket_fields ON ticket.id = joined_ticket_fields.id
        LEFT JOIN `{zendesk_project_id}.{zendesk_dataset_id}.ticket_form_latest` AS ticket_form ON ticket.ticket_form_id = ticket_form.id
        LEFT JOIN `{zendesk_project_id}.{zendesk_dataset_id}.organization_latest` AS org ON ticket.organization_id = org.id
        LEFT JOIN `{zendesk_project_id}.{zendesk_dataset_id}.group_latest` AS grp ON ticket.group_id = grp.id
        LEFT JOIN `{zendesk_project_id}.{zendesk_dataset_id}.brand_latest` AS brand ON ticket.brand_id = brand.id
),
cleaned_ticket AS (
    SELECT
        *
    EXCEPT
        (custom_fields, FIELDS)
    FROM
        joined_ticket
    WHERE
        STATUS != "deleted"
)
SELECT
    ticket.*
FROM
    cleaned_ticket AS ticket
    LEFT JOIN `{pubsub_project_id}.{pubsub_dataset_id}.zendesk_ticket` AS sent ON ticket.id = CAST(
        JSON_EXTRACT_SCALAR(sent.attributes, '$.{attribute_key_id}') AS INT64
    )
    AND ticket.updated_at = PARSE_TIMESTAMP(
        '%Y-%m-%dT%H:%M:%S%Ez',
        JSON_EXTRACT_SCALAR(sent.attributes, '$.{attribute_key_updated_at}')
    )
WHERE
    sent.payload IS NULL
