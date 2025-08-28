{{ config(
    materialized='incremental',
    unique_key='stripe_event_id',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp"
    },
    cluster_by=["user_id", "course_id"]
) }}
SELECT
    s.id AS stripe_event_id,
    CAST(s.created AS DATE) AS date_key,

    CAST(s.metadata_userid AS INT64) AS user_id,
    e.courseid AS course_id,
    c.category AS category_id,

    s.amount,
    s.amount_captured,
    s.amount_refunded,
    s.currency,

    s.billing_country,
    s.billing_email,
    s.billing_name,
    s.billing_phone,

    s.card_brand,
    s.card_last4,
    s.card_country,
    s.card_funding,

    s.receipt_url,
    s.created AS created_at
FROM `thesis-25-456305.staging_layer.stg_stripe_events` s
LEFT JOIN `thesis-25-456305.staging_layer.stg_mdl_enrol` e
    ON CAST(s.metadata_itemid AS INT64) = e.id
LEFT JOIN `thesis-25-456305.staging_layer.stg_mdl_course` c
    ON e.courseid = c.id
WHERE s.paid = true

{% if is_incremental() %}
  AND s.created > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}