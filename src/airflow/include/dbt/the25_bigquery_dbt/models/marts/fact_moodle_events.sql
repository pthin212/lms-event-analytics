{{ config(
    materialized='incremental',
    unique_key='moodle_event_id',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp"
    },
    cluster_by=["eventname", "user_id", "course_id"]
) }}

SELECT
    GENERATE_UUID() AS moodle_event_id,
    CAST(e.timecreated AS DATE) AS date_key,
    e.userid AS user_id,
    e.courseid AS course_id,

    CASE
    WHEN e.objecttable = 'forum_discussions' THEN e.objectid
    ELSE NULL
    END AS discussion_id,

    cat.id AS category_id,
    e.eventname,
    e.timecreated AS created_at
FROM `thesis-25-456305.staging_layer.stg_moodle_events` e
LEFT JOIN `thesis-25-456305.staging_layer.stg_mdl_course` c
    ON e.courseid = c.id
LEFT JOIN `thesis-25-456305.staging_layer.stg_mdl_course_categories` cat
    ON c.category = cat.id

{% if is_incremental() %}
  WHERE e.timecreated > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}