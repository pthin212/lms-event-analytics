{{ config(
    partition_by={
        "field": "timecreated",
        "data_type": "timestamp"
    }
) }}

SELECT
    eventname,
    objecttable,
    objectid,
    userid,
    courseid,
    timecreated
FROM `thesis-25-456305.raw_layer.moodle_events`