SELECT
    id,
    course,
    name,
    userid,
    TIMESTAMP_SECONDS(timemodified) AS timemodified,
    usermodified
FROM `thesis-25-456305.raw_layer.mdl_forum_discussions`