SELECT
    id,
    name,
    parent,
    coursecount,
    visible,
    TIMESTAMP_SECONDS(timemodified) AS timemodified
FROM `thesis-25-456305.raw_layer.mdl_course_categories`