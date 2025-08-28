SELECT
    id AS course_id,
    shortname,
    fullname,
    format,
    startdate,
    enddate,
    visible,
FROM `thesis-25-456305.staging_layer.stg_mdl_course`
WHERE format = 'topics'
