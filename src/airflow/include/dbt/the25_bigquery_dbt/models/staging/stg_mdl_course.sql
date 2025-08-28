SELECT
    id,
    category,
    shortname,
    fullname,
    format,
    TIMESTAMP_SECONDS(startdate) AS startdate,
    TIMESTAMP_SECONDS(enddate) AS enddate,
    visible,
    TIMESTAMP_SECONDS(timecreated) AS timecreated,
    TIMESTAMP_SECONDS(timemodified) AS timemodified
FROM `thesis-25-456305.raw_layer.mdl_course`