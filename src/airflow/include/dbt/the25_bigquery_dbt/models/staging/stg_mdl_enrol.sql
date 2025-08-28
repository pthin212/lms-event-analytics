SELECT
    id,
    enrol,
    status,
    courseid,
    name,
    enrolperiod,
    enrolstartdate,
    enrolenddate,
    cost,
    currency,
    TIMESTAMP_SECONDS(timecreated) AS timecreated,
    TIMESTAMP_SECONDS(timemodified) AS timemodified
FROM `thesis-25-456305.raw_layer.mdl_enrol`