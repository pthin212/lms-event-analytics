SELECT
    id,
    auth,
    deleted,
    username,
    firstname,
    lastname,
    email,
    emailstop,
    phone1,
    phone2,
    institution,
    city,
    country,
    TIMESTAMP_SECONDS(firstaccess) AS firstaccess,
    TIMESTAMP_SECONDS(lastlogin) AS lastlogin,
    TIMESTAMP_SECONDS(timecreated) AS timecreated,
    TIMESTAMP_SECONDS(timemodified) AS timemodified
FROM `thesis-25-456305.raw_layer.mdl_user`
