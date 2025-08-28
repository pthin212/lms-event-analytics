SELECT
    id AS user_id,
    username,
    firstname,
    lastname,
    email,
    phone1,
    institution,
    city,
    country,
    firstaccess,
    lastlogin,
    timecreated,
    timemodified,
    DATE(lastlogin) AS lastlogin_date
FROM `thesis-25-456305.staging_layer.stg_mdl_user`
WHERE deleted = 0