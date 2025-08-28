SELECT
    id AS category_id,
    name AS category_name,
    parent,
    coursecount,
    visible
FROM `thesis-25-456305.staging_layer.stg_mdl_course_categories`