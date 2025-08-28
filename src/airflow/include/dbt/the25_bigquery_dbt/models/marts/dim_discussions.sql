SELECT
    id AS discussion_id,
    course AS course_id,
    name AS discussion_name,
    userid AS creator_user_id,
    usermodified AS last_modified_by,
    timemodified AS last_modified_at
FROM `thesis-25-456305.staging_layer.stg_mdl_forum_discussions`