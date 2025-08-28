DROP TABLE IF EXISTS `thesis-25-456305.raw_layer.mdl_course_categories`;

CREATE TABLE `thesis-25-456305.raw_layer.mdl_course_categories` (
  id INT64,
  name STRING,
  idnumber STRING,
  description STRING,
  descriptionformat INT64,
  parent INT64,
  sortorder INT64,
  coursecount INT64,
  visible BOOL,
  visibleold BOOL,
  timemodified INT64,
  depth INT64,
  path STRING,
  theme STRING
);
