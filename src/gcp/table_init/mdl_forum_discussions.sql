DROP TABLE IF EXISTS `thesis-25-456305.raw_layer.mdl_forum_discussions`;

CREATE TABLE `thesis-25-456305.raw_layer.mdl_forum_discussions` (
  id INT64,
  course INT64,
  forum INT64,
  name STRING,
  firstpost INT64,
  userid INT64,
  groupid INT64,
  assessed BOOL,
  timemodified INT64,
  usermodified INT64,
  timestart INT64,
  timeend INT64,
  pinned BOOL,
  timelocked INT64
);
