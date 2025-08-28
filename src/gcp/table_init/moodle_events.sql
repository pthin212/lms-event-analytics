DROP TABLE IF EXISTS `thesis-25-456305.raw_layer.moodle_events`;

CREATE TABLE `thesis-25-456305.raw_layer.moodle_events` (
  eventname STRING,
  component STRING,
  action STRING,
  target STRING,
  objecttable STRING,
  objectid INT64,
  crud STRING,
  edulevel INT64,
  contextid INT64,
  contextlevel INT64,
  contextinstanceid INT64,
  userid INT64,
  courseid INT64,
  relateduserid INT64,
  anonymous BOOL,
  other STRUCT<
    username STRING,
    sessionid STRING
  >,
  timecreated TIMESTAMP,
  host STRING,
  extra STRING
);