CREATE TABLE test (
  space text,
  key text,
  value text,
  PRIMARY KEY (space, key)
) with CLUSTERING ORDER BY (key ASC);