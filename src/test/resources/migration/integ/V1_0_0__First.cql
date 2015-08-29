CREATE TABLE test1 (
  space text,
  key text,
  value text,
  PRIMARY KEY (space, key)
) with CLUSTERING ORDER BY (key ASC);

INSERT INTO test1 (space, key, value) VALUES ('foo', 'blah', 'meh');

UPDATE test1 SET value = 'profit!' WHERE space = 'foo' AND key = 'blah';