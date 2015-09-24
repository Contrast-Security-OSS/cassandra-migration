CREATE TABLE contents (
  id bigint PRIMARY KEY,
  title text,
  message text,
  created timestamp
);

CREATE TABLE messages (
  id bigint PRIMARY KEY,
  contents_id bigint,
  type int,
  delivered timestamp,
  organization_id bigint,
  application_id text,
  first_name text,
  last_name text,
  email_address text
);

INSERT INTO contents (id, title, message) VALUES (1, 'foo', 'bar');

UPDATE contents SET message = 'meh' WHERE id = 1;