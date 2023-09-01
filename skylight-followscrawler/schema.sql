CREATE SCHEMA followscrawler;

CREATE TABLE followscrawler.cursor (
    cursor TEXT NOT NULL
);
CREATE UNIQUE INDEX cursor_single ON followscrawler.cursor ((0));

CREATE TABLE followscrawler.pending (
    did TEXT PRIMARY KEY
);

CREATE TABLE followscrawler.errors (
    did TEXT PRIMARY KEY,
    why TEXT NOT NULL,
    ts TIMESTAMPTZ DEFAULT NOW()
);
