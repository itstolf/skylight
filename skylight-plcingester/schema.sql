CREATE SCHEMA plc;

CREATE TABLE plc.dids (
    did TEXT NOT NULL,
    also_known_as TEXT [] NOT NULL,
    PRIMARY KEY (did)
);

CREATE INDEX dids_also_known_as_idx ON plc.dids USING gin (also_known_as);

CREATE TABLE plc.cursor (
    cursor TEXT NOT NULL
);
CREATE UNIQUE INDEX cursor_single ON plc.cursor ((0));
