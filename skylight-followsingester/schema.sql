CREATE SCHEMA follows;

CREATE TABLE follows.cursor (
    cursor BIGINT NOT NULL
);
CREATE UNIQUE INDEX cursor_single ON follows.cursor ((0));

CREATE SEQUENCE follows.dids_id_seq
    AS INT
    START -2147483648
    MINVALUE -2147483648
    NO MAXVALUE;

CREATE TABLE follows.dids (
    id INT NOT NULL PRIMARY KEY DEFAULT nextval(
        'follows.dids_id_seq'::REGCLASS
    ),
    did TEXT NOT NULL
);
CREATE UNIQUE INDEX follows_dids_idx ON follows.dids (did);

CREATE TABLE follows.edges (
    actor_id INT NOT NULL,
    rkey TEXT NOT NULL,
    subject_id INT NOT NULL,
    PRIMARY KEY (actor_id, rkey)
);

CREATE INDEX edges_outgoing_idx ON follows.edges (actor_id, subject_id);
CREATE INDEX edges_incoming_idx ON follows.edges (subject_id, actor_id);
