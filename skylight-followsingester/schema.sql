CREATE SCHEMA follows;

CREATE TABLE follows.cursor (
    cursor BIGINT NOT NULL
);
CREATE UNIQUE INDEX cursor_single ON follows.cursor ((0));

CREATE SEQUENCE follows.dids_id_seq
AS INT -- noqa: PRS
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

CREATE TYPE follows.neighborhood_entry AS (
    actor_id INT,
    subject_ids INT []
);

CREATE OR REPLACE FUNCTION follows.neighborhood(
    ids INT [],
    ignore_ids INT []
) RETURNS SETOF follows.neighborhood_entry AS $$
if not ids:
    return []

import functools

mutuals_plan = plpy.prepare("""
    SELECT i.subject_id id
    FROM follows.edges i
    INNER JOIN follows.edges o ON
        i.actor_id = o.subject_id AND
        i.subject_id = o.actor_id
    WHERE
        i.actor_id = $1 AND
        i.subject_id != all($2)
    GROUP BY id
""", ["INT", "INT[]"])
mutuals = list(functools.reduce(
    set.intersection,
    ({m["id"] for m in plpy.execute(mutuals_plan, [id, ignore_ids])} for id in ids)
))

intersecting_mutuals_plan = plpy.prepare("""
    SELECT i.subject_id id
    FROM follows.edges i
    INNER JOIN follows.edges o ON
        i.actor_id = o.subject_id AND
        i.subject_id = o.actor_id
    WHERE
        i.actor_id = $1 AND
        i.subject_id = any($2)
""", ["INT", "INT[]"])

return (
    [a, [m["id"] for m in plpy.execute(intersecting_mutuals_plan, [a, mutuals])]]
    for a in mutuals
)
$$ LANGUAGE plpython3u;

CREATE TYPE follows.find_follows_path_result AS (
    path INT [],
    nodes_expanded BIGINT
);

CREATE OR REPLACE FUNCTION follows.find_follows_path(
    source_id INT,
    target_id INT,
    ignore_ids INT [],
    max_depth INT,
    max_mutuals INT
) RETURNS follows.find_follows_path_result AS $$
nodes_expanded = 0

if source_id == target_id:
    return [[source_id], nodes_expanded]

import collections

mutuals_plan = plpy.prepare("""
    SELECT i.subject_id AS id
    FROM follows.edges AS i
    INNER JOIN
        follows.edges AS o
        ON
            i.actor_id = o.subject_id
            AND i.subject_id = o.actor_id
            AND i.subject_id != all($2)
    WHERE i.actor_id = $1
    LIMIT $3
""", ["INT", "INT[]", "INT"])

source_q = collections.deque([(source_id, 0)])
source_visited = {source_id: None}

target_q = collections.deque([(target_id, 0)])
target_visited = {target_id: None}

def build_path(node, source_parents, target_parents):
    path = []
    while node is not None:
        path.append(node)
        node = source_parents[node]
    path.reverse()

    node = target_parents[path[-1]]
    while node is not None:
        path.append(node)
        node = target_parents[node]
    return path

while source_q and target_q:
    if len(source_q) <= len(target_q):
        q, other_q, visited, other_visited = source_q, target_q, source_visited, target_visited
    else:
        q, other_q, visited, other_visited = target_q, source_q, target_visited, source_visited

    id, depth = q.popleft()

    _, other_depth = other_q[0]
    if depth + 1 + other_depth >= max_depth:
        return [None, nodes_expanded]

    rows = plpy.execute(mutuals_plan, [id, ignore_ids, max_mutuals + 1 if max_mutuals > 0 else None])
    if max_mutuals > 0 and len(rows) > max_mutuals:
        continue

    for row in rows:
        neighbor = row['id']

        if neighbor in visited:
            continue
        visited[neighbor] = id
        nodes_expanded += 1

        q.append((neighbor, depth + 1))

        if neighbor not in other_visited:
            continue

        if len(source_q) <= len(target_q):
            return [build_path(neighbor, source_visited, target_visited), nodes_expanded]
        else:
            return [build_path(neighbor, target_visited, source_visited)[::-1], nodes_expanded]

return [[], nodes_expanded]
$$ LANGUAGE plpython3u;
