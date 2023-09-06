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

CREATE OR REPLACE FUNCTION follows.mutuals(
    ids INT [],
    ignore_ids INT []
) RETURNS TABLE (id INT) AS $$
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
return list(functools.reduce(
    set.intersection,
    ({m["id"] for m in plpy.execute(mutuals_plan, [id, ignore_ids])} for id in ids)
))
$$ LANGUAGE plpython3u STABLE;

CREATE OR REPLACE FUNCTION follows.neighborhood(
    ids INT [],
    ignore_ids INT []
) RETURNS TABLE (actor_id INT, subject_ids INT []) AS $$
mutuals_plan = plpy.prepare("""
    SELECT id
    FROM follows.mutuals($1, $2)
""", ["INT[]", "INT[]"])
mutuals = [m["id"] for m in plpy.execute(mutuals_plan, [ids, ignore_ids])]

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
$$ LANGUAGE plpython3u STABLE;

CREATE TYPE follows.find_follows_path_result AS (
    path INT [],
    nodes_expanded BIGINT
);

CREATE OR REPLACE FUNCTION follows.find_follows_path(
    source_id INT,
    target_id INT,
    ignore_ids INT [],
    max_depth INT
) RETURNS follows.find_follows_path_result AS $$
nodes_expanded = 0

if source_id == target_id:
    return [[source_id], nodes_expanded]

import collections

source_q = collections.deque([(source_id, 0)])
source_visited = {source_id: None}

target_q = collections.deque([(target_id, 0)])
target_visited = {target_id: None}

mutuals_plan = plpy.prepare("""
    SELECT id
    FROM follows.mutuals(ARRAY[$1], $2)
""", ["INT", "INT[]"])

def get_neighbors(id):
    return (row['id'] for row in plpy.execute(mutuals_plan, [id, ignore_ids]))

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

    for neighbor in get_neighbors(id):
        if neighbor in visited:
            continue
        visited[neighbor] = id
        nodes_expanded += 1

        q.append((neighbor, depth + 1))

        if neighbor in other_visited:
            if len(source_q) <= len(target_q):
                return [build_path(neighbor, source_visited, target_visited), nodes_expanded]
            else:
                return [build_path(neighbor, target_visited, source_visited)[::-1], nodes_expanded]

return [[], nodes_expanded]
$$ LANGUAGE plpython3u STABLE;
