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
import collections

mutuals_plan = plpy.prepare("""
    SELECT id
    FROM follows.mutuals(ARRAY[$1], $2)
""", ["INT", "INT[]"])

def get_neighbors(id):
    return (row['id'] for row in plpy.execute(mutuals_plan, [id, ignore_ids]))


def bibfs(source, target, get_neighbors, max_depth):
    nodes_expanded = 0

    if source == target:
        return [source], nodes_expanded

    source_q = collections.deque([(source, 0)])
    source_visited = {source: None}

    target_q = collections.deque([(target, 0)])
    target_visited = {target: None}

    while source_q and target_q:
        if len(source_q) <= len(target_q):
            q, visited, other_visited = source_q, source_visited, target_visited
        else:
            q, visited, other_visited = target_q, target_visited, source_visited

        id, depth = q.popleft()

        if depth >= max_depth:
            return None, nodes_expanded

        for neighbor in get_neighbors(id):
            if neighbor in visited:
                continue
            visited[neighbor] = id
            nodes_expanded += 1

            q.append((neighbor, depth + 1))

            if neighbor in other_visited:
                node = neighbor

                path = []
                while node is not None:
                    path.append(node)
                    node = source_visited[node]
                path.reverse()

                node = target_visited[path[-1]]
                while node is not None:
                    path.append(node)
                    node = target_visited[node]

                return path, nodes_expanded

    return [], nodes_expanded

return bibfs(source_id, target_id, get_neighbors, max_depth)
$$ LANGUAGE plpython3u STABLE;
