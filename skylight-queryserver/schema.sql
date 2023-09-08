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

CREATE OR REPLACE FUNCTION follows.incoming(
    ids INT [],
    ignore_ids INT []
) RETURNS TABLE (id INT) AS $$
if not ids:
    return []

import functools

mutuals_plan = plpy.prepare("""
    SELECT i.actor_id id
    FROM follows.edges i
    WHERE
        i.subject_id = $1 AND
        i.actor_id != all($2)
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

CREATE OR REPLACE FUNCTION follows.set_paths_generator(
    source_id INT,
    target_id INT,
    ignore_ids INT []
) RETURNS VOID AS $$

mutuals_plan = plpy.prepare("""
    SELECT id
    FROM follows.mutuals(ARRAY[$1], $2)
""", ["INT", "INT[]"])

def get_neighbors(id):
    return (row['id'] for row in plpy.execute(mutuals_plan, [id, ignore_ids]))

is_mutual_plan = plpy.prepare("""
    SELECT EXISTS (
        SELECT *
        FROM follows.edges i
        INNER JOIN follows.edges o ON
            i.actor_id = o.subject_id AND
            i.subject_id = o.actor_id
        WHERE
            i.actor_id = $1 AND
            i.subject_id = $2
    ) AS v
""", ["INT", "INT"])

def is_neighbor(source_id, target_id):
    row, = plpy.execute(is_mutual_plan, [source_id, target_id])
    return row["v"]


import collections

def paths(source, target, get_neighbors):
    if source == target:
        yield ([source], 0)
        return

    # Check for direct intersection.
    if is_neighbor(source, target):
        yield ([source, target], 0)

    nodes_expanded = 0

    source_q = collections.deque([source])
    source_visited = {source: None}

    target_q = collections.deque([target])
    target_visited = {target: None}

    while source_q and target_q:
        if len(source_q) <= len(target_q):
            q, visited, other_visited, toward = source_q, source_visited, target_visited, target
        else:
            q, visited, other_visited, toward = target_q, target_visited, source_visited, source

        node = q.popleft()
        for neighbor in get_neighbors(node):
            if neighbor in visited or neighbor == toward:
                continue
            visited[neighbor] = node
            nodes_expanded += 1

            q.append(neighbor)

            if neighbor in other_visited:
                n = neighbor
                path = []
                while n is not None:
                    path.append(n)
                    n = source_visited[n]
                path.reverse()
                n = target_visited[path[-1]]
                while n is not None:
                    path.append(n)
                    n = target_visited[n]
                yield path, nodes_expanded

GD['skylight_paths_generator'] = paths(source_id, target_id, get_neighbors)
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION follows.clear_paths_generator()
RETURNS VOID AS $$
del GD['skylight_paths_generator']
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION follows.next_paths(
    n INT
) RETURNS TABLE (path INT [], nodes_expanded INT) AS $$
if 'skylight_paths_generator' not in GD:
    plpy.error('set_paths_generator was not called in this session')
import itertools
yield from itertools.islice(GD['skylight_paths_generator'], n)
$$ LANGUAGE plpython3u;
