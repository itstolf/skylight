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

import collections

def paths(source, target, get_neighbors):
    nodes_expanded = 0

    if source == target:
        yield ([source], nodes_expanded)
        return

    source_q = collections.deque([(source, [source])])
    source_visited = {source: [source]}

    target_q = collections.deque([(target, [target])])
    target_visited = {target: [target]}

    while source_q and target_q:
        if len(source_q) <= len(target_q):
            q, visited, other_visited, is_forward = source_q, source_visited, target_visited, True
        else:
            q, visited, other_visited, is_forward = target_q, target_visited, source_visited, False

        node, path = q.popleft()
        for neighbor in get_neighbors(node):
            new_path = [*path, neighbor]

            if neighbor in other_visited:
                final_path = [*path, *other_visited[neighbor][::-1]]
                if not is_forward:
                    final_path.reverse()
                yield (final_path, nodes_expanded)

            if neighbor not in visited:
                visited[neighbor] = new_path
                q.append((neighbor, new_path))
                nodes_expanded += 1

GD['skylight_paths_generator'] = paths(source_id, target_id, get_neighbors)
$$ LANGUAGE plpython3u STABLE;

CREATE OR REPLACE FUNCTION follows.clear_paths_generator()
RETURNS VOID AS $$
del GD['skylight_paths_generator']
$$ LANGUAGE plpython3u STABLE;

CREATE OR REPLACE FUNCTION follows.next_paths(
    n INT
) RETURNS TABLE (path INT [], nodes_expanded INT) AS $$
if 'skylight_paths_generator' not in GD:
    plpy.error('set_paths_generator was not called in this session')
import itertools
yield from itertools.islice(GD['skylight_paths_generator'], n)
$$ LANGUAGE plpython3u STABLE;
