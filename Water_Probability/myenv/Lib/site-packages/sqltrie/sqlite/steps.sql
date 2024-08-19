DROP TABLE IF EXISTS temp_split;
DROP TABLE IF EXISTS temp_steps;

CREATE TEMP TABLE temp_split AS
WITH RECURSIVE
path (path) AS (
    VALUES('{path}')
),

split (depth, name, rpath) AS (
    SELECT
        1,
        (
            CASE WHEN instr(path, '/') == 0 THEN path ELSE substr(path, 0, instr(path, '/')) END
        ),
        (
            CASE WHEN instr(path, '/') == 0 THEN '' ELSE substr(path, instr(path, '/') + 1) END
        )
    FROM path

    UNION ALL

    SELECT
        split.depth + 1,
        (
            CASE WHEN instr(split.rpath, '/') == 0 THEN split.rpath ELSE substr(split.rpath, 0, instr(split.rpath, '/')) END
        ),
        (
            CASE WHEN instr(split.rpath, '/') == 0 THEN '' ELSE substr(split.rpath, instr(split.rpath, '/') + 1) END
        )
    FROM split WHERE split.rpath != ''
)

SELECT
    depth,
    name
FROM split;

CREATE TEMP TABLE temp_steps AS
WITH RECURSIVE
steps (id, pid, name, path, has_value, value, depth) AS (
    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        nodes.name,
        nodes.has_value,
        nodes.value,
        temp_split.depth
    FROM nodes, temp_split
    WHERE
        temp_split.depth == 1 AND nodes.pid == {root} AND nodes.name == temp_split.name

    UNION ALL

    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        steps.path || '/' || nodes.name,
        nodes.has_value,
        nodes.value,
        steps.depth + 1
    FROM nodes, steps, temp_split
    WHERE
        nodes.pid == steps.id
        AND temp_split.depth == steps.depth + 1
        AND temp_split.name == nodes.name
)

SELECT
    id,
    pid,
    name,
    path,
    has_value,
    value
FROM steps;
