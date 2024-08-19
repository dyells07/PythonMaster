DROP TABLE IF EXISTS temp_old_items;
DROP TABLE IF EXISTS temp_new_items;
DROP TABLE IF EXISTS temp_diff;
DROP INDEX IF EXISTS temp_old_items_path_idx;
DROP INDEX IF EXISTS temp_new_items_path_idx;

CREATE TEMP TABLE temp_old_items (
    id INTEGER PRIMARY KEY,
    pid INTEGER,
    name TEXT,
    path TEXT,
    has_value BOOLEAN,
    value BLOB,
    UNIQUE(pid, name),
    UNIQUE(id, pid),
    CHECK(id != pid)
);
CREATE INDEX IF NOT EXISTS temp_old_items_path_idx ON temp_old_items (path);

INSERT INTO temp_old_items
WITH RECURSIVE old_items (id, pid, name, path, has_value, value) AS (
    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        nodes.name,
        nodes.has_value,
        nodes.value
    FROM nodes WHERE nodes.pid == {old_root}

    UNION ALL

    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        old_items.path || '/' || nodes.name,
        nodes.has_value,
        nodes.value
    FROM nodes, old_items WHERE old_items.id == nodes.pid
)

SELECT * FROM old_items WHERE has_value;

CREATE TEMP TABLE temp_new_items (
    id INTEGER PRIMARY KEY,
    pid INTEGER,
    name TEXT,
    path TEXT,
    has_value BOOLEAN,
    value BLOB,
    UNIQUE(pid, name),
    UNIQUE(id, pid),
    CHECK(id != pid)
);
CREATE INDEX IF NOT EXISTS temp_new_items_path_idx ON temp_new_items (path);

INSERT INTO temp_new_items
WITH RECURSIVE new_items (id, pid, name, path, has_value, value) AS (
    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        nodes.name,
        nodes.has_value,
        nodes.value
    FROM nodes WHERE nodes.pid == {new_root}

    UNION ALL

    SELECT
        nodes.id,
        nodes.pid,
        nodes.name,
        new_items.path || '/' || nodes.name,
        nodes.has_value,
        nodes.value
    FROM nodes, new_items WHERE new_items.id == nodes.pid
)

SELECT * FROM new_items WHERE has_value;

CREATE TEMP TABLE temp_diff AS
WITH RECURSIVE diff (
    old_id,
    old_pid,
    old_name,
    old_path,
    old_has_value,
    old_value,
    new_id,
    new_pid,
    new_name,
    new_path,
    new_has_value,
    new_value
) AS (
    /* FULL OUTER JOIN is not supported, so we have to use two LEFT JOINs :( */
    SELECT
        old.id,
        old.pid,
        old.name,
        old.path,
        old.has_value,
        old.value,
        new.id,
        new.pid,
        new.name,
        new.path,
        new.has_value,
        new.value
    FROM
        temp_old_items AS old
    LEFT JOIN
        temp_new_items AS new
        ON old.path == new.path

    UNION

    SELECT
        old.id,
        old.pid,
        old.name,
        old.path,
        old.has_value,
        old.value,
        new.id,
        new.pid,
        new.name,
        new.path,
        new.has_value,
        new.value
    FROM
        temp_new_items AS new
    LEFT JOIN
        temp_old_items AS old
        ON old.path == new.path
)

SELECT
    (
        CASE WHEN old_id IS NULL THEN 'add' ELSE (
            CASE WHEN new_id IS NULL THEN 'delete' ELSE (
                CASE
                    WHEN old_value != new_value THEN 'modify' ELSE 'unchanged'
                END
                ) END
            ) END
    ) AS type,
    *
FROM diff
WHERE (
    {with_unchanged}
    OR type != 'unchanged'
);
