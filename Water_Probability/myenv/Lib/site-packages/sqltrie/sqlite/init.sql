PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pid INTEGER,
    name TEXT,
    has_value BOOLEAN,
    value BLOB,
    UNIQUE(pid, name),
    UNIQUE(id, pid),
    CHECK(id != pid)
);
CREATE INDEX IF NOT EXISTS nodes_pid_idx ON nodes (pid);
INSERT OR IGNORE INTO nodes (id, pid, name, has_value, value)
VALUES (1, NULL, "", 0, NULL);
