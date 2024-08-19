import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union
from uuid import uuid4

from attrs import define

from ..trie import (
    AbstractTrie,
    Change,
    NodeFactory,
    ShortKeyError,
    TrieKey,
    TrieNode,
    TrieStep,
)

# https://www.sqlite.org/lang_with.html
MIN_SQLITE_VER = (3, 8, 3)

# https://www.sqlite.org/lang_UPSERT.html
HAS_UPSERT = sqlite3.sqlite_version_info >= (3, 24, 0)

# NOTE: seems like "named" doesn't work without changing this global var,
# so unfortunately we have to stick with qmark.
assert sqlite3.paramstyle == "qmark"

scripts = Path(__file__).parent

ROOT_KEY = ()
ROOT_ID = 1
ROOT_NAME = "/"

INIT_SQL = (scripts / "init.sql").read_text()

STEPS_SQL = (scripts / "steps.sql").read_text()
STEPS_TABLE = "temp_steps"

DIFF_SQL = (scripts / "diff.sql").read_text()
DIFF_TABLE = "temp_diff"

DEFAULT_DB_FMT = "file:sqlitetrie_{id}?mode=memory&cache=shared"


@define(frozen=True)
class _SQLiteTrieNode:
    id: int
    pid: int
    name: str
    has_value: bool
    value: Optional[bytes]

    @classmethod
    def from_step(cls, step: sqlite3.Row):
        kwargs = dict(step)
        kwargs.pop("path", None)
        return cls(**kwargs)

    def get_children(
        self,
        conn: sqlite3.Connection,
        limit: Optional[int] = None,
    ) -> Iterator["_SQLiteTrieNode"]:
        limit_sql = ""
        if limit:
            limit_sql = f"LIMIT {limit}"

        for row in conn.execute(  # nosec
            f"""
            SELECT * FROM nodes WHERE nodes.pid == ? {limit_sql}
            """,
            (self.id,),
        ).fetchall():
            yield _SQLiteTrieNode(**row)

    def traverse(
        self,
        conn: sqlite3.Connection,
        node_factory: NodeFactory,
        key: TrieKey,
    ):
        def children() -> Iterator[Tuple[TrieKey, bytes]]:
            for node in self.get_children(conn):
                yield node.traverse(conn, node_factory, key + (node.name,))

        args: List[Any] = [None, key, children()]
        if self.has_value:
            args.append(self.value)
        return node_factory(*args)

    def iterate(
        self, conn: sqlite3.Connection, key: TrieKey, shallow: bool = False
    ) -> Iterator[Tuple[TrieKey, bytes]]:
        stack = [(key, self)]
        while stack:
            node_key, node = stack.pop()
            if node.has_value:
                yield node_key, node.value  # type: ignore
            if not (shallow and node.has_value):
                stack.extend(
                    (node_key + (child.name,), child)
                    for child in node.get_children(conn)
                )


class SQLiteTrie(AbstractTrie):
    def __init__(self, *args, **kwargs):
        self._root_key = ROOT_KEY
        self._root_id = ROOT_ID
        self._path = DEFAULT_DB_FMT.format(id=uuid4())
        self._local = threading.local()
        self._ids = {}
        super().__init__(*args, **kwargs)

    @classmethod
    def open(cls, path):
        trie = cls()
        trie._path = path
        return trie

    def close(self):
        self._ids = {}

        conn = getattr(self._local, "conn", None)
        if conn is None:
            return

        conn.close()

        try:
            delattr(self._local, "conn")
        except AttributeError:
            pass

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    @property
    def _conn(self):  # pylint: disable=method-hidden
        if sqlite3.sqlite_version_info < MIN_SQLITE_VER:
            raise RuntimeError(
                f"SQLite version is too old, please upgrade to >= {MIN_SQLITE_VER}"
            )

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = sqlite3.connect(self._path)
            conn.row_factory = sqlite3.Row
            conn.executescript(INIT_SQL)

        return conn

    def _create_node(self, key):
        try:
            return self._ids[key]
        except KeyError:
            pass

        rows = self._traverse(key)
        if rows:
            longest_prefix = tuple(rows[-1]["path"].split("/"))
            pid = rows[-1]["id"]
        else:
            longest_prefix = ()
            pid = self._root_id
        self._ids[longest_prefix] = pid

        node_key = longest_prefix
        for name in key[len(longest_prefix) :]:
            node_key = (*node_key, name)
            cur = self._conn.execute(
                """
                INSERT OR IGNORE
                    INTO nodes (pid, name)
                    VALUES (?, ?)
                """,
                (pid, name),
            )
            nid = cur.lastrowid
            self._ids[node_key] = nid
            pid = nid

        return pid

    def _traverse(self, key):
        path = "/".join(key).replace("'", "''")
        self._conn.executescript(STEPS_SQL.format(path=path, root=self._root_id))

        return self._conn.execute(f"SELECT * FROM {STEPS_TABLE}").fetchall()  # nosec

    def _get_node(self, key):
        if not key:
            return self._conn.execute(
                "SELECT * FROM nodes WHERE id == ?",
                (self._root_id,),
            ).fetchone()

        rows = list(self._traverse(key))
        if len(rows) != len(key):
            raise KeyError(key)

        return rows[-1]

    def _get_children(self, key, limit=None):
        node = self._get_node(key)

        limit_sql = ""
        if limit:
            limit_sql = f"LIMIT {limit}"

        return self._conn.execute(  # nosec
            f"""
            SELECT * FROM nodes WHERE nodes.pid == ? {limit_sql}
            """,
            (node["id"],),
        ).fetchall()

    def __setitem__(self, key, value):
        if not key:
            self._conn.execute(
                """
                UPDATE nodes SET has_value = True, value = ?  WHERE id == ?
                """,
                (value, self._root_id),
            )
            return

        pid = self._create_node(key[:-1])

        if HAS_UPSERT:
            self._conn.execute(
                """
                INSERT INTO
                    nodes (pid, name, has_value, value)
                    VALUES (:pid, :name, True, :value)
                    ON CONFLICT (pid, name) DO UPDATE SET has_value=True, value=:value
                """,
                {"pid": pid, "name": key[-1], "value": value},
            )
        else:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO
                    nodes (id, pid, name, has_value, value)
                    SELECT
                        COALESCE(
                            (SELECT id FROM nodes WHERE pid == :pid AND name == :name),
                            (SELECT MAX(id) + 1 FROM nodes)
                        ),
                        :pid,
                        :name,
                        1,
                        :value
                """,
                {"pid": pid, "name": key[-1], "value": value},
            )

    def __iter__(self):
        yield from (key for key, _ in self.items())

    def __getitem__(self, key):
        row = self._get_node(key)
        has_value = row["has_value"]
        if not has_value:
            raise ShortKeyError(key)
        return row["value"]

    def __delitem__(self, key):
        node = self._get_node(key)
        self._conn.execute(
            """
            UPDATE nodes SET has_value = 0, value = NULL WHERE id == ?
            """,
            (node["id"],),
        )

    def __len__(self):
        return len(list(self.items()))

    def prefixes(self, key: TrieKey) -> Iterator[TrieStep]:
        for row in self._traverse(key):
            if not row["has_value"]:
                continue

            yield (
                tuple(row["path"].split("/")),  # type: ignore
                row["value"],
            )

    def shortest_prefix(self, key: TrieKey) -> Optional[TrieStep]:
        return next(self.prefixes(key), None)

    def longest_prefix(self, key) -> Optional[TrieStep]:
        ret = None
        for step in self.prefixes(key):
            ret = step
        return ret

    def view(  # type: ignore
        self,
        key: Optional[TrieKey] = None,
    ) -> "SQLiteTrie":
        if not key:
            return self

        self.commit()
        try:
            nid = self._get_node(key)["id"]
        except KeyError:
            nid = self._create_node(key)
            self.commit()

        trie = SQLiteTrie()
        trie._path = self._path  # pylint: disable=protected-access
        trie._local = self._local  # pylint: disable=protected-access
        trie._root_key = key  # pylint: disable=protected-access
        trie._root_id = nid  # pylint: disable=protected-access
        return trie

    def items(self, prefix=None, shallow=False):
        key = prefix or ()
        node = _SQLiteTrieNode.from_step(self._get_node(key))
        yield from node.iterate(self._conn, key, shallow=shallow)

    def clear(self):
        self._conn.execute("DELETE FROM nodes")

    def has_node(self, key: TrieKey) -> bool:
        try:
            self._get_node(key)
            return True
        except KeyError:
            return False

    def delete_node(self, key: TrieKey):
        node = self._get_node(key)
        self._ids.pop(key, None)
        self._conn.execute(
            """
            DELETE FROM nodes WHERE id = ?
            """,
            (node["id"],),
        )

    def ls(
        self, key: TrieKey, with_values: Optional[bool] = False
    ) -> Iterator[Union[TrieKey, TrieNode]]:
        if with_values:
            yield from (  # type: ignore
                ((*key, row["name"]), row["value"]) for row in self._get_children(key)
            )
        else:
            yield from (  # type: ignore
                (*key, row["name"]) for row in self._get_children(key)
            )

    def traverse(self, node_factory: NodeFactory, prefix: Optional[TrieKey] = None):
        key = prefix or ()
        node = _SQLiteTrieNode.from_step(self._get_node(key))
        return node.traverse(self._conn, node_factory, key)

    def diff(self, old, new, with_unchanged=False):
        old_id = self._get_node(old)["id"]
        new_id = self._get_node(new)["id"]

        self._conn.executescript(
            DIFF_SQL.format(
                old_root=old_id,
                new_root=new_id,
                with_unchanged=int(with_unchanged),
            )
        )

        rows = self._conn.execute(f"SELECT * FROM {DIFF_TABLE}")  # nosec
        yield from (
            Change(
                row["type"],
                TrieNode(
                    tuple(row["old_path"].split("/")),
                    row["old_value"],
                ),
                TrieNode(
                    tuple(row["new_path"].split("/")),
                    row["new_value"],
                ),
            )
            for row in rows
        )
