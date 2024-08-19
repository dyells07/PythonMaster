import json
import platform
from abc import abstractmethod
from typing import Any, Optional

try:
    import orjson

    def json_loads(value):
        return orjson.loads(value)  # pylint: disable=no-member

    def json_dumps(value):
        return orjson.dumps(value)  # pylint: disable=no-member

except ImportError:
    # NOTE: orjson doesn't support PyPy, see
    # https://github.com/ijl/orjson/issues/90
    if platform.python_implementation() == "CPython":
        raise

    def json_loads(value):
        return json.load(value.decode("utf-8"))

    def json_dumps(value):
        return json.dumps(value).encode("utf-8")


from .trie import AbstractTrie, Iterator, NodeFactory, TrieKey


class SerializedTrie(AbstractTrie):
    @property
    @abstractmethod
    def _trie(self):
        pass

    def close(self):
        self._trie.close()

    def commit(self):
        self._trie.commit()

    def rollback(self):
        self._trie.rollback()

    @abstractmethod
    def _load(self, key: TrieKey, value: Optional[bytes]) -> Optional[Any]:
        pass

    @abstractmethod
    def _dump(self, key: TrieKey, value: Optional[Any]) -> Optional[bytes]:
        pass

    def __setitem__(self, key, value):
        self._trie[key] = self._dump(key, value)

    def __getitem__(self, key):
        raw = self._trie[key]
        return self._load(key, raw)

    def __delitem__(self, key):
        del self._trie[key]

    def __len__(self):
        return len(self._trie)

    def view(self, key: Optional[TrieKey] = None) -> "SerializedTrie":
        if not key:
            return self

        raw_trie = self._trie.view(key)
        trie = type(self)()
        # pylint: disable-next=protected-access
        trie._trie = raw_trie  # type: ignore
        return trie

    def items(self, *args, **kwargs):
        yield from (
            (key, self._load(key, raw))
            for key, raw in self._trie.items(*args, **kwargs)
        )

    def ls(self, key, with_values=False):
        entries = self._trie.ls(key, with_values=with_values)
        if with_values:
            yield from ((ekey, self._load(ekey, evalue)) for ekey, evalue in entries)
        else:
            yield from entries

    def traverse(self, node_factory: NodeFactory, prefix: Optional[TrieKey] = None):
        def _node_factory_wrapper(path_conv, path, children, *value):
            value = value[0] if value else None
            return node_factory(path_conv, path, children, self._load(path, value))

        return self._trie.traverse(_node_factory_wrapper, prefix=prefix)

    def diff(self, *args, **kwargs):
        yield from self._trie.diff(*args, **kwargs)

    def has_node(self, key):
        return self._trie.has_node(key)

    def delete_node(self, key):
        return self._trie.delete_node(key)

    def shortest_prefix(self, key):
        sprefix = self._trie.shortest_prefix(key)
        if sprefix is None:
            return None

        skey, raw = sprefix
        return key, self._load(skey, raw)

    def prefixes(self, key):
        for prefix, raw in self._trie.prefixes(key):
            yield (prefix, self._load(prefix, raw))

    def longest_prefix(self, key):
        lprefix = self._trie.longest_prefix(key)
        if lprefix is None:
            return None

        lkey, raw = lprefix
        return lkey, self._load(lkey, raw)

    def __iter__(self) -> Iterator[TrieKey]:
        yield from self._trie


class JSONTrie(SerializedTrie):  # pylint: disable=abstract-method
    def _load(self, key: TrieKey, value: Optional[bytes]) -> Optional[Any]:
        if value is None:
            return None
        return json_loads(value)

    def _dump(self, key: TrieKey, value: Optional[Any]) -> Optional[bytes]:
        if value is None:
            return None
        return json_dumps(value)
