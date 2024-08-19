from abc import abstractmethod
from collections.abc import MutableMapping
from typing import (
    Any,
    Callable,
    Iterator,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

from attrs import define


class ShortKeyError(KeyError):
    """Raised when given key is a prefix of an existing longer key
    but does not have a value associated with itself."""


TrieKey = Union[Tuple[()], Tuple[str, ...]]
TrieStep = Tuple[Optional[TrieKey], Optional[bytes]]


class TrieNode(NamedTuple):
    key: TrieKey
    value: Optional[bytes]


PathConv = Optional[Callable[[Any], TrieKey]]
_T = TypeVar("_T")


class NodeFactory(Protocol):
    def __call__(
        self,
        path_conv: PathConv,
        path: Any,
        children: Iterator[_T],
        value: Any = (),
    ) -> _T: ...


ADD = "add"
MODIFY = "modify"
RENAME = "rename"
DELETE = "delete"
UNCHANGED = "unchanged"


@define(frozen=True, unsafe_hash=True, order=True)
class Change:
    typ: str
    old: Optional[TrieNode]
    new: Optional[TrieNode]

    @property
    def key(self) -> TrieKey:
        if self.typ == RENAME:
            raise ValueError

        if self.typ == ADD:
            entry = self.new
        else:
            entry = self.old

        assert entry
        assert entry.key
        return entry.key

    def __bool__(self) -> bool:
        return self.typ != UNCHANGED


class AbstractTrie(MutableMapping):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    @classmethod
    @abstractmethod
    def open(cls, path: str) -> "AbstractTrie":
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def items(  # type: ignore
        self, prefix: Optional[TrieKey] = None, shallow: Optional[bool] = False
    ) -> Iterator[Tuple[TrieKey, bytes]]:
        pass

    @abstractmethod
    def view(self, key: Optional[TrieKey] = None) -> "AbstractTrie":
        pass

    @abstractmethod
    def has_node(self, key: TrieKey) -> bool:
        pass

    @abstractmethod
    def delete_node(self, key: TrieKey) -> bool:
        # NOTE: this will leave orphans down the tree
        pass

    @abstractmethod
    def prefixes(self, key: TrieKey) -> Iterator[TrieStep]:
        pass

    @abstractmethod
    def shortest_prefix(self, key: TrieKey) -> Optional[TrieStep]:
        pass

    @abstractmethod
    def longest_prefix(self, key: TrieKey) -> Optional[TrieStep]:
        pass

    @abstractmethod
    # pylint: disable-next=invalid-name
    def ls(
        self, key: TrieKey, with_values: bool = False
    ) -> Iterator[Union[TrieKey, TrieNode]]:
        pass

    @abstractmethod
    def traverse(
        self, node_factory: NodeFactory, prefix: Optional[TrieKey] = None
    ) -> _T:
        pass

    @abstractmethod
    def diff(
        self, old: TrieKey, new: TrieKey, with_unchanged: bool = False
    ) -> Iterator[Change]:
        pass
