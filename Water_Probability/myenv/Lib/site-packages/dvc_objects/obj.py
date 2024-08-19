from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .fs.base import AnyFSPath, FileSystem


class Object:
    __slots__ = ("path", "fs", "oid")

    def __init__(
        self,
        path: "AnyFSPath",
        fs: "FileSystem",
        oid: str,
    ):
        self.path = path
        self.fs = fs
        self.oid = oid

    def __len__(self):
        return 1

    def __str__(self):
        return f"object {self.oid}"

    def __bool__(self):
        return bool(self.oid)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self.path == other.path and self.fs == other.fs and self.oid == other.oid

    def __hash__(self):
        return hash(
            (
                self.oid,
                self.path,
                self.fs.protocol if self.fs else None,
            )
        )
