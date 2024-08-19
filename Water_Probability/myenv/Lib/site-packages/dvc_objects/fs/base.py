import asyncio
import datetime
import logging
import ntpath
import os
import posixpath
import shutil
from collections.abc import Iterable, Iterator, Sequence
from functools import partial
from multiprocessing import cpu_count
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Optional,
    Union,
    overload,
)
from urllib.parse import urlsplit, urlunsplit

import fsspec
from fsspec.asyn import get_loop
from fsspec.callbacks import DEFAULT_CALLBACK

from dvc_objects.compat import cached_property
from dvc_objects.executors import ThreadPoolExecutor, batch_coros

from .callbacks import wrap_file
from .errors import RemoteMissingDepsError

if TYPE_CHECKING:
    from typing import BinaryIO, TextIO

    from fsspec.spec import AbstractFileSystem


logger = logging.getLogger(__name__)


FSPath = str
AnyFSPath = str

# An info() entry, might evolve to a TypedDict
# in the future (e.g for properly type 'size' etc).
Entry = dict[str, Any]


class LinkError(OSError):
    def __init__(self, link: str, fs: "FileSystem", path: str) -> None:
        import errno

        super().__init__(
            errno.EPERM,
            f"{link} is not supported for {fs.protocol} by {type(fs)}",
            path,
        )


class FileSystem:
    sep = "/"

    flavour = posixpath
    protocol = "base"
    REQUIRES: ClassVar[dict[str, str]] = {}
    _JOBS = 4 * cpu_count()

    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 2
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    PARAM_CHECKSUM: ClassVar[Optional[str]] = None

    def __init__(self, fs=None, **kwargs: Any):
        self._check_requires(**kwargs)

        self.jobs = kwargs.get("jobs") or self._JOBS
        self.hash_jobs = kwargs.get("checksum_jobs") or self.HASH_JOBS
        self._config = kwargs
        if fs:
            self.fs = fs

    @cached_property
    def fs_args(self) -> dict[str, Any]:
        ret = {"skip_instance_cache": True}
        ret.update(self._prepare_credentials(**self._config))
        return ret

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def root_marker(self) -> str:
        return self.fs.root_marker

    def getcwd(self) -> str:
        return ""

    def chdir(self, path: str):
        raise NotImplementedError

    @classmethod
    def join(cls, *parts: str) -> str:
        return cls.flavour.join(*parts)

    @classmethod
    def split(cls, path: str) -> tuple[str, str]:
        return cls.flavour.split(path)

    @classmethod
    def splitext(cls, path: str) -> tuple[str, str]:
        return cls.flavour.splitext(path)

    def normpath(self, path: str) -> str:
        if self.flavour == ntpath:
            return self.flavour.normpath(path)

        parts = list(urlsplit(path))
        parts[2] = self.flavour.normpath(parts[2])
        return urlunsplit(parts)

    @classmethod
    def isabs(cls, path: str) -> bool:
        return cls.flavour.isabs(path)

    def abspath(self, path: str) -> str:
        if not self.isabs(path):
            path = self.join(self.getcwd(), path)
        return self.normpath(path)

    @classmethod
    def commonprefix(cls, paths: Sequence[str]) -> str:
        return cls.flavour.commonprefix(paths)

    @classmethod
    def commonpath(cls, paths: Iterable[str]) -> str:
        return cls.flavour.commonpath(list(paths))

    @classmethod
    def parts(cls, path: str) -> tuple[str, ...]:
        drive, path = cls.flavour.splitdrive(path.rstrip(cls.flavour.sep))

        ret = []
        while True:
            path, part = cls.flavour.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        if drive:
            ret = [drive, *ret]

        return tuple(ret)

    @classmethod
    def parent(cls, path: str) -> str:
        return cls.flavour.dirname(path)

    @classmethod
    def dirname(cls, path: str) -> str:
        return cls.parent(path)

    @classmethod
    def parents(cls, path: str) -> Iterator[str]:
        while True:
            parent = cls.flavour.dirname(path)
            if parent == path:
                break
            yield parent
            path = parent

    @classmethod
    def name(cls, path: str) -> str:
        return cls.flavour.basename(path)

    @classmethod
    def suffix(cls, path: str) -> str:
        name = cls.name(path)
        _, dot, suffix = name.partition(".")
        return dot + suffix

    @classmethod
    def with_name(cls, path: str, name: str) -> str:
        return cls.join(cls.parent(path), name)

    @classmethod
    def with_suffix(cls, path: str, suffix: str) -> str:
        return cls.splitext(path)[0] + suffix

    @classmethod
    def isin(cls, left: str, right: str) -> bool:
        if left == right:
            return False
        try:
            common = cls.commonpath([left, right])
        except ValueError:
            # Paths don't have the same drive
            return False
        return common == right

    @classmethod
    def isin_or_eq(cls, left: str, right: str) -> bool:
        return left == right or cls.isin(left, right)

    @classmethod
    def overlaps(cls, left: str, right: str) -> bool:
        return cls.isin_or_eq(left, right) or cls.isin(right, left)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        if start is None:
            start = "."
        return self.flavour.relpath(self.abspath(path), start=self.abspath(start))

    def relparts(self, path: str, start: Optional[str] = None) -> tuple[str, ...]:
        return self.parts(self.relpath(path, start=start))

    @classmethod
    def as_posix(cls, path: str) -> str:
        return path.replace(cls.flavour.sep, posixpath.sep)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return path

    def unstrip_protocol(self, path: str) -> str:
        return path

    @cached_property
    def fs(self) -> "AbstractFileSystem":
        raise NotImplementedError

    @property
    def version_aware(self) -> bool:
        return self._config.get("version_aware", False)

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str) -> "dict[str, Any]":
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(urlpath)
        options.pop("path", None)
        options.pop("protocol", None)
        return options

    def _prepare_credentials(
        self,
        **config: dict[str, Any],
    ) -> dict[str, Any]:
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    @classmethod
    def get_missing_deps(cls) -> list[str]:
        from importlib.util import find_spec

        return [pkg for pkg, mod in cls.REQUIRES.items() if not find_spec(mod)]

    def _check_requires(self, **kwargs):
        from .scheme import Schemes

        missing = self.get_missing_deps()
        if not missing:
            return

        proto = self.protocol
        if proto == Schemes.WEBDAVS:
            proto = Schemes.WEBDAV

        url = kwargs.get("url", f"{self.protocol}://")
        raise RemoteMissingDepsError(self, proto, url, missing)

    def isdir(self, path: AnyFSPath) -> bool:
        return self.fs.isdir(path)

    def isfile(self, path: AnyFSPath) -> bool:
        return self.fs.isfile(path)

    def is_empty(self, path: AnyFSPath) -> bool:
        entry = self.info(path)
        if entry["type"] == "directory":
            return not self.fs.ls(path)
        return entry["size"] == 0

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: Literal["rb", "br", "wb"],
        **kwargs: Any,
    ) -> "BinaryIO":
        return self.open(path, mode, **kwargs)

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: Literal["r", "rt", "w"] = "r",
        **kwargs: Any,
    ) -> "TextIO":
        ...

    def open(
        self,
        path: AnyFSPath,
        mode: str = "r",
        **kwargs: Any,
    ) -> "IO[Any]":
        if "b" in mode:
            kwargs.pop("encoding", None)
        return self.fs.open(path, mode=mode, **kwargs)

    def read_block(
        self,
        path: AnyFSPath,
        offset: int,
        length: int,
        delimiter: Optional[bytes] = None,
    ) -> bytes:
        return self.fs.read_block(path, offset, length, delimiter=delimiter)

    def cat(
        self,
        path: Union[AnyFSPath, list[AnyFSPath]],
        recursive: bool = False,
        on_error: Literal["raise", "omit", "return"] = "raise",
        **kwargs: Any,
    ) -> Union[bytes, dict[AnyFSPath, bytes]]:
        return self.fs.cat(path, recursive=recursive, on_error=on_error, **kwargs)

    def cat_ranges(
        self,
        paths: list[AnyFSPath],
        starts: list[int],
        ends: list[int],
        max_gap: Optional[int] = None,
        **kwargs,
    ) -> list[bytes]:
        return self.fs.cat_ranges(paths, starts, ends, max_gap=max_gap, **kwargs)

    def cat_file(
        self,
        path: AnyFSPath,
        start: Optional[int] = None,
        end: Optional[int] = None,
        **kwargs: Any,
    ) -> bytes:
        return self.fs.cat_file(path, start=start, end=end, **kwargs)

    def head(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.head(path, size=size)

    def tail(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.tail(path, size=size)

    def pipe_file(self, path: AnyFSPath, value: bytes, **kwargs: Any) -> None:
        return self.fs.pipe_file(path, value, **kwargs)

    write_bytes = pipe_file
    read_bytes = cat_file

    def read_text(
        self,
        path: AnyFSPath,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        return self.fs.read_text(
            path, encoding=encoding, errors=errors, newline=newline, **kwargs
        )

    def write_text(
        self,
        path: AnyFSPath,
        value: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.fs.write_text(
            path,
            value,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )

    def pipe(
        self,
        path: Union[AnyFSPath, dict[AnyFSPath, bytes]],
        value: Optional[bytes] = None,
        **kwargs: Any,
    ) -> None:
        return self.fs.pipe(path, value=value, **kwargs)

    def touch(self, path: AnyFSPath, truncate: bool = True, **kwargs: Any) -> None:
        return self.fs.touch(path, truncate=truncate, **kwargs)

    def checksum(self, path: AnyFSPath) -> str:
        return self.fs.checksum(path)

    def copy(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.makedirs(self.parent(to_info))
        self.fs.copy(from_info, to_info)

    def cp_file(self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any) -> None:
        self.fs.cp_file(from_info, to_info, **kwargs)

    @overload
    def exists(
        self,
        path: AnyFSPath,
        callback: fsspec.Callback = ...,
        batch_size: Optional[int] = ...,
    ) -> bool:
        ...

    @overload
    def exists(
        self,
        path: list[AnyFSPath],
        callback: fsspec.Callback = ...,
        batch_size: Optional[int] = ...,
    ) -> list[bool]:
        ...

    def exists(
        self,
        path: Union[AnyFSPath, list[AnyFSPath]],
        callback: fsspec.Callback = DEFAULT_CALLBACK,
        batch_size: Optional[int] = None,
    ) -> Union[bool, list[bool]]:
        if isinstance(path, str):
            return self.fs.exists(path)
        callback.set_size(len(path))
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [self.fs._exists(p) for p in path],
                    batch_size=jobs,
                    callback=callback,
                ),
                loop,
            )
            return fut.result()

        with ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True) as executor:
            it = executor.map(self.fs.exists, path)
            return list(callback.wrap(it))

    def lexists(self, path: AnyFSPath) -> bool:
        return self.fs.lexists(path)

    def symlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.symlink(from_info, to_info)
        except AttributeError as e:
            raise LinkError("symlink", self, from_info) from e

    def link(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.link(from_info, to_info)
        except AttributeError as e:
            raise LinkError("hardlink", self, from_info) from e

    hardlink = link

    def reflink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.reflink(from_info, to_info)
        except AttributeError as e:
            raise LinkError("reflink", self, from_info) from e

    def islink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.islink(path)
        except AttributeError:
            return False

    is_symlink = islink

    def is_hardlink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.is_hardlink(path)
        except AttributeError:
            return False

    def iscopy(self, path: AnyFSPath) -> bool:
        return not (self.is_symlink(path) or self.is_hardlink(path))

    @overload
    def ls(self, path: AnyFSPath, detail: Literal[True], **kwargs) -> "Iterator[Entry]":
        ...

    @overload
    def ls(self, path: AnyFSPath, detail: Literal[False], **kwargs) -> Iterator[str]:
        ...

    def ls(self, path, detail=False, **kwargs):
        return self.fs.ls(path, detail=detail, **kwargs)

    def find(
        self,
        path: Union[AnyFSPath, list[AnyFSPath]],
        prefix: bool = False,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[str]:
        if isinstance(path, str):
            yield from self.fs.find(path)
            return
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [self.fs._find(p) for p in path],
                    batch_size=jobs,
                ),
                loop,
            )
            for result in fut.result():
                yield from result
            return
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            find = partial(self.fs.find)
            for result in executor.imap_unordered(find, path):
                yield from result

    def mv(self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any) -> None:
        self.fs.mv(from_info, to_info)

    move = mv

    def rmdir(self, path: AnyFSPath) -> None:
        self.fs.rmdir(path)

    def rm_file(self, path: AnyFSPath) -> None:
        self.fs.rm_file(path)

    def rm(
        self,
        path: Union[AnyFSPath, list[AnyFSPath]],
        recursive: bool = False,
        **kwargs,
    ) -> None:
        self.fs.rm(path, recursive=recursive, **kwargs)

    remove = rm

    @overload
    def info(
        self,
        path: AnyFSPath,
        callback: fsspec.Callback = ...,
        batch_size: Optional[int] = ...,
        **kwargs,
    ) -> "Entry":
        ...

    @overload
    def info(
        self,
        path: list[AnyFSPath],
        callback: fsspec.Callback = ...,
        batch_size: Optional[int] = ...,
    ) -> list["Entry"]:
        ...

    def info(self, path, callback=DEFAULT_CALLBACK, batch_size=None, **kwargs):
        if isinstance(path, str):
            return self.fs.info(path, **kwargs)
        callback.set_size(len(path))
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [self.fs._info(p, **kwargs) for p in path],
                    batch_size=jobs,
                    callback=callback,
                ),
                loop,
            )
            return fut.result()

        func = partial(self.fs.info, **kwargs)
        with ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True) as executor:
            it = executor.map(func, path)
            return list(callback.wrap(it))

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
        self.fs.mkdir(path, create_parents=create_parents, **kwargs)

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        self.fs.makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def put_file(
        self,
        from_file: Union[AnyFSPath, "BinaryIO"],
        to_info: AnyFSPath,
        callback: fsspec.Callback = DEFAULT_CALLBACK,
        size: Optional[int] = None,
        **kwargs,
    ) -> None:
        if size:
            callback.set_size(size)
        if hasattr(from_file, "read"):
            stream = wrap_file(from_file, callback)
            self.upload_fobj(stream, to_info, size=size)
        else:
            assert isinstance(from_file, str)
            self.fs.put_file(os.fspath(from_file), to_info, callback=callback, **kwargs)
        self.fs.invalidate_cache(self.parent(to_info))

    def get_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: fsspec.Callback = DEFAULT_CALLBACK,
        **kwargs,
    ) -> None:
        self.fs.get_file(from_info, to_info, callback=callback, **kwargs)

    def upload_fobj(self, fobj: IO, to_info: AnyFSPath, **kwargs) -> None:
        self.makedirs(self.parent(to_info))
        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(
                fobj,
                fdest,
                length=getattr(fdest, "blocksize", None),  # type: ignore[arg-type]
            )

    def walk(self, path: AnyFSPath, **kwargs: Any):
        return self.fs.walk(path, **kwargs)

    def glob(self, path: AnyFSPath, **kwargs: Any):
        return self.fs.glob(path, **kwargs)

    def size(self, path: AnyFSPath) -> Optional[int]:
        return self.fs.size(path)

    def sizes(self, paths: list[AnyFSPath]) -> list[Optional[int]]:
        return self.fs.sizes(paths)

    def du(
        self,
        path: AnyFSPath,
        total: bool = True,
        maxdepth: Optional[int] = None,
        **kwargs: Any,
    ) -> Union[int, dict[AnyFSPath, int]]:
        return self.fs.du(path, total=total, maxdepth=maxdepth, **kwargs)

    def put(
        self,
        from_info: Union[AnyFSPath, list[AnyFSPath]],
        to_info: Union[AnyFSPath, list[AnyFSPath]],
        callback: fsspec.Callback = DEFAULT_CALLBACK,
        recursive: bool = False,
        batch_size: Optional[int] = None,
    ):
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.put(
                from_info,
                to_info,
                callback=callback,
                batch_size=jobs,
                recursive=recursive,
            )

        assert not recursive, "not implemented yet"
        from_infos = [from_info] if isinstance(from_info, str) else from_info
        to_infos = [to_info] if isinstance(to_info, str) else to_info

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)

        def put_file(from_path, to_path):
            with callback.branched(from_path, to_path) as child:
                return self.put_file(from_path, to_path, callback=child)

        with executor:
            it = executor.imap_unordered(put_file, from_infos, to_infos)
            list(callback.wrap(it))

    def get(
        self,
        from_info: Union[AnyFSPath, list[AnyFSPath]],
        to_info: Union[AnyFSPath, list[AnyFSPath]],
        callback: fsspec.Callback = DEFAULT_CALLBACK,
        recursive: bool = False,
        batch_size: Optional[int] = None,
    ) -> None:
        # Currently, the implementation is non-recursive if the paths are
        # provided as a list, and recursive if it's a single path.
        from .local import localfs

        def get_file(rpath, lpath, **kwargs):
            localfs.makedirs(localfs.parent(lpath), exist_ok=True)
            with callback.branched(rpath, lpath) as child:
                self.fs.get_file(rpath, lpath, callback=child, **kwargs)

        if isinstance(from_info, list) and isinstance(to_info, list):
            from_infos: list[AnyFSPath] = from_info
            to_infos: list[AnyFSPath] = to_info
        else:
            assert isinstance(from_info, str)
            assert isinstance(to_info, str)

            if not self.isdir(from_info):
                callback.set_size(1)
                get_file(from_info, to_info)
                callback.relative_update()
                return

            from_infos = list(self.find(from_info))
            if not from_infos:
                return localfs.makedirs(to_info, exist_ok=True)

            to_infos = [
                localfs.join(to_info, *self.relparts(info, from_info))
                for info in from_infos
            ]

        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.get(
                from_infos,
                to_infos,
                callback=callback,
                batch_size=jobs,
            )

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            it = executor.imap_unordered(get_file, from_infos, to_infos)
            list(callback.wrap(it))

    def ukey(self, path: AnyFSPath) -> str:
        return self.fs.ukey(path)

    def created(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.created(path)

    def modified(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.modified(path)

    def sign(self, path: AnyFSPath, expiration: int = 100, **kwargs: Any) -> str:
        return self.fs.sign(path, expiration=expiration, **kwargs)


class ObjectFileSystem(FileSystem):
    TRAVERSE_PREFIX_LEN = 3

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        # For object storages make this method a no-op. The original
        # fs.makedirs() method will only check if the bucket exists
        # and create if it doesn't though we don't want to support
        # that behavior, and the check will cost some time so we'll
        # simply ignore all mkdir()/makedirs() calls.
        return None

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
        return None

    def find(
        self,
        path: Union[AnyFSPath, list[AnyFSPath]],
        prefix: bool = False,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[str]:
        if isinstance(path, str):
            paths = [path]
        else:
            paths = path

        def _make_args(paths: list[AnyFSPath]) -> Iterator[tuple[str, str]]:
            for path in paths:
                if prefix and not path.endswith(self.flavour.sep):
                    parent = self.parent(path)
                    yield parent, self.parts(path)[-1]
                else:
                    yield path, ""

        args = list(_make_args(paths))
        if len(args) == 1:
            path, prefix_str = args[0]
            yield from self.fs.find(path, prefix=prefix_str)
            return

        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            loop = get_loop()
            fut = asyncio.run_coroutine_threadsafe(
                batch_coros(
                    [
                        self.fs._find(path, prefix=prefix_str)
                        for path, prefix_str in args
                    ],
                    batch_size=jobs,
                ),
                loop,
            )
            for result in fut.result():
                yield from result
            return
        # NOTE: this is not parallelized yet since imap_unordered does not
        # handle kwargs. We do not actually support any non-async object
        # storages, so this can be addressed when it is actually needed
        for path, prefix_str in args:
            yield from self.fs.find(path, prefix=prefix_str)
