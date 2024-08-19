import itertools
import logging
from collections.abc import Iterable, Iterator
from contextlib import suppress
from functools import partial
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    Optional,
    Union,
    cast,
)

from fsspec.callbacks import DEFAULT_CALLBACK

from .errors import ObjectDBPermissionError
from .obj import Object

if TYPE_CHECKING:
    from fsspec import Callback

    from .fs.base import AnyFSPath, FileSystem


logger = logging.getLogger(__name__)


def noop(*args, **kwargs):
    pass


def wrap_iter(iterable, callback):
    for index, item in enumerate(iterable, start=1):
        yield item
        callback(index)


class ObjectDB:
    def __init__(self, fs: "FileSystem", path: str, read_only: bool = False, **config):
        self.fs = fs
        self.path = path
        self.read_only = read_only
        self._dirs: Optional[set] = None

    def __eq__(self, other: object):
        return isinstance(other, ObjectDB) and (
            self.fs.protocol == other.fs.protocol
            and self.path == other.path
            and self.read_only == other.read_only
        )

    def __hash__(self):
        return hash((self.fs.protocol, self.path, self.read_only))

    def __repr__(self):
        fs = self.fs
        path = self.path
        read_only = self.read_only
        return f"{self.__class__.__name__}({fs=!r}, {path=!r}, {read_only=!r})"

    def __rich_repr__(self):
        yield "fs", self.fs
        yield "path", self.path
        yield "read_only", self.read_only, False

    def _init(self, dname: str) -> None:
        if self.read_only:
            return

        if self._dirs is None:
            self._dirs = set()
            with suppress(FileNotFoundError, NotImplementedError):
                self._dirs = {
                    self.fs.name(path) for path in self.fs.ls(self.path, detail=False)
                }

        if dname in self._dirs:
            return

        self.makedirs(self.fs.join(self.path, dname))
        self._dirs.add(dname)

    def exists(self, oid: str) -> bool:
        return self.fs.isfile(self.oid_to_path(oid))

    def exists_prefix(self, short_oid: str) -> str:
        path = self.oid_to_path(short_oid)
        if len(short_oid) <= 2:
            raise ValueError(short_oid, [])

        if self.exists(path):
            return short_oid

        prefix, _ = self._oid_parts(short_oid)
        ret = [
            oid
            for oid in self._list_oids(prefixes=[prefix])
            if oid.startswith(short_oid)
        ]
        if not ret:
            raise KeyError(short_oid)
        if len(ret) == 1:
            return ret[0]
        raise ValueError(short_oid, ret)

    def move(self, from_info, to_info):
        self.fs.move(from_info, to_info)

    def makedirs(self, path):
        self.fs.makedirs(path)

    def get(self, oid: str) -> Object:
        return Object(
            self.oid_to_path(oid),
            self.fs,
            oid,
        )

    def add_bytes(self, oid: str, data: Union[bytes, BinaryIO]) -> None:
        if self.read_only:
            raise ObjectDBPermissionError("Cannot add to read-only ODB")

        if isinstance(data, bytes):
            fobj: "BinaryIO" = BytesIO(data)
            size: Optional[int] = len(data)
        else:
            fobj = data
            size = cast("Optional[int]", getattr(fobj, "size", None))

        self._init(self._oid_parts(oid)[0])

        path = self.oid_to_path(oid)
        self.fs.put_file(fobj, path, size=size)

    def add(
        self,
        path: Union["AnyFSPath", list["AnyFSPath"]],
        fs: "FileSystem",
        oid: Union[str, list[str]],
        hardlink: bool = False,
        callback: "Callback" = DEFAULT_CALLBACK,
        check_exists: bool = True,
        on_error: Optional[Callable[[str, BaseException], None]] = None,
        **kwargs,
    ) -> int:
        from dvc_objects.fs import generic

        if self.read_only:
            raise ObjectDBPermissionError("Cannot add to read-only ODB")

        if isinstance(path, str):
            path = [path]
        if isinstance(oid, str):
            oid = [oid]
        assert len(path) == len(oid)

        if check_exists:
            to_add = [
                (from_p, to_oid)
                for from_p, to_oid in zip(path, oid)
                if not self.exists(to_oid)
            ]
        else:
            to_add = list(zip(path, oid))
        if not to_add:
            return 0

        for parts in {self._oid_parts(to_oid)[0] for _, to_oid in to_add}:
            self._init(parts)

        failed = 0

        def _on_error(_from_p: str, _to_p: str, exc: BaseException):
            assert on_error is not None
            nonlocal failed
            failed += 1
            oid = self.path_to_oid(_to_p)
            on_error(oid, exc)

        from_paths, to_oids = zip(*to_add)
        jobs: Optional[int] = kwargs.get("batch_size", kwargs.get("jobs"))

        callback.set_size(len(from_paths))

        generic.transfer(
            fs,
            list(from_paths),
            self.fs,
            [self.oid_to_path(to_oid) for to_oid in to_oids],
            hardlink=hardlink,
            callback=callback,
            batch_size=jobs,
            on_error=_on_error if on_error is not None else None,
        )
        return len(to_add) - failed

    def delete(self, oid: str):
        self.fs.remove(self.oid_to_path(oid))

    def clear(self):
        for oid in self.all():
            self.delete(oid)

    def _oid_parts(self, oid: str) -> tuple[str, str]:
        return oid[:2], oid[2:]

    def oid_to_path(self, oid) -> str:
        return self.fs.join(self.path, *self._oid_parts(oid))

    def _list_prefixes(
        self,
        prefixes: Optional[Iterable[str]] = None,
        jobs: Optional[int] = None,
    ) -> Iterator[str]:
        if prefixes:
            paths: Union[str, list[str]] = list(map(self.oid_to_path, prefixes))
            if len(paths) == 1:
                paths = paths[0]
            prefix = True
        else:
            paths = self.path
            prefix = False
        yield from self.fs.find(paths, batch_size=jobs, prefix=prefix)

    def path_to_oid(self, path) -> str:
        if self.fs.isabs(path):
            self_path = self.fs.abspath(self.path)
        else:
            self_path = self.path
        self_parts = self.fs.parts(self_path)
        parts = self.fs.parts(path)[len(self_parts) :]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def _list_oids(
        self,
        prefixes: Optional[Iterable[str]] = None,
        jobs: Optional[int] = None,
    ) -> Iterator[str]:
        """Iterate over oids in this fs.

        If `prefix` is specified, only oids which begin with `prefix`
        will be returned.
        """
        for path in self._list_prefixes(prefixes=prefixes, jobs=jobs):
            try:
                yield self.path_to_oid(path)
            except ValueError:
                logger.debug("'%s' doesn't look like a cache file, skipping", path)

    def _oids_with_limit(
        self,
        limit: int,
        prefixes: Optional[Iterable[str]] = None,
        jobs: Optional[int] = None,
    ) -> Iterator[str]:
        for i, oid in enumerate(self._list_oids(prefixes=prefixes, jobs=jobs), start=1):
            yield oid
            if i > limit:
                logger.debug(
                    "`_list_oids()` returned max %r oids, skipping remaining results",
                    limit,
                )
                return

    def _max_estimation_size(self, oids):
        # Max remote size allowed for us to use traverse method
        return max(
            self.fs.TRAVERSE_THRESHOLD_SIZE,
            len(oids)
            / self.fs.TRAVERSE_WEIGHT_MULTIPLIER
            * self.fs.LIST_OBJECT_PAGE_SIZE,
        )

    def _estimate_remote_size(self, oids=None, progress=noop):
        """Estimate fs size based on number of entries beginning with
        "00..." prefix.

        Takes a progress callback that returns current_estimated_size.
        """
        prefix = "0" * self.fs.TRAVERSE_PREFIX_LEN
        total_prefixes = pow(16, self.fs.TRAVERSE_PREFIX_LEN)
        if oids:
            max_oids = self._max_estimation_size(oids)
        else:
            max_oids = None

        def iter_with_pbar(oids):
            total = 0
            for oid in oids:
                total += total_prefixes
                progress(total)
                yield oid

        if max_oids:
            oids = self._oids_with_limit(max_oids / total_prefixes, prefixes=[prefix])
        else:
            oids = self._list_oids(prefixes=[prefix])

        remote_oids = set(iter_with_pbar(oids))
        if remote_oids:
            remote_size = total_prefixes * len(remote_oids)
        else:
            remote_size = total_prefixes
        logger.debug("Estimated remote size: %s files", remote_size)
        return remote_size, remote_oids

    def _list_oids_traverse(self, remote_size, remote_oids, jobs=None):
        """Iterate over all oids found in this fs.
        Hashes are fetched in parallel according to prefix, except in
        cases where the remote size is very small.

        All oids from the remote (including any from the size
        estimation step passed via the `remote_oids` argument) will be
        returned.

        NOTE: For large remotes the list of oids will be very
        big(e.g. 100M entries, md5 for each is 32 bytes, so ~3200Mb list)
        and we don't really need all of it at the same time, so it makes
        sense to use a generator to gradually iterate over it, without
        keeping all of it in memory.
        """
        num_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        if num_pages < 256 / self.fs.jobs:
            # Fetching prefixes in parallel requires at least 255 more
            # requests, for small enough remotes it will be faster to fetch
            # entire cache without splitting it into prefixes.
            #
            # NOTE: this ends up re-fetching oids that were already
            # fetched during remote size estimation
            traverse_prefixes = None
        else:
            yield from remote_oids
            traverse_prefixes = [f"{i:02x}" for i in range(1, 256)]
            if self.fs.TRAVERSE_PREFIX_LEN > 2:
                traverse_prefixes += [
                    "{0:0{1}x}".format(i, self.fs.TRAVERSE_PREFIX_LEN)
                    for i in range(1, pow(16, self.fs.TRAVERSE_PREFIX_LEN - 2))
                ]

        yield from self._list_oids(prefixes=traverse_prefixes, jobs=jobs)

    def all(self, jobs=None):
        """Iterate over all oids in this fs.

        Hashes will be fetched in parallel threads according to prefix
        (except for small remotes) and a progress bar will be displayed.
        """
        if not self.fs.CAN_TRAVERSE:
            return self._list_oids(jobs=jobs)

        remote_size, remote_oids = self._estimate_remote_size()
        return self._list_oids_traverse(remote_size, remote_oids, jobs=jobs)

    def list_oids_exists(self, oids: Iterable[str], jobs: Optional[int] = None):
        """Return list of the specified oids which exist in this fs.
        Hashes will be queried individually.
        """
        paths = list(map(self.oid_to_path, oids))
        logger.debug("Querying %s oids via object_exists", len(paths))
        in_remote = self.fs.exists(paths, batch_size=jobs)
        yield from itertools.compress(oids, in_remote)

    def oids_exist(self, oids, jobs=None, progress=noop):
        """Check if the given oids are stored in the remote.

        There are two ways of performing this check:

        - Traverse method: Get a list of all the files in the remote
            (traversing the cache directory) and compare it with
            the given oids. Cache entries will be retrieved in parallel
            threads according to prefix (i.e. entries starting with, "00...",
            "01...", and so on) and a progress bar will be displayed.

        - Exists method: For each given oid, run the `exists`
            method and filter the oids that aren't on the remote.
            This is done in parallel threads.
            It also shows a progress bar when performing the check.

        The reason for such an odd logic is that most of the remotes
        take much shorter time to just retrieve everything they have under
        a certain prefix (e.g. s3, gs, ssh, hdfs). Other remotes that can
        check if particular file exists much quicker, use their own
        implementation of oids_exist (see ssh, local).

        Which method to use will be automatically determined after estimating
        the size of the remote cache, and comparing the estimated size with
        len(oids). To estimate the size of the remote cache, we fetch
        a small subset of cache entries (i.e. entries starting with "00...").
        Based on the number of entries in that subset, the size of the full
        cache can be estimated, since the cache is evenly distributed according
        to oid.

        Takes a callback that returns value in the format of:
        (phase, total, current). The phase can be {"estimating, "querying"}.

        Returns:
            A list with oids that were found in the remote
        """
        # Remotes which do not use traverse prefix should override
        # oids_exist() (see ssh, local)
        assert self.fs.TRAVERSE_PREFIX_LEN >= 2

        # During the tests, for ensuring that the traverse behavior
        # is working we turn on this option. It will ensure the
        # _list_oids_traverse() is called.
        always_traverse = getattr(self.fs, "_ALWAYS_TRAVERSE", False)

        oids = set(oids)
        if (len(oids) == 1 or not self.fs.CAN_TRAVERSE) and not always_traverse:
            remote_oids = self.list_oids_exists(oids, jobs=jobs)
            callback = partial(progress, "querying", len(oids))
            return list(wrap_iter(remote_oids, callback))

        # Max remote size allowed for us to use traverse method

        estimator_cb = partial(progress, "estimating", None)
        remote_size, remote_oids = self._estimate_remote_size(
            oids, progress=estimator_cb
        )

        traverse_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        # For sufficiently large remotes, traverse must be weighted to account
        # for performance overhead from large lists/sets.
        # From testing with S3, for remotes with 1M+ files, object_exists is
        # faster until len(oids) is at least 10k~100k
        if remote_size > self.fs.TRAVERSE_THRESHOLD_SIZE:
            traverse_weight = traverse_pages * self.fs.TRAVERSE_WEIGHT_MULTIPLIER
        else:
            traverse_weight = traverse_pages
        if len(oids) < traverse_weight and not always_traverse:
            logger.debug(
                "Large remote (%r oids < %r traverse weight), "
                "using object_exists for remaining oids",
                len(oids),
                traverse_weight,
            )
            remaining_oids = oids - remote_oids
            ret = list(oids & remote_oids)
            callback = partial(progress, "querying", len(remaining_oids))
            ret.extend(
                wrap_iter(self.list_oids_exists(remaining_oids, jobs=jobs), callback)
            )
            return ret

        logger.debug("Querying %r oids via traverse", len(oids))
        remote_oids = self._list_oids_traverse(remote_size, remote_oids, jobs=jobs)
        callback = partial(progress, "querying", remote_size)
        return list(oids & set(wrap_iter(remote_oids, callback)))
