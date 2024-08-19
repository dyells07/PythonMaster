import errno
import logging
import os
import shutil
import stat
import sys
import threading
from collections.abc import Collection, Iterator
from concurrent import futures
from contextlib import contextmanager, suppress
from secrets import token_urlsafe
from typing import TYPE_CHECKING, Any, Optional, Union

from fsspec.callbacks import DEFAULT_CALLBACK

from dvc_objects.executors import ThreadPoolExecutor

from . import system
from .callbacks import wrap_file

if TYPE_CHECKING:
    from fsspec import Callback

    from .base import AnyFSPath, FileSystem


logger = logging.getLogger(__name__)

umask = os.umask(0)
os.umask(umask)

LOCAL_CHUNK_SIZE = 2**20  # 1 MB
COPY_PBAR_MIN_SIZE = 2**30  # 1 GB


def is_exec(mode: int) -> bool:
    return bool(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))


def relpath(path: "AnyFSPath", start: "AnyFSPath" = os.curdir) -> "AnyFSPath":
    path = os.fspath(path)
    start = os.path.abspath(os.fspath(start))

    # Windows path on different drive than curdir doesn't have relpath
    if os.name == "nt":
        # Since python 3.8 os.realpath resolves network shares to their UNC
        # path. So, to be certain that relative paths correctly captured,
        # we need to resolve to UNC path first. We resolve only the drive
        # name so that we don't follow any 'real' symlinks on the path
        def resolve_network_drive_windows(path_to_resolve):
            drive, tail = os.path.splitdrive(path_to_resolve)
            return os.path.join(os.path.realpath(drive), tail)

        path = resolve_network_drive_windows(os.path.abspath(path))
        start = resolve_network_drive_windows(start)
        if not os.path.commonprefix([start, path]):
            return path
    return os.path.relpath(path, start)


def move(src: "AnyFSPath", dst: "AnyFSPath") -> None:
    """Atomically move src to dst and chmod it with mode.

    Moving is performed in two stages to make the whole operation atomic in
    case src and dst are on different filesystems and actual physical copying
    of data is happening.
    """
    dst = os.path.abspath(dst)
    tmp = tmp_fname(dst)

    if os.path.islink(src):
        shutil.copy(src, tmp)
        _unlink(src, _chmod)
    else:
        shutil.move(src, tmp)

    shutil.move(tmp, dst)


def _chmod(func, p, excinfo):
    perm = os.lstat(p).st_mode
    perm |= stat.S_IWRITE

    try:
        os.chmod(p, perm)
    except OSError as exc:
        # broken symlink or file is not owned by us
        if exc.errno not in [errno.ENOENT, errno.EPERM]:
            raise

    func(p)


def _unlink(path: "AnyFSPath", onerror):
    try:
        os.unlink(path)
    except OSError:
        onerror(os.unlink, path, sys.exc_info())


def remove(path: "AnyFSPath") -> None:
    logger.debug("Removing '%s'", path)

    try:
        if os.path.isdir(path):
            shutil.rmtree(path, onerror=_chmod)
        else:
            _unlink(path, _chmod)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def makedirs(path, exist_ok: bool = False, mode: Optional[int] = None) -> None:
    if mode is None:
        os.makedirs(path, exist_ok=exist_ok)
        return

    # Modified version of os.makedirs() with support for extended mode
    # (e.g. S_ISGID)
    head, tail = os.path.split(path)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not os.path.exists(head):
        try:
            makedirs(head, exist_ok=exist_ok, mode=mode)
        except FileExistsError:
            # Defeats race condition when another thread created the path
            pass
        cdir = os.curdir
        if isinstance(tail, bytes):
            cdir = bytes(os.curdir, "ASCII")  # type: ignore[assignment]
        if tail == cdir:  # xxx/newdir/. exists if xxx/newdir exists
            return
    try:
        os.mkdir(path, mode)
    except OSError:
        # Cannot rely on checking for EEXIST, since the operating system
        # could give priority to other errors like EACCES or EROFS
        if not exist_ok or not os.path.isdir(path):
            raise

    try:
        os.chmod(path, mode)
    except OSError:
        logger.debug(  # type: ignore[attr-defined]
            "failed to chmod '%o' '%s'", mode, path, exc_info=True
        )


def copyfile(
    src: "AnyFSPath",
    dest: "AnyFSPath",
    callback: "Callback" = DEFAULT_CALLBACK,
) -> None:
    """Copy file with progress bar"""
    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    try:
        system.reflink(src, dest)
        # NOTE: reflink may or may not clone src permissions
        os.chmod(dest, 0o666 & ~umask)
        return
    except OSError:
        pass

    total = os.path.getsize(src)
    if total < COPY_PBAR_MIN_SIZE:
        shutil.copyfile(src, dest)
        return

    callback.set_size(total)
    with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
        wrapped = wrap_file(fsrc, callback)
        shutil.copyfileobj(wrapped, fdest, length=LOCAL_CHUNK_SIZE)


def tmp_fname(prefix: str = "") -> str:
    """Temporary name for a partial download"""
    return f"{prefix}.{token_urlsafe(16)}.tmp"


@contextmanager
def as_atomic(
    fs: "FileSystem", to_info: "AnyFSPath", create_parents: bool = False
) -> Iterator["AnyFSPath"]:
    parent = fs.parent(to_info)
    if create_parents:
        fs.makedirs(parent, exist_ok=True)

    tmp_info = fs.join(parent, tmp_fname())
    try:
        yield tmp_info
    except BaseException:
        # Handle stuff like KeyboardInterrupt
        # as well as other errors that might
        # arise during file transfer.
        with suppress(FileNotFoundError):
            fs.remove(tmp_info)
        raise
    else:
        fs.move(tmp_info, to_info)


def exists(
    fs: "FileSystem",
    file_paths: Union["AnyFSPath", Collection["AnyFSPath"]],
    callback: "Callback" = DEFAULT_CALLBACK,
    batch_size: Optional[int] = None,
) -> dict[str, bool]:
    """Return batched fs.exists results.

    Runs batched fs.exists() calls in parallel with fs.ls() until all paths
    have been checked.
    """
    if not file_paths:
        return {}
    paths = {file_paths} if isinstance(file_paths, str) else set(file_paths)
    if len(paths) == 1:
        path = paths.pop()
        return {path: fs.exists(path)}

    paths_lock = threading.Lock()
    results: dict[str, bool] = {}
    results_lock = threading.Lock()
    callback.set_size(len(paths))
    jobs = batch_size or fs.jobs
    exists_jobs = jobs - 1 if jobs > 1 else 1
    executor = ThreadPoolExecutor(max_workers=2, cancel_on_error=True)
    logger.debug("Querying status for '%d' files", len(paths))
    exist_fut = executor.submit(
        _exist_query,
        fs,
        paths,
        paths_lock,
        results,
        results_lock,
        exists_jobs,
        callback,
    )
    list_fut = executor.submit(
        _list_query,
        fs,
        paths,
        paths_lock,
        results,
        results_lock,
        callback,
    )
    _done, not_done = futures.wait(
        [exist_fut, list_fut], return_when=futures.FIRST_COMPLETED
    )
    for fut in not_done:
        fut.cancel()
    # NOTE: if we started a long running lsdir it will continue to run in
    # the background until the task completes
    executor.shutdown(wait=False)
    return results


def _exist_query(
    fs: "FileSystem",
    paths: set["AnyFSPath"],
    paths_lock: threading.Lock,
    results: dict[str, bool],
    results_lock: threading.Lock,
    batch_size: int,
    callback: "Callback",
):
    while True:
        with paths_lock:
            if not paths:
                return
            batch = [paths.pop() for _ in range(batch_size) if paths]
        for i, result in enumerate(fs.exists(batch, batch_size=batch_size)):
            with results_lock:
                path = batch[i]
                if path not in results:
                    results[path] = result
                    callback.relative_update()


def _list_query(
    fs: "FileSystem",
    paths: set["AnyFSPath"],
    paths_lock: threading.Lock,
    results: dict[str, bool],
    results_lock: threading.Lock,
    callback: "Callback",
):
    with paths_lock:
        parents = {fs.parent(path) for path in paths}
    for parent in parents:
        with paths_lock:
            if not paths:
                return
        kwargs: dict[str, Any] = {}
        if fs.version_aware:
            kwargs["versions"] = True
        contents = fs.ls(parent, **kwargs)
        with paths_lock:
            exist_paths = set()
            for path in contents:
                if path in paths:
                    paths.remove(path)
                    exist_paths.add(path)
        with results_lock:
            for path in exist_paths:
                if path not in results:
                    results[path] = True
                    callback.relative_update()
    with paths_lock, results_lock:
        while paths:
            path = paths.pop()
            if path not in results:
                results[path] = False
                callback.relative_update()
