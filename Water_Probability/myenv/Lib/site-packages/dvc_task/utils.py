"""General utilities."""

import errno
import logging
import os
import shutil
import stat
import sys
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def _chmod(func: Callable, path: str, excinfo: Any):
    perm = os.lstat(path).st_mode
    perm |= stat.S_IWRITE

    try:
        os.chmod(path, perm)
    except OSError as exc:
        # broken symlink or file is not owned by us
        if exc.errno not in [errno.ENOENT, errno.EPERM]:
            raise

    func(path)


def _unlink(path: str, onerror: Callable):
    try:
        os.unlink(path)
    except OSError:
        onerror(os.unlink, path, sys.exc_info())


def remove(path: str):
    """Remove the specified path."""
    logger.debug("Removing '%s'", path)
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, onerror=_chmod)
        else:
            _unlink(path, _chmod)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def makedirs(path: str, exist_ok: bool = False, mode: Optional[int] = None):
    """Make the specified directory and any parent directories."""
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
        if tail == cdir:  # foo/newdir/. exists if foo/newdir exists
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
        logger.debug("failed to chmod '%o' '%s'", mode, path, exc_info=True)


def unc_path(path: str) -> str:
    """Return UNC formatted path.

    Returns the unmodified path on posix platforms.
    """
    # Celery/Kombu URLs only take absolute filesystem paths
    # (UNC paths on windows)
    path = os.path.abspath(path)
    if os.name != "nt":
        return path
    drive, tail = os.path.splitdrive(path)
    if drive.endswith(":"):
        return f"\\\\?\\{drive}{tail}"
    return f"{drive}{tail}"
