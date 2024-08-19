import logging
import os
import shutil

import fsspec

from . import system
from .base import FileSystem
from .utils import copyfile, makedirs, move, remove, tmp_fname

logger = logging.getLogger(__name__)


class FsspecLocalFileSystem(fsspec.AbstractFileSystem):
    sep = os.sep

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fs = fsspec.filesystem("file")

    def makedirs(self, path, exist_ok=False):
        self.fs.makedirs(path, exist_ok=exist_ok)

    def mkdir(self, path, create_parents=True, **kwargs):
        self.fs.mkdir(path, create_parents=create_parents, **kwargs)

    def lexists(self, path, **kwargs):
        return self.fs.lexists(path, **kwargs)

    def exists(self, path, **kwargs):
        # TODO: replace this with os.path.exists once the problem is fixed on
        # the fsspec https://github.com/intake/filesystem_spec/issues/742
        return self.lexists(path)

    def checksum(self, path) -> str:
        return self.fs.checksum(path)

    def info(self, path, **kwargs):
        return self.fs.info(path)

    def ls(self, path, **kwargs):
        return self.fs.ls(path, **kwargs)

    def isfile(self, path) -> bool:
        return os.path.isfile(path)

    def isdir(self, path) -> bool:
        return os.path.isdir(path)

    def walk(self, path, maxdepth=None, topdown=True, detail=False, **kwargs):
        """Directory fs generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            path,
            topdown=topdown,
        ):
            if detail:
                dirs_dict = {name: self.info(os.path.join(root, name)) for name in dirs}
                files_dict = {
                    name: self.info(os.path.join(root, name)) for name in files
                }
                yield (
                    os.path.normpath(root),
                    dirs_dict,
                    files_dict,
                )
                # NOTE: with os.walk you can modify dirs to avoid walking
                # into them. This is us emulating that behaviour for
                # dicts returned by detail=True.
                dirs[:] = list(dirs_dict.keys())
            else:
                yield os.path.normpath(root), dirs, files

    def find(self, path, **kwargs):
        for root, _, files in self.walk(path, **kwargs):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{file}"

    @classmethod
    def _parent(cls, path):
        return os.path.dirname(path)

    def put_file(self, lpath, rpath, callback=None, **kwargs):
        parent = self._parent(rpath)
        makedirs(parent, exist_ok=True)
        tmp_file = os.path.join(parent, tmp_fname())
        copyfile(lpath, tmp_file, callback=callback)
        os.replace(tmp_file, rpath)

    def get_file(self, rpath, lpath, callback=None, **kwargs):
        if self.isdir(rpath):
            # emulating fsspec's localfs.get_file
            self.makedirs(lpath, exist_ok=True)
            return

        copyfile(rpath, lpath, callback=callback)

    def mv(self, path1, path2, **kwargs):
        self.makedirs(self._parent(path2), exist_ok=True)
        move(path1, path2)

    def rmdir(self, path):
        self.fs.rmdir(path)

    def rm_file(self, path):
        remove(path)

    def rm(self, path, recursive=False, maxdepth=None):
        if isinstance(path, str):
            path = [path]
        for p in path:
            remove(p)

    def cp_file(self, path1, path2, **kwargs):
        return self.copy(path1, path2, **kwargs)

    def copy(self, path1, path2, recursive=False, on_error=None, **kwargs):
        tmp_info = os.path.join(self._parent(path2), tmp_fname(""))
        try:
            copyfile(path1, tmp_info)
            os.rename(tmp_info, path2)
        except Exception:
            self.rm_file(tmp_info)
            raise

    def open(self, path, mode="r", encoding=None, **kwargs):
        return open(path, mode=mode, encoding=encoding)  # noqa: SIM115

    def symlink(self, path1, path2):
        return self.fs.symlink(path1, path2)

    def islink(self, path):
        return self.fs.islink(path)

    @staticmethod
    def is_hardlink(path):
        return system.is_hardlink(path)

    def link(self, path1, path2):
        # If there are a lot of empty files (which happens a lot in datasets),
        # and the cache type is `hardlink`, we might reach link limits and
        # will get something like: `too many links error`
        #
        # This is because all those empty files will have the same hash
        # (i.e. 68b329da9893e34099c7d8ad5cb9c940), therefore, they will be
        # linked to the same file in the cache.
        #
        # From https://en.wikipedia.org/wiki/Hard_link
        #   * ext4 limits the number of hard links on a file to 65,000
        #   * Windows with NTFS has a limit of 1024 hard links on a file
        #
        # That's why we simply create an empty file rather than a link.
        if self.size(path1) == 0:
            self.open(path2, "w").close()

            logger.debug("Created empty file: %s -> %s", path1, path2)
            return

        return system.hardlink(path1, path2)

    def reflink(self, path1, path2):
        return system.reflink(path1, path2)

    def created(self, path):
        return self.fs.created(path)

    def modified(self, path):
        return self.fs.modified(path)


class LocalFileSystem(FileSystem):
    sep = os.sep

    flavour = os.path
    protocol = "local"
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, fs=None, **kwargs):
        fs = fs or FsspecLocalFileSystem(**kwargs)
        super().__init__(fs, **kwargs)

    def getcwd(self):
        return os.getcwd()

    def normpath(self, path: str) -> str:
        return self.flavour.normpath(path)

    def realpath(self, path: str) -> str:
        return self.flavour.realpath(path)

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(self.parent(to_info))
        tmp_info = self.join(self.parent(to_info), tmp_fname(""))
        try:
            with open(tmp_info, "wb+") as fdest:
                shutil.copyfileobj(fobj, fdest)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise


localfs = LocalFileSystem()
