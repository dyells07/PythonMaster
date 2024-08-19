"""Managed process module."""

import json
import logging
import multiprocessing as mp
import os
import shlex
import subprocess
import tempfile
from contextlib import AbstractContextManager, ExitStack
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Union

from funcy import cached_property
from shortuuid import uuid

from dvc_task.utils import makedirs

from .exceptions import TimeoutExpired

logger = logging.getLogger(__name__)


@dataclass
class ProcessInfo:
    """Process information."""

    pid: int
    stdin: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    returncode: Optional[int]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProcessInfo":
        """Construct ProcessInfo from the specified dictionary."""
        return cls(**data)

    @classmethod
    def load(cls, filename: str) -> "ProcessInfo":
        """Construct the process information from a file."""
        with open(filename, encoding="utf-8") as fobj:
            return cls.from_dict(json.load(fobj))

    def asdict(self) -> Dict[str, Any]:
        """Return this info as a dictionary."""
        return asdict(self)

    def dump(self, filename: str) -> None:
        """Dump the process information into a file."""
        directory, file = os.path.split(filename)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=directory,
            prefix=f"{file}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(self.asdict(), tmp)
        os.replace(tmp.name, filename)


class ManagedProcess(AbstractContextManager):
    """Class to manage the specified process with redirected output.

    stdout and stderr will both be redirected to <name>.out.
    Interactive processes (requiring stdin input) are currently unsupported.
    """

    def __init__(
        self,
        args: Union[str, List[str]],
        env: Optional[Dict[str, str]] = None,
        wdir: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Construct a MangedProcess.

        Arguments:
            args: Command to be run.
            env: Optional environment variables.
            wdir: If specified, redirected output files will be placed in
                `wdir`. Defaults to current working directory.
            name: Name to use for this process, if not specified a UUID will be
                generated instead.
        """
        self.args: List[str] = (
            shlex.split(args, posix=os.name == "posix")
            if isinstance(args, str)
            else list(args)
        )
        self.env = env
        self.wdir = wdir
        self.name = name or uuid()
        self.returncode: Optional[int] = None
        self._fd_stack = ExitStack()
        self._proc: Optional[subprocess.Popen] = None

    def __enter__(self):
        if self._proc is None:
            self.run()
        return self

    def __exit__(self, *args, **kwargs):
        self.wait()

    def _close_fds(self):
        with self._fd_stack:
            pass

    def _make_path(self, path: str) -> str:
        return os.path.join(self.wdir, path) if self.wdir else path

    @cached_property
    def stdout_path(self) -> str:
        """Return redirected stdout path."""
        return self._make_path(f"{self.name}.out")

    @cached_property
    def info_path(self) -> str:
        """Return process information file path."""
        return self._make_path(f"{self.name}.json")

    @cached_property
    def pidfile_path(self) -> str:
        """Return process pidfile path."""
        return self._make_path(f"{self.name}.pid")

    @property
    def info(self) -> "ProcessInfo":
        """Return process information."""
        return ProcessInfo(
            pid=self.pid,
            stdin=None,
            stdout=self.stdout_path,
            stderr=None,
            returncode=self.returncode,
        )

    @property
    def pid(self) -> int:
        """Return process PID.

        Raises:
            ValueError: Process is not running.
        """
        if self._proc is None:
            raise ValueError
        return self._proc.pid

    def _make_wdir(self):
        if self.wdir:
            makedirs(self.wdir, exist_ok=True)

    def _dump(self):
        self._make_wdir()
        self.info.dump(self.info_path)

        with open(self.pidfile_path, "w", encoding="utf-8") as fobj:
            fobj.write(str(self.pid))

    def run(self):
        """Run this process."""
        self._make_wdir()
        logger.debug(
            "Appending output to '%s'",
            self.stdout_path,
        )
        stdout = self._fd_stack.enter_context(open(self.stdout_path, "ab"))  # noqa: SIM115
        try:
            self._proc = subprocess.Popen(
                self.args,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=subprocess.STDOUT,
                close_fds=True,
                shell=False,  # noqa: S603
                env=self.env,
            )
            self._dump()
        except Exception:
            if self._proc is not None:
                self._proc.kill()
            self._close_fds()
            raise

    def wait(self, timeout: Optional[int] = None) -> Optional[int]:
        """Block until a process started with `run` has completed.

        Raises:
            TimeoutExpired if `timeout` was set and the process
            did not terminate after `timeout` seconds.
        """
        if self.returncode is not None or self._proc is None:
            return self.returncode
        try:
            self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutExpired(exc.cmd, exc.timeout) from exc
        except KeyboardInterrupt:
            pass
        self.returncode = self._proc.returncode
        self._close_fds()
        self._dump()
        return self.returncode

    @classmethod
    def spawn(cls, *args, **kwargs) -> Optional[int]:
        """Spawn a ManagedProcess command in the background.

        Returns: The spawned process PID.
        """
        proc = _DaemonProcess(
            target=cls._spawn,
            args=args,
            kwargs=kwargs,
            daemon=True,
        )
        proc.start()
        # Do not terminate the child daemon when the main process exits
        mp.process._children.discard(proc)  # type: ignore[attr-defined]
        return proc.pid

    @classmethod
    def _spawn(cls, *args, **kwargs):
        with cls(*args, **kwargs):
            pass


class _DaemonProcess(mp.Process):
    def run(self):
        if os.name != "nt":
            os.setpgid(0, 0)
        super().run()
