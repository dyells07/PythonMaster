"""Serverless process manager."""

import locale
import logging
import os
import signal
import sys
import time
from typing import Dict, Generator, List, Optional, Tuple, Union

from celery import Signature, signature
from funcy.flow import reraise
from shortuuid import uuid

from dvc_task.utils import remove

from .exceptions import ProcessNotTerminatedError, UnsupportedSignalError
from .process import ProcessInfo

logger = logging.getLogger(__name__)


class ProcessManager:
    """Manager for controlling background ManagedProcess(es) via celery.

    Spawned process entries are kept in the manager directory until they
    are explicitly removed (with remove() or cleanup()) so that return
    value and log information can be accessed after a process has completed.
    """

    def __init__(
        self,
        wdir: Optional[str] = None,
    ):
        """Construct a ProcessManager

        Arguments:
            wdir: Directory used for storing process information. Defaults
                to the current working directory.
        """
        self.wdir = wdir or os.curdir

    def __iter__(self) -> Generator[str, None, None]:
        if not os.path.exists(self.wdir):
            return
        yield from os.listdir(self.wdir)

    @reraise(FileNotFoundError, KeyError)
    def __getitem__(self, key: str) -> "ProcessInfo":
        info_path = self._get_info_path(key)
        return ProcessInfo.load(info_path)

    @reraise(FileNotFoundError, KeyError)
    def __setitem__(self, key: str, value: "ProcessInfo"):
        info_path = self._get_info_path(key)
        value.dump(info_path)

    def __delitem__(self, key: str) -> None:
        path = os.path.join(self.wdir, key)
        if os.path.exists(path):
            remove(path)

    def _get_info_path(self, key: str) -> str:
        return os.path.join(self.wdir, key, f"{key}.json")

    def get(self, key: str, default=None) -> "ProcessInfo":
        """Return the specified process."""
        try:
            return self[key]
        except KeyError:
            return default

    def processes(self) -> Generator[Tuple[str, "ProcessInfo"], None, None]:
        """Iterate over managed processes."""
        for name in self:
            try:
                yield name, self[name]
            except KeyError:
                continue

    def run_signature(  # noqa: PLR0913
        self,
        args: Union[str, List[str]],
        name: Optional[str] = None,
        task: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        immutable: bool = False,
    ) -> Signature:
        """Return signature for a task which runs a command in the background.

        Arguments:
            args: Command to run.
            name: Optional name to use for the spawned process.
            task: Optional name of Celery task to use for spawning the process.
                Defaults to 'dvc_task.proc.tasks.run'.
            env: Optional environment to be passed into the process.
            immutable: True if the returned Signature should be immutable.

        Returns:
            Celery signature for the run task.
        """
        name = name or uuid()
        task = task or "dvc_task.proc.tasks.run"
        return signature(
            task,
            args=(args,),
            kwargs={
                "name": name,
                "wdir": os.path.join(self.wdir, name),
                "env": env,
            },
            immutable=immutable,
        )

    def send_signal(self, name: str, sig: int, group: bool = False):  # noqa: C901, PLR0912
        """Send `signal` to the specified named process."""
        try:
            process_info = self[name]
        except KeyError as exc:
            raise ProcessLookupError from exc
        if sys.platform == "win32":
            if sig not in (
                signal.SIGTERM,
                signal.CTRL_C_EVENT,
                signal.CTRL_BREAK_EVENT,
            ):
                raise UnsupportedSignalError(sig)

        def handle_closed_process():
            logging.warning("Process '%s' had already aborted unexpectedly.", name)
            process_info.returncode = -1
            self[name] = process_info

        if process_info.returncode is None:
            try:
                if sys.platform != "win32" and group:
                    pgid = os.getpgid(process_info.pid)
                    os.killpg(pgid, sig)
                else:
                    os.kill(process_info.pid, sig)
            except ProcessLookupError:
                handle_closed_process()
                raise
            except OSError as exc:
                if sys.platform == "win32":
                    if exc.winerror == 87:
                        handle_closed_process()
                        raise ProcessLookupError from exc
                raise
        else:
            raise ProcessLookupError

    def interrupt(self, name: str, group: bool = True):
        """Send interrupt signal to specified named process"""
        if sys.platform == "win32":
            self.send_signal(
                name,
                signal.CTRL_C_EVENT,
                group,
            )
        else:
            self.send_signal(name, signal.SIGINT, group)

    def terminate(self, name: str, group: bool = False):
        """Terminate the specified named process."""
        self.send_signal(name, signal.SIGTERM, group)

    def kill(self, name: str, group: bool = False):
        """Kill the specified named process."""
        if sys.platform == "win32":
            self.send_signal(name, signal.SIGTERM, group)
        else:
            self.send_signal(name, signal.SIGKILL, group)

    def remove(self, name: str, force: bool = False):
        """Remove the specified named process from this manager.

        If the specified process is still running, it will be forcefully killed
        if `force` is True`, otherwise an exception will be raised.

        Raises:
            ProcessNotTerminatedError if the specified process is still
            running and was not forcefully killed.
        """
        try:
            process_info = self[name]
        except KeyError:
            return
        if process_info.returncode is None and not force:
            raise ProcessNotTerminatedError(name)
        try:
            self.kill(name)
        except ProcessLookupError:
            pass
        del self[name]

    def cleanup(self, force: bool = False):
        """Remove stale (terminated) processes from this manager."""
        for name in self:
            try:
                self.remove(name, force)
            except ProcessNotTerminatedError:
                continue

    def follow(
        self,
        name: str,
        encoding: Optional[str] = None,
        sleep_interval: int = 1,
    ) -> Generator[str, None, None]:
        """Iterate over lines in redirected output for a process.

        This will block calling thread when waiting for output (until the
        followed process has exited).

        Arguments:
            name: Process name.
            encoding: Text encoding for redirected output. Defaults to
                `locale.getpreferredencoding()`.
            sleep_interval: Sleep interval for follow iterations (when waiting
                for output).

        Note:
            Yielded strings may not always end in line terminators (all
            available output will yielded if EOF is reached).
        """
        output_path = self[name].stdout
        if output_path is None:
            return
        with open(
            output_path,
            encoding=encoding or locale.getpreferredencoding(),
        ) as fobj:
            while True:
                offset = fobj.tell()
                line = fobj.readline()
                if line:
                    yield line
                else:
                    info = self[name]
                    if info.returncode is not None:
                        return
                    time.sleep(sleep_interval)
                    fobj.seek(offset)
