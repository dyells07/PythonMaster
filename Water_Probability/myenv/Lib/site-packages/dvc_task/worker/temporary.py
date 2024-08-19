"""Temporary worker module."""

import logging
import os
import threading
import time
from typing import Any, Dict, List, Mapping, Optional

from celery import Celery
from celery.utils.nodenames import default_nodename

from dvc_task.app.filesystem import FSApp

logger = logging.getLogger(__name__)


class TemporaryWorker:
    """Temporary worker that automatically shuts down when queue is empty."""

    def __init__(
        self,
        app: Celery,
        timeout: int = 60,
        **kwargs,
    ):
        """Construct a worker.

        Arguments:
            app: Celery application instance.
            timeout: Queue timeout in seconds. Worker will be terminated if the
                queue remains empty after timeout.

        Additional keyword arguments will be passed as celery worker
        configuration.
        """
        self.app = app
        self.timeout = timeout
        self.config = kwargs

    def ping(self, name: str, timeout: float = 1.0) -> Optional[List[Dict[str, Any]]]:
        """Ping the specified worker."""
        return self._ping(destination=[default_nodename(name)], timeout=timeout)

    def _ping(
        self, *, destination: Optional[List[str]] = None, timeout: float = 1.0
    ) -> Optional[List[Dict[str, Any]]]:
        return self.app.control.ping(destination=destination, timeout=timeout)

    def start(self, name: str, fsapp_clean: bool = False) -> None:
        """Start the worker if it does not already exist.

        Runs the Celery worker main thread in the current process.

        Arguments:
            name: Celery worker name.
            fsapp_clean: Automatically cleanup FSApp broker on shutdown. Has no
                effect unless app is an FSApp instance.
        """
        if os.name == "nt":
            # see https://github.com/celery/billiard/issues/247
            os.environ["FORKED_BY_MULTIPROCESSING"] = "1"

        if not self.ping(name):
            monitor = threading.Thread(
                target=self.monitor,
                daemon=True,
                args=(name,),
            )
            monitor.start()
            config = dict(self.config)
            config["hostname"] = name
            argv = ["worker"]
            argv.extend(self._parse_config(config))
            self.app.worker_main(argv=argv)
            if fsapp_clean and isinstance(self.app, FSApp):  # type: ignore[unreachable]
                logger.info("cleaning up FSApp broker.")
                self.app.clean()
            logger.info("done")

    @staticmethod
    def _parse_config(config: Mapping[str, Any]) -> List[str]:
        loglevel = config.get("loglevel", "info")
        argv = [f"--loglevel={loglevel}"]
        for key in ("hostname", "pool", "concurrency", "prefetch_multiplier"):
            value = config.get(key)
            if value:
                argv_key = key.replace("_", "-")
                argv.append(f"--{argv_key}={value}")
        for key in (
            "without_heartbeat",
            "without_mingle",
            "without_gossip",
        ):
            if config.get(key):
                argv_key = key.replace("_", "-")
                argv.append(f"--{argv_key}")
        if config.get("task_events"):
            argv.append("-E")
        return argv

    def monitor(self, name: str) -> None:
        """Monitor the worker and stop it when the queue is empty."""
        nodename = default_nodename(name)

        def _tasksets(nodes):
            for taskset in (
                nodes.active(),
                nodes.scheduled(),
                nodes.reserved(),
            ):
                if taskset is not None:
                    yield from taskset.values()

            if isinstance(self.app, FSApp):
                yield from self.app.iter_queued()

        logger.debug("monitor: watching celery worker '%s'", nodename)
        while True:
            time.sleep(self.timeout)
            nodes = self.app.control.inspect(  # type: ignore[call-arg]
                destination=[nodename],
                limit=1,
            )
            if nodes is None or not any(tasks for tasks in _tasksets(nodes)):
                logger.info("monitor: shutting down due to empty queue.")
                break
        logger.debug("monitor: sending shutdown to '%s'.", nodename)
        self.app.control.shutdown()
        logger.debug("monitor: done")
