"""(Local) filesystem based Celery application."""

import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, cast

from celery import Celery
from kombu.message import Message
from kombu.transport.filesystem import LOCK_SH, lock, unlock
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads

from dvc_task.utils import makedirs, remove, unc_path

logger = logging.getLogger(__name__)


def _get_fs_config(
    wdir: str,
    mkdir: bool = False,
    task_serializer: str = "json",
    result_serializer: str = "json",
) -> Dict[str, Any]:
    broker_path = os.path.join(wdir, "broker")
    broker_control_path = unc_path(os.path.join(broker_path, "control"))
    broker_in_path = unc_path(os.path.join(broker_path, "in"))
    broker_processed_path = unc_path(os.path.join(broker_path, "processed"))
    result_path = os.path.join(wdir, "result")

    if mkdir:
        for path in (
            broker_control_path,
            broker_in_path,
            broker_processed_path,
            result_path,
        ):
            makedirs(path, exist_ok=True)

    return {
        "broker_url": "filesystem://",
        "broker_transport_options": {
            "control_folder": broker_control_path,
            "data_folder_in": broker_in_path,
            "data_folder_out": broker_in_path,
            "processed_folder": broker_processed_path,
            "store_processed": True,
        },
        "result_backend": f"file://{unc_path(result_path)}",
        "result_persistent": True,
        "task_serializer": task_serializer,
        "result_serializer": result_serializer,
        "accept_content": [task_serializer],
    }


class FSApp(Celery):
    """Local filesystem-based Celery application.

    Uses Kombu filesystem:// broker and results backend
    """

    def __init__(
        self,
        *args,
        wdir: Optional[str] = None,
        mkdir: bool = False,
        task_serializer: str = "json",
        result_serializer: str = "json",
        **kwargs: Any,
    ):
        """Construct an FSApp.

        Arguments:
            wdir: App broker/results directory. Defaults to current working
                directory.
            mkdir: Create broker/results subdirectories if they do not already
                exist.
            task_serializer: Default task serializer.
            result_serializer: Default result serializer.

        Additional arguments will be passed into the Celery constructor.
        """
        super().__init__(*args, **kwargs)
        self.wdir = wdir or os.getcwd()
        self.conf.update(
            _get_fs_config(
                self.wdir,
                mkdir=mkdir,
                task_serializer=task_serializer,
                result_serializer=result_serializer,
            )
        )
        logger.debug("Initialized filesystem:// app in '%s'", wdir)
        self._processed_msg_path_cache: Dict[str, str] = {}
        self._queued_msg_path_cache: Dict[str, str] = {}

    def __reduce_keys__(self) -> Dict[str, Any]:
        keys = super().__reduce_keys__()  # type: ignore[misc]
        keys.update({"wdir": self.wdir})
        return keys

    def _iter_folder(
        self,
        folder_name: str,
        path_cache: Dict[str, str],
        queue: Optional[str] = None,
    ) -> Iterator[Message]:
        """Iterate over queued tasks inside a folder

        Arguments:
            folder_name: the folder to iterate
            path_cache: cache of message path.
            queue: Optional name of queue.
        """
        with self.connection_for_read() as conn:  # type: ignore[attr-defined]
            with conn.channel() as channel:  # type: ignore[attr-defined]
                folder = getattr(channel, folder_name)
                for filename in sorted(os.listdir(folder)):
                    path = os.path.join(folder, filename)
                    try:
                        with open(path, "rb") as fobj:
                            lock(fobj, LOCK_SH)
                            try:
                                payload = fobj.read()
                            finally:
                                unlock(fobj)
                    except FileNotFoundError:
                        # Messages returned by `listdir` call may have been
                        # acknowledged and moved to `processed_folder` by the
                        # time we try to read them here
                        continue
                    if not payload:
                        continue
                    msg = channel.Message(loads(bytes_to_str(payload)), channel=channel)
                    path_cache[msg.delivery_tag] = path
                    if queue is None:
                        yield msg
                    else:
                        delivery_info = msg.properties.get("delivery_info", {})
                        if delivery_info.get("routing_key") == queue:
                            yield msg

    def _iter_data_folder(self, queue: Optional[str] = None) -> Iterator[Message]:
        yield from self._iter_folder(
            "data_folder_in", self._queued_msg_path_cache, queue=queue
        )

    def _iter_processed_folder(self, queue: Optional[str] = None) -> Iterator[Message]:
        yield from self._iter_folder(
            "processed_folder", self._processed_msg_path_cache, queue=queue
        )

    def iter_queued(self, queue: Optional[str] = None) -> Iterator[Message]:
        """Iterate over queued tasks which have not been taken by a worker.

        Arguments:
            queue: Optional name of queue.
        """
        queue = queue or self.conf.task_default_queue
        yield from self._iter_data_folder(queue=queue)

    def iter_processed(self, queue: Optional[str] = None) -> Iterator[Message]:
        """Iterate over tasks which have been taken by a worker.

        Arguments:
            queue: Optional name of queue.
        """
        queue = queue or self.conf.task_default_queue
        yield from self._iter_processed_folder(queue=queue)

    @staticmethod
    def _delete_msg(
        delivery_tag: str,
        msg_collection: Iterable[Message],
        path_cache: Dict[str, str],
    ):
        """delete the specified message.

        Arguments:
            delivery_tag: delivery tag of the message to be deleted.
            msg_collection: where to found this message.
            path_cache: cache of message path.

        Raises:
            ValueError: Invalid delivery_tag
        """
        path = path_cache.get(delivery_tag)
        if path and os.path.exists(path):
            remove(path)
            del path_cache[delivery_tag]
            return

        for msg in msg_collection:
            if msg.delivery_tag == delivery_tag:
                remove(path_cache[delivery_tag])
                del path_cache[delivery_tag]
                return
        raise ValueError(f"Message '{delivery_tag}' not found")

    def reject(self, delivery_tag: str):
        """Reject the specified message.

        Allows the caller to reject FS broker messages without establishing a
        full Kombu consumer. Requeue is not supported.

        Raises:
            ValueError: Invalid delivery_tag
        """
        self._delete_msg(delivery_tag, self.iter_queued(), self._queued_msg_path_cache)

    def purge(self, delivery_tag: str):
        """Purge the specified processed message.

        Allows the caller to purge completed FS broker messages without
        establishing a full Kombu consumer. Requeue is not supported.

        Raises:
            ValueError: Invalid delivery_tag
        """
        self._delete_msg(
            delivery_tag, self.iter_processed(), self._processed_msg_path_cache
        )

    def _gc(self, exclude: Optional[List[str]] = None):
        """Garbage collect expired FS broker messages.

        Arguments:
            exclude: Exclude (do not garbage collect) messages from the specified
                queues.
        """

        def _delete_expired(
            msg: Message,
            queues: Set[str],
            now: float,
            cache: Dict[str, str],
            include_tickets: bool = False,
        ):
            assert isinstance(msg.properties, dict)
            properties = cast(Dict[str, Any], msg.properties)
            delivery_info: Dict[str, str] = properties.get("delivery_info", {})
            if queues:
                routing_key = delivery_info.get("routing_key")
                if routing_key and routing_key in queues:
                    return
            headers = cast(Dict[str, Any], msg.headers)
            expires: Optional[float] = headers.get("expires")
            ticket = msg.headers.get("ticket")
            if include_tickets and ticket or (expires is not None and expires <= now):
                assert msg.delivery_tag
                try:
                    self._delete_msg(msg.delivery_tag, [], cache)
                except ValueError:
                    pass

        queues = set(exclude) if exclude else set()
        now = datetime.now().timestamp()  # noqa: DTZ005
        for msg in self._iter_data_folder():
            _delete_expired(msg, queues, now, self._queued_msg_path_cache)
        for msg in self._iter_processed_folder():
            _delete_expired(
                msg, queues, now, self._processed_msg_path_cache, include_tickets=True
            )

    def clean(self):
        """Clean extraneous celery messages from this FSApp."""
        self._gc(exclude=[self.conf.task_default_queue])
        self._clean_pidbox(f"reply.{self.conf.task_default_queue}.pidbox")

    def _clean_pidbox(self, exchange: str):
        """Clean pidbox replies for the specified exchange."""

        def _delete_replies(msg: Message, exchange: str, cache: Dict[str, str]):
            assert isinstance(msg.properties, dict)
            properties = cast(Dict[str, Any], msg.properties)
            delivery_info: Dict[str, str] = properties.get("delivery_info", {})
            if delivery_info.get("exchange", "") == exchange:
                assert msg.delivery_tag
                try:
                    self._delete_msg(msg.delivery_tag, [], cache)
                except ValueError:
                    pass

        for msg in self._iter_data_folder():
            _delete_replies(msg, exchange, self._queued_msg_path_cache)
