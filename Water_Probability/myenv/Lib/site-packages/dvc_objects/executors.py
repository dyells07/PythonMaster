import asyncio
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from concurrent import futures
from itertools import islice
from typing import Any, Callable, Optional, TypeVar

from fsspec import Callback

_T = TypeVar("_T")


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __init__(
        self, max_workers: Optional[int] = None, cancel_on_error: bool = False, **kwargs
    ):
        super().__init__(max_workers=max_workers, **kwargs)
        self._cancel_on_error = cancel_on_error

    def imap_unordered(
        self, fn: Callable[..., _T], *iterables: Iterable[Any]
    ) -> Iterator[_T]:
        """Lazier version of map that does not preserve ordering of results.

        It does not create all the futures at once to reduce memory usage.
        """
        it = zip(*iterables)
        if self._max_workers == 1:
            for args in it:
                yield fn(*args)
            return

        def create_taskset(n: int) -> set[futures.Future]:
            return {self.submit(fn, *args) for args in islice(it, n)}

        tasks = create_taskset(self._max_workers * 5)
        while tasks:
            done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
            for fut in done:
                yield fut.result()
            tasks.update(create_taskset(len(done)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        cancel_futures = self._cancel_on_error and exc_val is not None
        self.shutdown(wait=True, cancel_futures=cancel_futures)
        return False


async def batch_coros(  # noqa: C901
    coros: Sequence[Coroutine],
    batch_size: Optional[int] = None,
    callback: Optional[Callback] = None,
    timeout: Optional[int] = None,
    return_exceptions: bool = False,
    nofiles: bool = False,
) -> list[Any]:
    """Run the given in coroutines in parallel.

    The asyncio loop will be kept saturated with up to `batch_size` tasks in
    the loop at a time.

    Tasks are not guaranteed to run in order, but results are returned in the
    original order.
    """
    from fsspec.asyn import _get_batch_size

    if batch_size is None:
        batch_size = _get_batch_size(nofiles=nofiles)
    if batch_size == -1:
        batch_size = len(coros)
    assert batch_size > 0

    def create_taskset(n: int) -> dict[asyncio.Task, int]:
        return {asyncio.create_task(coro): i for i, coro in islice(it, n)}

    it: Iterator[tuple[int, Coroutine]] = enumerate(coros)
    tasks = create_taskset(batch_size)
    results: dict[int, Any] = {}
    while tasks:
        done, pending = await asyncio.wait(
            tasks.keys(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done and timeout:
            for pending_fut in pending:
                pending_fut.cancel()
            raise TimeoutError
        for fut in done:
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                if not return_exceptions:
                    for pending_fut in pending:
                        pending_fut.cancel()
                    raise
                result = exc
            index = tasks.pop(fut)
            results[index] = result
            if callback is not None:
                callback.relative_update()

        tasks.update(create_taskset(len(done)))

    return [results[k] for k in sorted(results)]
