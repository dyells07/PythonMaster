from functools import wraps
from typing import TYPE_CHECKING, BinaryIO, cast

if TYPE_CHECKING:
    import fsspec


class CallbackStream:
    def __init__(self, stream, callback: "fsspec.Callback"):
        self.stream = stream

        @wraps(stream.read)
        def read(*args, **kwargs):
            data = stream.read(*args, **kwargs)
            callback.relative_update(len(data))
            return data

        self.read = read

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


def wrap_file(file, callback: "fsspec.Callback") -> BinaryIO:
    return cast(BinaryIO, CallbackStream(file, callback))
