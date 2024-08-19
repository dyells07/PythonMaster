from aiohttp_retry import ExponentialRetry, RetryClient


class ReadOnlyRetryClient(RetryClient):
    """Disables retries for PUT/POST requests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._write_opts = ExponentialRetry(
            attempts=1, retry_all_server_errors=False
        )

    def post(self, *args, **kwargs):
        kwargs["retry_options"] = self._write_opts
        return super().post(*args, **kwargs)

    def put(self, *args, **kwargs):
        kwargs["retry_options"] = self._write_opts
        return super().put(*args, **kwargs)
