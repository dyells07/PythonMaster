import threading
from functools import cached_property
from getpass import getpass
from typing import TYPE_CHECKING, Union

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.errors import ConfigError
from funcy import memoize, wrap_with

if TYPE_CHECKING:
    from ssl import SSLContext


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user):
    return getpass(f"Enter a password for host '{host}' user '{user}':\n")


def make_context(
    ssl_verify: Union[bool, str, None]
) -> Union["SSLContext", bool, None]:
    if isinstance(ssl_verify, bool) or ssl_verify is None:
        return ssl_verify

    # If this is a path, then we will create an
    # SSL context for it, and load the given certificate.
    import ssl

    context = ssl.create_default_context()
    context.load_verify_locations(ssl_verify)
    return context


# pylint: disable=abstract-method
class HTTPFileSystem(FileSystem):
    protocol = "http"
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"aiohttp": "aiohttp", "aiohttp-retry": "aiohttp_retry"}
    CAN_TRAVERSE = False

    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 60

    def __init__(
        self,
        fs=None,
        ssl_verify=None,
        read_timeout=REQUEST_TIMEOUT,
        connect_timeout=REQUEST_TIMEOUT,
        **kwargs,
    ):
        super().__init__(fs, **kwargs)

        self.fs_args["upload_method"] = kwargs.get("method", "POST")
        client_kwargs = self.fs_args.setdefault("client_kwargs", {})
        client_kwargs.update(
            {
                "ssl_verify": ssl_verify,
                "read_timeout": read_timeout,
                "connect_timeout": connect_timeout,
                "trust_env": True,  # Allow reading proxy configs from the env
            }
        )

    def _prepare_credentials(self, **config):
        import aiohttp

        auth_method = config.get("auth")
        if not auth_method:
            return {}

        user = config.get("user")
        password = config.get("password")

        if password is None and config.get("ask_password"):
            password = ask_password(config.get("url"), user or "custom")

        client_kwargs = {}
        if auth_method == "basic":
            if user is None or password is None:
                raise ConfigError(
                    "HTTP 'basic' authentication require both "
                    "'user' and 'password'"
                )
            client_kwargs["auth"] = aiohttp.BasicAuth(user, password)
        elif auth_method == "custom":
            custom_auth_header = config.get("custom_auth_header")
            if custom_auth_header is None or password is None:
                raise ConfigError(
                    "HTTP 'custom' authentication require both "
                    "'custom_auth_header' and 'password'"
                )
            client_kwargs["headers"] = {custom_auth_header: password}
        else:
            raise NotImplementedError(
                f"Auth method {auth_method!r} is not supported."
            )
        return {"client_kwargs": client_kwargs}

    async def get_client(
        self, ssl_verify, read_timeout, connect_timeout, **kwargs
    ):
        import aiohttp
        from aiohttp_retry import ExponentialRetry

        from .retry import ReadOnlyRetryClient

        kwargs["retry_options"] = ExponentialRetry(
            attempts=self.SESSION_RETRIES,
            factor=self.SESSION_BACKOFF_FACTOR,
            max_timeout=self.REQUEST_TIMEOUT,
            exceptions={aiohttp.ClientError},
        )

        # The default total timeout for an aiohttp request is 300 seconds
        # which is too low for DVC's interactions when dealing with large
        # data blobs. We remove the total timeout, and only limit the time
        # that is spent when connecting to the remote server and waiting
        # for new data portions.
        kwargs["timeout"] = aiohttp.ClientTimeout(
            total=None,
            connect=connect_timeout,
            sock_connect=connect_timeout,
            sock_read=read_timeout,
        )

        kwargs["connector"] = aiohttp.TCPConnector(
            # Force cleanup of closed SSL transports.
            # See https://github.com/iterative/dvc/issues/7414
            enable_cleanup_closed=True,
            ssl=make_context(ssl_verify),
        )

        return ReadOnlyRetryClient(**kwargs)

    @cached_property
    def fs(self):
        from .spec import HTTPFileSystem as _HTTPFileSystem

        return _HTTPFileSystem(
            get_client=self.get_client,
            **self.fs_args,
        )

    def unstrip_protocol(self, path: str) -> str:
        return path

    # pylint: disable=arguments-differ

    def find(self, *args, **kwargs):
        raise NotImplementedError

    def isdir(self, *args, **kwargs):
        return False

    def ls(self, *args, **kwargs):
        raise NotImplementedError

    def walk(self, *args, **kwargs):
        raise NotImplementedError

    # pylint: enable=arguments-differ


class HTTPSFileSystem(HTTPFileSystem):  # pylint:disable=abstract-method
    protocol = "https"
