import ssl

import pytest
from dvc.fs import HTTPFileSystem
from fsspec.asyn import get_loop as get_fsspec_loop
from fsspec.asyn import sync


def test_public_auth_method():
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "user": "",
        "password": "",
    }

    fs = HTTPFileSystem(**config)

    assert "auth" not in fs.fs_args["client_kwargs"]
    assert "headers" not in fs.fs_args


def test_basic_auth_method():
    user = "username"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    assert fs.fs_args["client_kwargs"]["auth"].login == user
    assert fs.fs_args["client_kwargs"]["auth"].password == password


def test_custom_auth_method():
    header = "Custom-Header"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "auth": "custom",
        "custom_auth_header": header,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    headers = fs.fs_args["client_kwargs"]["headers"]
    assert header in headers
    assert headers[header] == password


def test_ssl_verify_disable():
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "ssl_verify": False,
    }

    fs = HTTPFileSystem(**config)
    session = sync(get_fsspec_loop(), fs.fs.set_session)

    # pylint: disable-next=protected-access
    assert not session._client._connector._ssl


def test_ssl_verify_custom_cert(mocker):
    mocker.patch("ssl.SSLContext.load_verify_locations")
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "ssl_verify": "/path/to/custom/cabundle.pem",
    }

    fs = HTTPFileSystem(**config)
    session = sync(get_fsspec_loop(), fs.fs.set_session)

    # pylint: disable-next=protected-access
    assert isinstance(session._client.connector._ssl, ssl.SSLContext)


def test_http_method():
    config = {
        "url": "http://example.com/",
        "path": "file.html",
    }

    fs = HTTPFileSystem(**config, method="PUT")
    assert fs.fs_args.get("upload_method") == "PUT"
    assert fs.fs.upload_method == "PUT"

    fs = HTTPFileSystem(**config, method="POST")
    assert fs.fs_args.get("upload_method") == "POST"
    assert fs.fs.upload_method == "POST"


@pytest.mark.parametrize(
    "kwarg,value",
    (
        ("connect_timeout", 42),
        ("read_timeout", 42),
    ),
)
def test_timeout_options(kwarg, value):
    url = "https://remote.dvc.org/get-started"
    config = {"url": url, kwarg: value}
    fs = HTTPFileSystem(**config)

    assert fs.fs_args["client_kwargs"][kwarg] == value
