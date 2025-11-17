from pathlib import Path

from pyfdb.pyfdb import URI
import pytest


def test_uri_initialization_string_unkown_scheme():
    uri = URI.from_str("scheme://netloc/path;parameters?query#fragment")

    assert uri.scheme() == "unix"


def test_uri_initialization_string_http_easy():
    uri = URI.from_str(
        "http://user@localhost:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )

    assert uri.scheme() == "http"

    assert uri.name() == "/a/b;c=1;d=two/e;f=3"
    assert uri.user() == "user"
    assert uri.host() == "localhost"
    assert uri.port() == 8443

    with pytest.raises(RuntimeError, match="Not implemented"):
        uri.path()

    assert uri.fragment() == "section-2"

    print(uri)


def test_uri_initialization_string():
    uri = URI.from_str(
        "scheme+demo://user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )

    assert uri.scheme() == "unix"

    assert (
        uri.name()
        == "scheme+demo://user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )
    assert uri.user() == ""
    assert uri.host() == ""
    assert uri.port() == -1
    assert (
        uri.path()
        == "scheme+demo:/user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )
    assert uri.fragment() == ""

    print(uri)


def test_uri_initialization_raw_string():
    uri = URI.from_str(
        "scheme+demo://user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )

    assert uri.scheme() == "unix"

    assert (
        uri.name()
        == "scheme+demo://user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )
    assert uri.user() == ""
    assert uri.host() == ""
    assert uri.port() == -1
    assert (
        uri.path()
        == "scheme+demo:/user:pa%3Ass@[2001:db8:85a3::8a2e:370:7334]:8443/a/b;c=1;d=two/e;f=3?q=search%20terms&sort=asc&flag&list=a,b,c#section-2"
    )
    assert uri.fragment() == ""

    print(uri.rawStr())


def test_uri_initialization_path():
    uri = URI.from_path(path=Path("/test"))

    assert uri.scheme() == "file"
    assert uri.path() == "/test"


def test_uri_initialization_scheme_uri():
    other_uri = URI.from_scheme_host_port(scheme="file", host="old-host", port=4321)
    uri = URI.from_scheme_uri(scheme="http", uri=other_uri)

    assert uri.scheme() == "http"
    assert uri.host() == "old-host"
    assert uri.port() == 4321


def test_uri_initialization_scheme_host_port():
    uri = URI.from_scheme_host_port(scheme="file", host="host", port=1234)

    assert uri.scheme() == "file"
    assert uri.host() == "host"
    assert uri.port() == 1234


def test_uri_initialization_scheme_uri_host_port():
    other_uri = URI.from_scheme_host_port(scheme="file", host="host", port=1234)
    uri = URI.from_scheme_uri_host_port(
        scheme="file", uri=other_uri, host="new-host", port=4321
    )

    assert uri.scheme() == "file"
    assert uri.host() == "new-host"
    assert uri.port() == 4321
