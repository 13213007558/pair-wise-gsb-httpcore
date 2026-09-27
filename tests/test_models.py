import typing

import pytest

import httpcore

# URL


def test_url():
    url = httpcore.URL("https://www.example.com/")
    assert url == httpcore.URL(
        scheme="https", host="www.example.com", port=None, target="/"
    )
    assert bytes(url) == b"https://www.example.com/"


def test_url_with_port():
    url = httpcore.URL("https://www.example.com:443/")
    assert url == httpcore.URL(
        scheme="https", host="www.example.com", port=443, target="/"
    )
    assert bytes(url) == b"https://www.example.com:443/"


def test_url_with_invalid_argument():
    with pytest.raises(TypeError) as exc_info:
        httpcore.URL(123)  # type: ignore
    assert str(exc_info.value) == "url must be bytes or str, but got int."


def test_url_cannot_include_unicode_strings():
    """
    URLs instantiated with strings outside of the plain ASCII range are disallowed,
    but the explicit style allows for these ambiguous cases to be precisely expressed.
    """
    with pytest.raises(TypeError) as exc_info:
        httpcore.URL("https://www.example.com/☺")
    assert str(exc_info.value) == "url strings may not include unicode characters."

    httpcore.URL(scheme=b"https", host=b"www.example.com", target="/☺".encode("utf-8"))


def test_url_origin_socks5():
    url = httpcore.URL("socks5://127.0.0.1")
    origin = url.origin
    assert origin == httpcore.Origin(scheme=b"socks5", host=b"127.0.0.1", port=1080)
    assert str(origin) == "socks5://127.0.0.1:1080"

    url = httpcore.URL("socks5h://127.0.0.1")
    origin = url.origin
    assert origin == httpcore.Origin(scheme=b"socks5h", host=b"127.0.0.1", port=1080)
    assert str(origin) == "socks5h://127.0.0.1:1080"


# IPv6 zone identifiers (RFC 6874)


@pytest.mark.parametrize(
    "url,host,port,target",
    [
        ("http://[fe80::1%25eth0]/", b"[fe80::1%25eth0]", None, "/"),
        ("http://[fe80::1%25eth0]/path?x=1", b"[fe80::1%25eth0]", None, "/path?x=1"),
        ("http://[fe80::1%25eth0]:8080/", b"[fe80::1%25eth0]", 8080, "/"),
        ("http://[fe80::1]/", b"[fe80::1]", None, "/"),
        ("http://[::1]:8080/path", b"[::1]", 8080, "/path"),
    ],
)
def test_url_ipv6_zone_id(url, host, port, target):
    """
    IPv6 literals are normalized to their bracketed RFC 6874 wire form,
    retaining any percent-encoded zone identifier.
    """
    parsed = httpcore.URL(url)
    assert parsed.scheme == b"http"
    assert parsed.host == host
    assert parsed.port == port
    assert parsed.target == target.encode("ascii")


def test_url_ipv6_zone_id_display_form():
    """
    The display form of a URL uses the bracketed RFC 6874 wire form,
    so that it round-trips consistently.
    """
    url = httpcore.URL("http://[fe80::1%25eth0]/path?x=1")
    assert bytes(url) == b"http://[fe80::1%25eth0]/path?x=1"
    assert httpcore.URL(bytes(url)) == url

    url = httpcore.URL("http://[fe80::1%25eth0]:8080/")
    assert bytes(url) == b"http://[fe80::1%25eth0]:8080/"
    assert httpcore.URL(bytes(url)) == url


def test_url_ipv6_zone_id_equivalent_constructions():
    """
    URLs constructed from a string or from explicit components are
    normalized to the same canonical form.
    """
    url = httpcore.URL("http://[fe80::1%25eth0]/")
    assert url == httpcore.URL(
        scheme="http", host="[fe80::1%25eth0]", port=None, target="/"
    )
    assert url == httpcore.URL(
        scheme="http", host="fe80::1%25eth0", port=None, target="/"
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://[fe80::1%eth0]/",  # Zone identifier requires a '%25' delimiter.
        "http://[fe80::1%]/",  # Bare '%' delimiter.
        "http://[fe80::1%25]/",  # Empty zone identifier.
        "http://[::zz]/",  # Invalid IPv6 address.
        "http://[::1/",  # Mismatched brackets.
        "http://example.com:bad/",  # Non-numeric port.
        "http://example.com:99999/",  # Out-of-range port.
    ],
)
def test_url_with_invalid_authority(url):
    """
    Invalid authorities raise a clear error at the point the URL is
    constructed, rather than failing once a request is sent.
    """
    with pytest.raises(ValueError):
        httpcore.URL(url)


@pytest.mark.parametrize(
    "host",
    [
        "fe80::1%eth0",  # Zone identifier requires a '%25' delimiter.
        "fe80::1%25",  # Empty zone identifier.
        "[fe80::1%25",  # Mismatched brackets.
        "::zz",  # Invalid IPv6 address.
    ],
)
def test_url_with_invalid_authority_using_explicit_components(host):
    """
    Invalid authorities also raise a clear error when the URL is
    constructed from explicit components.
    """
    with pytest.raises(ValueError):
        httpcore.URL(scheme="http", host=host, port=None, target="/")


@pytest.mark.parametrize(
    "url,other_url,equal",
    [
        # The default port may be either implicit or explicit.
        ("http://[fe80::1%25eth0]/", "http://[fe80::1%25eth0]:80/", True),
        ("http://[fe80::1%25eth0]:8080/", "http://[fe80::1%25eth0]:8080/", True),
        # Different zone identifiers must not share connections.
        ("http://[fe80::1%25eth0]/", "http://[fe80::1%25eth1]/", False),
        ("http://[fe80::1%25eth0]/", "http://[fe80::1]/", False),
        # Different explicit ports must not share connections.
        ("http://[fe80::1%25eth0]/", "http://[fe80::1%25eth0]:8080/", False),
        ("http://[fe80::1%25eth0]:8080/", "http://[fe80::1%25eth0]:8081/", False),
        # Different schemes must not share connections.
        ("http://[fe80::1%25eth0]/", "https://[fe80::1%25eth0]/", False),
    ],
)
def test_url_ipv6_zone_id_connection_reuse_key(url, other_url, equal):
    """
    The origin is the connection-reuse key. It must be stable for URLs
    that are semantically equivalent, and must differ for URLs that
    differ in zone identifier, port, or scheme.
    """
    origin = httpcore.URL(url).origin
    other_origin = httpcore.URL(other_url).origin
    assert (origin == other_origin) == equal
    assert (str(origin) == str(other_origin)) == equal


def test_url_ipv6_zone_id_origin():
    url = httpcore.URL("http://[fe80::1%25eth0]/")
    origin = url.origin
    assert origin == httpcore.Origin(b"http", b"[fe80::1%25eth0]", 80)
    assert str(origin) == "http://[fe80::1%25eth0]:80"

    url = httpcore.URL("https://[fe80::1%25eth0]:8443/")
    origin = url.origin
    assert origin == httpcore.Origin(b"https", b"[fe80::1%25eth0]", 8443)
    assert str(origin) == "https://[fe80::1%25eth0]:8443"


@pytest.mark.parametrize(
    "url,overrides,expected",
    [
        (
            "http://[fe80::1%25eth0]/",
            {"port": 8080},
            b"http://[fe80::1%25eth0]:8080/",
        ),
        (
            "http://[fe80::1%25eth0]/",
            {"target": "/other?x=1"},
            b"http://[fe80::1%25eth0]/other?x=1",
        ),
        (
            "http://[fe80::1%25eth0]:8080/",
            {"scheme": "https"},
            b"https://[fe80::1%25eth0]:8080/",
        ),
        (
            "http://[fe80::1%25eth0]/",
            {"host": "[fe80::2%25eth0]"},
            b"http://[fe80::2%25eth0]/",
        ),
        (
            "http://[fe80::1%25eth0]/",
            {},
            b"http://[fe80::1%25eth0]/",
        ),
    ],
)
def test_url_copy_with_ipv6_zone_id(url, overrides, expected):
    """
    Copying a URL preserves the canonical RFC 6874 form of the host.
    """
    copied = httpcore.URL(url).copy_with(**overrides)
    assert bytes(copied) == expected
    assert copied.host.startswith(b"[")


@pytest.mark.parametrize(
    "url,relative,expected",
    [
        (
            "http://[fe80::1%25eth0]/some/path",
            "../other",
            b"http://[fe80::1%25eth0]/other",
        ),
        (
            "http://[fe80::1%25eth0]/some/path",
            "/absolute",
            b"http://[fe80::1%25eth0]/absolute",
        ),
        (
            "http://[fe80::1%25eth0]:8080/some/path",
            "other",
            b"http://[fe80::1%25eth0]:8080/some/other",
        ),
        (
            "http://[fe80::1%25eth0]/some/path",
            "https://[fe80::2%25eth1]/elsewhere",
            b"https://[fe80::2%25eth1]/elsewhere",
        ),
    ],
)
def test_url_join_ipv6_zone_id(url, relative, expected):
    """
    Joining a URL preserves the canonical RFC 6874 form of the host.
    """
    joined = httpcore.URL(url).join(relative)
    assert bytes(joined) == expected


# Request


def test_request():
    request = httpcore.Request("GET", "https://www.example.com/")
    assert request.method == b"GET"
    assert request.url == httpcore.URL("https://www.example.com/")
    assert request.headers == []
    assert request.extensions == {}
    assert repr(request) == "<Request [b'GET']>"
    assert (
        repr(request.url)
        == "URL(scheme=b'https', host=b'www.example.com', port=None, target=b'/')"
    )
    assert repr(request.stream) == "<ByteStream [0 bytes]>"


def test_request_with_target_extension():
    extensions = {"target": b"/another_path"}
    request = httpcore.Request(
        "GET", "https://www.example.com/path", extensions=extensions
    )
    assert request.url.target == b"/another_path"

    extensions = {"target": b"/unescaped|path"}
    request = httpcore.Request(
        "GET", "https://www.example.com/path", extensions=extensions
    )
    assert request.url.target == b"/unescaped|path"


def test_request_with_invalid_method():
    with pytest.raises(TypeError) as exc_info:
        httpcore.Request(123, "https://www.example.com/")  # type: ignore
    assert str(exc_info.value) == "method must be bytes or str, but got int."


def test_request_with_invalid_url():
    with pytest.raises(TypeError) as exc_info:
        httpcore.Request("GET", 123)  # type: ignore
    assert str(exc_info.value) == "url must be a URL, bytes, or str, but got int."


def test_request_with_invalid_headers():
    with pytest.raises(TypeError) as exc_info:
        httpcore.Request("GET", "https://www.example.com/", headers=123)  # type: ignore
    assert (
        str(exc_info.value)
        == "headers must be a mapping or sequence of two-tuples, but got int."
    )


# Response


def test_response():
    response = httpcore.Response(200)
    assert response.status == 200
    assert response.headers == []
    assert response.extensions == {}
    assert repr(response) == "<Response [200]>"
    assert repr(response.stream) == "<ByteStream [0 bytes]>"


# Tests for reading and streaming sync byte streams...


class ByteIterator:
    def __init__(self, chunks: typing.List[bytes]) -> None:
        self._chunks = chunks

    def __iter__(self) -> typing.Iterator[bytes]:
        for chunk in self._chunks:
            yield chunk


def test_response_sync_read():
    stream = ByteIterator([b"Hello, ", b"world!"])
    response = httpcore.Response(200, content=stream)
    assert response.read() == b"Hello, world!"
    assert response.content == b"Hello, world!"


def test_response_sync_streaming():
    stream = ByteIterator([b"Hello, ", b"world!"])
    response = httpcore.Response(200, content=stream)
    content = b"".join([chunk for chunk in response.iter_stream()])
    assert content == b"Hello, world!"

    # We streamed the response rather than reading it, so .content is not available.
    with pytest.raises(RuntimeError):
        response.content

    # Once we've streamed the response, we can't access the stream again.
    with pytest.raises(RuntimeError):
        for _chunk in response.iter_stream():
            pass  # pragma: nocover


# Tests for reading and streaming async byte streams...


class AsyncByteIterator:
    def __init__(self, chunks: typing.List[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


@pytest.mark.trio
async def test_response_async_read():
    stream = AsyncByteIterator([b"Hello, ", b"world!"])
    response = httpcore.Response(200, content=stream)
    assert await response.aread() == b"Hello, world!"
    assert response.content == b"Hello, world!"


@pytest.mark.trio
async def test_response_async_streaming():
    stream = AsyncByteIterator([b"Hello, ", b"world!"])
    response = httpcore.Response(200, content=stream)
    content = b"".join([chunk async for chunk in response.aiter_stream()])
    assert content == b"Hello, world!"

    # We streamed the response rather than reading it, so .content is not available.
    with pytest.raises(RuntimeError):
        response.content

    # Once we've streamed the response, we can't access the stream again.
    with pytest.raises(RuntimeError):
        async for chunk in response.aiter_stream():
            pass  # pragma: nocover
