import typing

import pytest

import httpcore


@pytest.mark.parametrize(
    ("url", "host", "port", "authority", "origin"),
    [
        (
            "http://[FE80::1%25en0]/path?x=1",
            b"fe80::1%25en0",
            None,
            b"[fe80::1%25en0]",
            httpcore.Origin(b"http", b"fe80::1%25en0", 80),
        ),
        (
            "http://[fe80::1%25en0]:8080/path",
            b"fe80::1%25en0",
            8080,
            b"[fe80::1%25en0]:8080",
            httpcore.Origin(b"http", b"fe80::1%25en0", 8080),
        ),
        (
            "https://[2001:db8::1%25eth0%2f2]/",
            b"2001:db8::1%25eth0%2f2",
            None,
            b"[2001:db8::1%25eth0%2f2]",
            httpcore.Origin(b"https", b"2001:db8::1%25eth0%2f2", 443),
        ),
    ],
)
def test_url_ipv6_zone_identifier(url, host, port, authority, origin):
    parsed = httpcore.URL(url)
    assert parsed.scheme == origin.scheme
    assert parsed.host == host
    assert parsed.port == port
    assert parsed.raw_path == parsed.target
    assert parsed.authority == authority
    assert parsed.origin == origin
    assert bytes(parsed).startswith(parsed.scheme + b"://[")
    assert authority in bytes(parsed)
    assert parsed.origin.socket_host == host.replace(b"%25", b"%", 1).decode()
    assert parsed.origin.tls_hostname is not None
    assert b"%25" not in parsed.origin.tls_hostname.encode()


@pytest.mark.parametrize(
    "url",
    [
        "http://[fe80::1%en0]/",
        "http://[fe80::1%25]/",
        "http://[fe80::1%25%zz]/",
        "http://[fe80::zzzz]/",
        "http://[fe80::1/",
        "http://[fe80::1]:99999/",
    ],
)
def test_url_with_invalid_ipv6_authority(url):
    with pytest.raises(httpcore.InvalidURL):
        httpcore.URL(url)
    with pytest.raises(httpcore.InvalidURL):
        httpcore.Request("GET", url)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"raw_path": b"/other?x=1"}, b"http://[fe80::1%25en0]:8080/other?x=1"),
        (
            {"host": b"fe80::1%25eth1"},
            b"http://[fe80::1%25eth1]:8080/path",
        ),
        ({"port": 8081}, b"http://[fe80::1%25en0]:8081/path"),
    ],
)
def test_url_copy_preserves_ipv6_zone_authority(kwargs, expected):
    url = httpcore.URL("http://[fe80::1%25en0]:8080/path")
    copied = url.copy(**kwargs)
    assert bytes(copied) == expected
    assert copied is not url


@pytest.mark.parametrize(
    ("relative", "expected"),
    [
        ("/other", b"http://[fe80::1%25en0]:8080/other"),
        (b"../other?x=1", b"http://[fe80::1%25en0]:8080/other?x=1"),
        (
            "http://[fe80::1%25eth1]/joined",
            b"http://[fe80::1%25eth1]/joined",
        ),
    ],
)
def test_url_join_preserves_ipv6_zone_authority(relative, expected):
    url = httpcore.URL("http://[fe80::1%25en0]:8080/a/base")
    assert bytes(url.join(relative)) == expected


def test_ipv6_zone_origin_pool_key():
    default = httpcore.URL("http://[fe80::1%25en0]/").origin
    other_zone = httpcore.URL("http://[fe80::1%25eth1]/").origin
    explicit_port = httpcore.URL("http://[fe80::1%25en0]:8080/").origin
    other_port = httpcore.URL("http://[fe80::1%25en0]:8081/").origin

    assert default == httpcore.Origin(b"http", b"fe80::1%25en0", 80)
    assert len({default, other_zone, explicit_port, other_port}) == 4
    assert default.authority == b"[fe80::1%25en0]:80"
    assert explicit_port.authority == b"[fe80::1%25en0]:8080"


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
