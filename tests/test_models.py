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


# URLs including IPv6 zone identifiers (RFC 6874)


@pytest.mark.parametrize(
    "url,display",
    [
        ("http://[::1]/", "http://[::1]:80"),
        ("http://[fe80::1%25eth0]/", "http://[fe80::1%25eth0]:80"),
        ("http://[fe80::1%25eth0]:8080/", "http://[fe80::1%25eth0]:8080"),
        ("https://[fe80::1%25eth0]/", "https://[fe80::1%25eth0]:443"),
    ],
)
def test_url_origin_with_ipv6_zone_identifier(url, display):
    origin = httpcore.URL(url).origin
    assert str(origin) == display


@pytest.mark.parametrize(
    "url,host,port,display",
    [
        (
            "http://[fe80::1%25eth0]/",
            b"fe80::1%25eth0",
            None,
            b"http://[fe80::1%25eth0]/",
        ),
        (
            "http://[fe80::1%eth0]/",
            b"fe80::1%25eth0",
            None,
            b"http://[fe80::1%25eth0]/",
        ),
        (
            "http://[fe80::1%25eth0]:8080/path?x=1",
            b"fe80::1%25eth0",
            8080,
            b"http://[fe80::1%25eth0]:8080/path?x=1",
        ),
        (
            "https://[fe80::1%25eth0]/",
            b"fe80::1%25eth0",
            None,
            b"https://[fe80::1%25eth0]/",
        ),
        ("http://[::1]/", b"::1", None, b"http://[::1]/"),
        ("http://[::1]:8080/", b"::1", 8080, b"http://[::1]:8080/"),
    ],
)
def test_url_with_ipv6_zone_identifier(url, host, port, display):
    """
    IPv6 literals may include an RFC 6874 zone identifier, either using
    the percent-encoded form used within URLs, or the plain form used
    on-the-wire. Both forms normalize to the same URL, with a display
    form that encloses the literal in square brackets.
    """
    parsed = httpcore.URL(url)
    assert parsed.host == host
    assert parsed.port == port
    assert bytes(parsed) == display


@pytest.mark.parametrize(
    "other",
    [
        # The percent-encoded and plain forms of the zone identifier.
        "http://[fe80::1%25eth0]/",
        "http://[fe80::1%eth0]/",
        # An explicit port matching the scheme default.
        "http://[fe80::1%25eth0]:80/",
    ],
)
def test_equivalent_ipv6_zone_identifier_urls_share_an_origin(other):
    """
    Semantically equivalent URLs must map onto the same origin, so that
    connections to them are reused rather than being pooled separately.
    """
    url = httpcore.URL("http://[fe80::1%25eth0]/")
    assert httpcore.URL(other).origin == url.origin


@pytest.mark.parametrize(
    "other",
    [
        "http://[fe80::1%25eth1]/",  # A differing zone identifier.
        "http://[fe80::2%25eth0]/",  # A differing address.
        "http://[fe80::1%25eth0]:8080/",  # A differing port.
        "https://[fe80::1%25eth0]/",  # A differing scheme.
    ],
)
def test_differing_ipv6_zone_identifier_urls_do_not_share_an_origin(other):
    """
    URLs that differ by zone identifier, address, port, or scheme must
    not be merged onto the same origin.
    """
    url = httpcore.URL("http://[fe80::1%25eth0]/")
    assert httpcore.URL(other).origin != url.origin


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/path?x=1",
        "http://[::1]:8080/path",
        "http://[fe80::1%25eth0]/path?x=1",
        "http://[fe80::1%eth0]:8080/path",
        "https://[fe80::1%25eth0]/",
    ],
)
def test_url_copy_and_join_with_ipv6_zone_identifier(url):
    """
    URLs may be copied by round-tripping through their display form,
    or joined from their individual components, without changing the
    resulting origin.
    """
    original = httpcore.URL(url)

    copied = httpcore.URL(bytes(original))
    assert copied == original
    assert copied.origin == original.origin

    joined = httpcore.URL(
        scheme=original.scheme,
        host=original.host,
        port=original.port,
        target=original.target,
    )
    assert joined == original
    assert joined.origin == original.origin
    assert bytes(joined) == bytes(original)


@pytest.mark.parametrize(
    "url",
    [
        "http://[fe80::1/",  # Unbalanced brackets.
        "http://[fe80::1%25eth0",  # Unbalanced brackets.
        "http://example.com:abc/",  # Non-numeric port.
        "http://example.com:99999/",  # Out-of-range port.
    ],
)
def test_url_with_invalid_authority(url):
    """
    Invalid URL authorities raise a clear error at construction time,
    rather than failing once a request is sent.
    """
    with pytest.raises(ValueError):
        httpcore.URL(url)


@pytest.mark.parametrize(
    "host",
    [
        b"[fe80::1%25eth0",  # Unbalanced brackets.
        b"exam ple.com",  # Whitespace.
        b"example.com:8080",  # Port included in the host.
        b"fe80::1%25",  # Empty zone identifier.
        b"not-an-ip:v6",  # Invalid IPv6 literal.
    ],
)
def test_url_with_invalid_host_component(host):
    """
    Invalid hosts given as URL components raise a clear error at
    construction time, rather than failing once a request is sent.
    """
    with pytest.raises(ValueError):
        httpcore.URL(scheme=b"http", host=host, target=b"/")


def test_url_host_component_normalization():
    """
    Hosts given as URL components are normalized in the same way as
    hosts parsed from a URL string.
    """
    url = httpcore.URL(scheme=b"http", host=b"[fe80::1%eth0]", port=8080, target=b"/")
    assert url.host == b"fe80::1%25eth0"
    assert bytes(url) == b"http://[fe80::1%25eth0]:8080/"


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
