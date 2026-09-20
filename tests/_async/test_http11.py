import pytest

import httpcore


@pytest.mark.anyio
async def test_http11_connection():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert not conn.is_closed()
        assert conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<AsyncHTTP11Connection ['https://example.com:443', IDLE, Request Count: 1]>"
        )


@pytest.mark.anyio
async def test_http11_connection_unread_response():
    """
    If the client releases the response without reading it to termination,
    then the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )


@pytest.mark.anyio
async def test_http11_connection_with_remote_protocol_error():
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream([b"Wait, this isn't valid HTTP!", b""])
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )


@pytest.mark.anyio
async def test_http11_connection_with_incomplete_response():
    """
    We should be gracefully handling the case where the connection ends prematurely.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, wor",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )


@pytest.mark.anyio
async def test_http11_connection_with_local_protocol_error():
    """
    If a local protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.LocalProtocolError) as exc_info:
            await conn.request("GET", "https://example.com/", headers={"Host": "\0"})

        assert str(exc_info.value) == "Illegal header value b'\\x00'"

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )


@pytest.mark.anyio
async def test_http11_connection_handles_one_active_request():
    """
    Attempting to send a request while one is already in-flight will raise
    a ConnectionNotAvailable exception.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.ConnectionNotAvailable):
                await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_attempt_close():
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            await response.aread()
            assert response.status == 200
            assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http11_request_to_incorrect_origin():
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream([])
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            await conn.request("GET", "https://other.com/")


@pytest.mark.anyio
async def test_http11_expect_continue():
    """
    HTTP "100 Continue" is an interim response.
    We simply ignore it and return the final response.

    https://httpwg.org/specs/rfc9110.html#status.100
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/100
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        async with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = await network_stream.read(max_bytes=1024)
            assert content == b"..."


@pytest.mark.anyio
async def test_http11_upgrade_with_trailing_data():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    In `CONNECT` and `Upgrade:` requests, we need to handover the trailing data
    in the h11.Connection object.

    https://h11.readthedocs.io/en/latest/api.html#switching-protocols
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        # The first element of this mock network stream buffer simulates networking
        # in which response headers and data are received at once.
        # This means that "foobar" becomes trailing data.
        [
            (
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Connection: upgrade\r\n"
                b"Upgrade: custom\r\n"
                b"\r\n"
                b"foobar"
            ),
            b"baz",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        async with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]

            content = await network_stream.read(max_bytes=3)
            assert content == b"foo"
            content = await network_stream.read(max_bytes=3)
            assert content == b"bar"
            content = await network_stream.read(max_bytes=3)
            assert content == b"baz"

            # Lazy tests for AsyncHTTP11UpgradeStream
            await network_stream.write(b"spam")
            invalid = network_stream.get_extra_info("invalid")
            assert invalid is None
            await network_stream.aclose()


@pytest.mark.anyio
async def test_http11_early_hints():
    """
    HTTP "103 Early Hints" is an interim response.
    We simply ignore it and return the final response.

    https://datatracker.ietf.org/doc/rfc8297/
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 103 Early Hints\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js.css>; rel=preload; as=style\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/html; charset=utf-8\r\n",
            b"Content-Length: 30\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js>; rel=preload; as=script\r\n",
            b"\r\n",
            b"<html>Hello, world! ...</html>",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"<html>Hello, world! ...</html>"


@pytest.mark.anyio
async def test_http11_header_sub_100kb():
    """
    A connection should be able to handle a http header size up to 100kB.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",  # 17
            b"Content-Type: plain/text\r\n",  # 43
            b"Cookie: " + b"x" * (100 * 1024 - 72) + b"\r\n",  # 102381
            b"Content-Length: 0\r\n",  # 102400
            b"\r\n",
            b"",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b""


@pytest.mark.anyio
async def test_http11_connection_with_trailers():
    """
    Trailers sent with a chunked response are available on the response
    once the body has been read to completion, preserving the raw bytes,
    ordering, and any duplicate fields. They are not included in the
    initial response headers.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n",
            b"8\r\n, world!\r\n",
            # The final chunk and trailer lines are split across
            # read boundaries, part-way through a trailer field.
            b"0\r\nX-Digest: abc",
            b"123\r\n",
            b"X-Digest: def456\r\n",
            b"X-Checksum: sha256\r\n",
            b"\r\n",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200
            # Trailers are not available until the body is fully read.
            assert response.trailers == []
            content = await response.aread()
            assert content == b"Hello, world!"
            assert response.trailers == [
                (b"X-Digest", b"abc123"),
                (b"X-Digest", b"def456"),
                (b"X-Checksum", b"sha256"),
            ]
            # The trailers are not mixed into the initial response headers.
            assert response.headers == [
                (b"Content-Type", b"plain/text"),
                (b"Transfer-Encoding", b"chunked"),
            ]

        # A response that has been read to completion is reusable.
        assert conn.is_idle()
        assert conn.is_available()


@pytest.mark.anyio
async def test_http11_connection_with_empty_trailers():
    """
    A chunked response with no trailing headers returns an explicitly
    empty list of trailers.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"d\r\nHello, world!\r\n",
            b"0\r\n",
            b"\r\n",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert response.trailers == []


@pytest.mark.anyio
async def test_http11_connection_trailers_not_shared_between_requests():
    """
    Requests sent over a reused connection each get their own trailers.
    A response without trailers does not pick up trailers from an
    earlier response on the same connection.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n",
            b"0\r\n",
            b"X-Digest: abc123\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 5\r\n",
            b"\r\n",
            b"World",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.content == b"Hello"
        assert response.trailers == [(b"X-Digest", b"abc123")]

        response = await conn.request("GET", "https://example.com/")
        assert response.content == b"World"
        assert response.trailers == []


@pytest.mark.anyio
async def test_http11_connection_trailers_unavailable_if_response_closed_early():
    """
    If the response is closed before the body has been read to completion,
    then no trailers are available, and the connection is not reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n",
            b"8\r\n, world!\r\n",
            b"0\r\n",
            b"X-Digest: abc123\r\n",
            b"\r\n",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            stream_iterator = response.aiter_stream()
            async for chunk in stream_iterator:
                assert chunk == b"Hello"
                break
            await stream_iterator.aclose()
            # The response was closed part-way through the body,
            # so no trailers were received.
            assert response.trailers == []

        assert not conn.is_idle()
        assert conn.is_closed()


@pytest.mark.anyio
async def test_http11_connection_trailers_unavailable_on_read_failure():
    """
    If reading the response body fails part-way through, then the error
    is raised to the caller, and no trailers are available.
    """

    class ReadFailureStream(httpcore.AsyncMockStream):
        async def read(self, max_bytes, timeout=None):
            if self._buffer and self._buffer[0] == b"FAIL":
                raise httpcore.ReadError("Unable to read")
            return await super().read(max_bytes, timeout)

    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ReadFailureStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n",
            b"FAIL",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            with pytest.raises(httpcore.ReadError):
                await response.aread()
            assert response.trailers == []

        assert not conn.is_idle()
        assert conn.is_closed()


@pytest.mark.anyio
async def test_http11_connection_with_invalid_trailers():
    """
    Malformed trailers are a protocol error. The response must not be
    treated as if it had completed successfully.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n",
            b"0\r\n",
            b"This is not a valid trailer field\r\n",
            b"\r\n",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            with pytest.raises(httpcore.RemoteProtocolError):
                await response.aread()
            assert response.trailers == []

        assert not conn.is_idle()
        assert conn.is_closed()
