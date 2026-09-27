import typing

import pytest

import httpcore


class ExpectContinueStream(httpcore.NetworkStream):
    """
    A controlled byte stream that records all reads and writes, and that
    replays a scripted sequence of incoming bytes or exceptions.

    This allows tests to verify the exact ordering of outgoing request
    headers and request body against incoming responses.
    """

    def __init__(self, script: typing.List[typing.Union[bytes, Exception]]) -> None:
        self._script = list(script)
        self.events: typing.List[typing.Tuple[str, typing.Any]] = []
        self._closed = False

    def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if self._closed:
            raise httpcore.ReadError("Connection closed")
        self.events.append(("read", timeout))
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        if self._closed:
            raise httpcore.WriteError("Connection closed")
        self.events.append(("write", buffer))

    def close(self) -> None:
        self._closed = True


def written_bytes(stream: ExpectContinueStream) -> typing.List[bytes]:
    return [data for event, data in stream.events if event == "write"]


def body_write_index(stream: ExpectContinueStream, content: bytes) -> int:
    return next(
        index
        for index, (event, data) in enumerate(stream.events)
        if event == "write" and content in data
    )


def read_indexes(stream: ExpectContinueStream) -> typing.List[int]:
    return [
        index for index, (event, data) in enumerate(stream.events) if event == "read"
    ]



def test_http11_connection():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert not conn.is_closed()
        assert conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', IDLE, Request Count: 1]>"
        )



def test_http11_connection_unread_response():
    """
    If the client releases the response without reading it to termination,
    then the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_remote_protocol_error():
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([b"Wait, this isn't valid HTTP!", b""])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_incomplete_response():
    """
    We should be gracefully handling the case where the connection ends prematurely.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, wor",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_local_protocol_error():
    """
    If a local protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.LocalProtocolError) as exc_info:
            conn.request("GET", "https://example.com/", headers={"Host": "\0"})

        assert str(exc_info.value) == "Illegal header value b'\\x00'"

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_handles_one_active_request():
    """
    Attempting to send a request while one is already in-flight will raise
    a ConnectionNotAvailable exception.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.ConnectionNotAvailable):
                conn.request("GET", "https://example.com/")



def test_http11_connection_attempt_close():
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            response.read()
            assert response.status == 200
            assert response.content == b"Hello, world!"



def test_http11_request_to_incorrect_origin():
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            conn.request("GET", "https://other.com/")



def test_http11_expect_continue():
    """
    HTTP "100 Continue" is an interim response.
    We simply ignore it and return the final response.

    https://httpwg.org/specs/rfc9110.html#status.100
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/100
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = network_stream.read(max_bytes=1024)
            assert content == b"..."



def test_http11_upgrade_with_trailing_data():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    In `CONNECT` and `Upgrade:` requests, we need to handover the trailing data
    in the h11.Connection object.

    https://h11.readthedocs.io/en/latest/api.html#switching-protocols
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.MockStream(
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
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]

            content = network_stream.read(max_bytes=3)
            assert content == b"foo"
            content = network_stream.read(max_bytes=3)
            assert content == b"bar"
            content = network_stream.read(max_bytes=3)
            assert content == b"baz"

            # Lazy tests for HTTP11UpgradeStream
            network_stream.write(b"spam")
            invalid = network_stream.get_extra_info("invalid")
            assert invalid is None
            network_stream.close()



def test_http11_early_hints():
    """
    HTTP "103 Early Hints" is an interim response.
    We simply ignore it and return the final response.

    https://datatracker.ietf.org/doc/rfc8297/
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"<html>Hello, world! ...</html>"



def test_http11_header_sub_100kb():
    """
    A connection should be able to handle a http header size up to 100kB.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",  # 17
            b"Content-Type: plain/text\r\n",  # 43
            b"Cookie: " + b"x" * (100 * 1024 - 72) + b"\r\n",  # 102381
            b"Content-Length: 0\r\n",  # 102400
            b"\r\n",
            b"",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b""



def test_http11_expect_100_continue_sends_headers_then_body():
    """
    Requests including an "Expect: 100-continue" header send the request
    headers first, and only send the request body once the server has
    responded with a "100 Continue" informational response.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
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
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request headers are sent first, without the request body.
        writes = written_bytes(stream)
        assert writes[0].startswith(b"POST / HTTP/1.1\r\n")
        assert b"Expect: 100-continue" in writes[0]
        assert b"Hello, world!" not in writes[0]

        # The request body is only sent after reading from the server.
        assert min(read_indexes(stream)) < body_write_index(stream, b"Hello, world!")

        # The connection is reusable once the response is complete.
        assert conn.is_idle()
        assert conn.is_available()



def test_http11_expect_100_continue_connection_reuse():
    """
    Once a request using "Expect: 100-continue" has completed, the
    connection is safely reusable for a following request.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 5\r\n",
            b"\r\n",
            b"first",
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 6\r\n",
            b"\r\n",
            b"second",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello",
        )
        assert response.status == 200
        assert response.content == b"first"

        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello!",
        )
        assert response.status == 200
        assert response.content == b"second"

        assert conn.is_idle()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', IDLE, Request Count: 2]>"
        )



def test_http11_expect_100_continue_rejected():
    """
    If the server responds to "Expect: 100-continue" directly with a final
    response such as "417 Expectation Failed", then the request body is not
    sent, and the request content iterator is never read.
    """
    content_reads = 0

    def body() -> typing.Iterator[bytes]:
        nonlocal content_reads
        content_reads += 1
        yield b"Hello, world!"

    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 417 Expectation Failed\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body(),
        )
        assert response.status == 417

        # The request content iterator was never read.
        assert content_reads == 0

        # Only the request headers were sent.
        writes = written_bytes(stream)
        assert len(writes) == 1
        assert b"Hello, world!" not in writes[0]

        # The connection cannot be reused, since the request body was not sent.
        assert not conn.is_idle()
        assert conn.is_closed()



def test_http11_expect_100_continue_with_other_informational_responses():
    """
    Informational responses other than "100 Continue", such as
    "103 Early Hints", neither trigger sending the request body,
    nor count as a final response.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            (
                b"HTTP/1.1 103 Early Hints\r\n"
                b"Link: </style.css>; rel=preload; as=style\r\n"
                b"\r\n"
            ),
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # Both the "103 Early Hints" and the "100 Continue" responses were
        # read before the request body was sent.
        reads_before_body = [
            index
            for index in read_indexes(stream)
            if index < body_write_index(stream, b"Hello, world!")
        ]
        assert len(reads_before_body) == 2

        assert conn.is_idle()



def test_http11_expect_100_continue_timeout_fallback():
    """
    If no response arrives within the configured "continue" timeout, then
    the request body is sent anyway. The wait is not treated as a failure.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            httpcore.ReadTimeout("timed out"),
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
            extensions={"timeout": {"continue": 1.0}},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The "continue" timeout was applied while waiting for the response.
        assert ("read", 1.0) in stream.events

        # The request body was sent once the wait had elapsed.
        writes = written_bytes(stream)
        assert writes[1] == b"Hello, world!"

        assert conn.is_idle()



def test_http11_expect_100_continue_late_response_not_duplicated():
    """
    A "100 Continue" response that arrives after the "continue" timeout has
    already triggered sending the request body must not cause the request
    body to be sent twice.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            httpcore.ReadTimeout("timed out"),
            b"HTTP/1.1 100 Continue\r\n",  # A late "100 Continue" response.
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 5\r\n",
            b"\r\n",
            b"Hello",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello",
            extensions={"timeout": {"continue": 1.0}},
        )
        assert response.status == 200
        assert response.content == b"Hello"

        # The request body was sent exactly once.
        writes = written_bytes(stream)
        assert writes.count(b"Hello") == 1

        assert conn.is_idle()



def test_http11_expect_100_continue_final_response_with_body():
    """
    If the server sends a final response including a response body instead of
    a "100 Continue" response, then the request body is not sent, and the
    response body can be read as usual.
    """
    content_reads = 0

    def body() -> typing.Iterator[bytes]:
        nonlocal content_reads
        content_reads += 1
        yield b"Hello, world!"

    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body(),
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request content iterator was never read.
        assert content_reads == 0

        # The connection cannot be reused, since the request body was not sent.
        assert not conn.is_idle()
        assert conn.is_closed()



def test_http11_expect_100_continue_empty_body():
    """
    An empty request body with "Expect: 100-continue" still completes the
    request/response cycle cleanly, leaving the connection reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=b"",
        )
        assert response.status == 200
        assert response.content == b""

        assert conn.is_idle()
        assert conn.is_available()



def test_http11_expect_100_continue_server_disconnect():
    """
    If the server rejects the request and closes the connection while the
    client is waiting for a "100 Continue" response, then the connection
    lifecycle ends cleanly.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream([b""])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request(
                "POST",
                "https://example.com/",
                headers={"Expect": "100-continue"},
                content=b"Hello, world!",
            )

        assert not conn.is_idle()
        assert conn.is_closed()



def test_http11_expect_100_continue_body_send_error():
    """
    If the request content iterator fails while sending the request body,
    then the connection lifecycle ends cleanly.
    """

    def body() -> typing.Iterator[bytes]:
        yield b"Hello, "
        raise RuntimeError("Something went wrong")

    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            conn.request(
                "POST",
                "https://example.com/",
                headers={"Expect": "100-continue"},
                content=body(),
            )

        assert not conn.is_idle()
        assert conn.is_closed()



def test_http11_request_body_sent_immediately_without_expect():
    """
    Requests without an "Expect: 100-continue" header send the request body
    immediately, without waiting for any response from the server.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            content=b"Hello, world!",
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request body is sent before reading any response.
        assert body_write_index(stream, b"Hello, world!") < min(read_indexes(stream))
