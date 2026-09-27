import typing

import pytest

import httpcore


class ExpectContinueStream(httpcore.NetworkStream):
    """
    A controlled byte stream that records everything the client writes,
    and replays a scripted sequence of reads, raising `ReadTimeout`
    once no scripted data remains.
    """

    def __init__(
        self,
        read_script: typing.Optional[
            typing.List[typing.Union[bytes, Exception]]
        ] = None,
        fail_on_write: typing.Optional[int] = None,
    ) -> None:
        self.written: typing.List[bytes] = []
        self.read_script = list(read_script or [])
        self._fail_on_write = fail_on_write
        self._closed = False

    def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if self._closed:
            raise httpcore.ReadError("Connection closed")
        if not self.read_script:
            raise httpcore.ReadTimeout()
        item = self.read_script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        if self._fail_on_write is not None and len(self.written) >= self._fail_on_write:
            raise httpcore.WriteError("Write failed")
        self.written.append(buffer)

    def close(self) -> None:
        self._closed = True



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



def test_http11_expect_100_continue():
    """
    When sending a request with "Expect: 100-continue" the client sends
    the request headers, waits for a "100 Continue" response, and only
    then sends the request body.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    body_iterations = []

    def request_body() -> typing.Iterator[bytes]:
        body_iterations.append(b"Hello, world!")
        yield b"Hello, world!"

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=request_body(),
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request headers are sent first, including "Expect: 100-continue".
        # The request body is only sent once "100 Continue" has been received.
        assert stream.written[0].startswith(b"POST / HTTP/1.1\r\n")
        assert b"Expect: 100-continue" in stream.written[0]
        assert b"Hello, world!" not in stream.written[0]
        assert b"Hello, world!" in b"".join(stream.written[1:])
        assert body_iterations == [b"Hello, world!"]

        # The connection is reusable once the response has been read.
        assert conn.is_idle()
        assert conn.is_available()
        assert not conn.is_closed()

        # A following request can safely reuse the connection.
        stream.read_script.extend(
            [
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ]
        )
        response = conn.request("GET", "http://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert conn.is_idle()



def test_http11_expect_100_continue_final_response():
    """
    If the server responds with a final response such as
    "417 Expectation Failed" instead of "100 Continue", then the request
    body is not sent, and the request iterator is never consumed.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 417 Expectation Failed\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    body_iterations = []

    def request_body() -> typing.Iterator[bytes]:
        body_iterations.append(b"Hello, world!")
        yield b"Hello, world!"

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "http://example.com/",
            headers={"Expect": "100-continue", "Content-Length": "13"},
            content=request_body(),
        )
        assert response.status == 417
        assert response.content == b""

        # The request body was never sent...
        assert body_iterations == []
        assert b"Hello, world!" not in b"".join(stream.written)

        # ...and the connection is not reusable, since the request
        # body was not sent.
        assert not conn.is_idle()
        assert not conn.is_available()
        assert conn.is_closed()



def test_http11_expect_100_continue_empty_body_final_response():
    """
    If the request body is empty and the server responds with a final
    response directly, then the connection lifecycle completes neatly,
    and the connection remains reusable.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
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
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=b"",
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert conn.is_available()
        assert not conn.is_closed()



def test_http11_expect_100_continue_timeout():
    """
    If the continue timeout expires without any response from the server,
    then the request body is sent anyways. This is not treated as an error.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            httpcore.ReadTimeout(),
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
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
            extensions={"timeout": {"continue": 0.1}},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request headers were sent, followed by the request body.
        assert b"Expect: 100-continue" in stream.written[0]
        assert b"Hello, world!" in b"".join(stream.written[1:])

        assert conn.is_idle()
        assert conn.is_available()
        assert not conn.is_closed()



def test_http11_expect_100_continue_multiple_informational_responses():
    """
    Informational responses such as "103 Early Hints" neither trigger
    sending the request body, nor count as a final response.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 103 Early Hints\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"\r\n",
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
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request body was sent exactly once.
        assert b"".join(stream.written).count(b"Hello, world!") == 1



def test_http11_expect_100_continue_late_100():
    """
    If the continue timeout expires and a "100 Continue" response arrives
    late, once the request body has already been sent, then the request
    body must not be sent a second time.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            httpcore.ReadTimeout(),
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
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
            extensions={"timeout": {"continue": 0.1}},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The request body was sent exactly once.
        assert b"".join(stream.written).count(b"Hello, world!") == 1



def test_http11_expect_100_continue_server_disconnect():
    """
    If the server rejects the request and closes the connection while the
    client is waiting for "100 Continue", then the connection lifecycle
    ends neatly.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream([b""])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request(
                "POST",
                "http://example.com/",
                headers={"Expect": "100-continue"},
                content=b"Hello, world!",
            )

        assert not conn.is_idle()
        assert not conn.is_available()
        assert conn.is_closed()



@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_http11_expect_100_continue_write_error():
    """
    If sending the request body fails after "100 Continue" was received,
    then the connection lifecycle still ends neatly.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ],
        fail_on_write=1,
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "http://example.com/",
            headers={"Expect": "100-continue"},
            content=b"Hello, world!",
        )
        assert response.status == 200
        assert response.content == b""

        assert not conn.is_idle()
        assert not conn.is_available()
        assert conn.is_closed()
