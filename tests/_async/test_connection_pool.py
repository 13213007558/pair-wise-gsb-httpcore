import logging
import typing

import h11
import hpack
import hyperframe.frame
import pytest
import trio as concurrency

import httpcore


class FragmentedMockStream(httpcore.AsyncMockStream):
    """
    A mock stream that fragments the buffered response into small reads,
    and records how many times it has been closed.
    """

    def __init__(
        self,
        buffer: typing.List[bytes],
        http2: bool = False,
        fragment_size: int = 3,
    ) -> None:
        super().__init__(buffer, http2)
        self._fragment_size = fragment_size
        self.close_count = 0

    async def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if self._closed or not self._buffer:
            return await super().read(max_bytes, timeout)
        fragment = self._buffer[0][: self._fragment_size]
        remainder = self._buffer[0][self._fragment_size :]
        if remainder:
            self._buffer[0] = remainder
        else:
            self._buffer.pop(0)
        return fragment

    async def aclose(self) -> None:
        self.close_count += 1
        await super().aclose()


class FragmentedMockBackend(httpcore.AsyncMockBackend):
    """
    A mock backend that hands out a fragmented stream per connection,
    using a separate response buffer for each new connection, and
    recording every stream it creates.
    """

    def __init__(
        self,
        buffers: typing.List[typing.List[bytes]],
        http2: bool = False,
        fragment_size: int = 3,
    ) -> None:
        super().__init__([], http2)
        self._buffers = buffers
        self._fragment_size = fragment_size
        self.streams: typing.List[FragmentedMockStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncMockStream:
        buffer = self._buffers[len(self.streams)]
        stream = FragmentedMockStream(
            list(buffer), http2=self._http2, fragment_size=self._fragment_size
        )
        self.streams.append(stream)
        return stream


@pytest.mark.anyio
async def test_connection_pool_with_keepalive():
    """
    By default HTTP/1.1 requests should be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_keepalive_connections=1
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a request to a different origin will not reuse the existing IDLE connection.
        async with pool.stream("GET", "http://example.com/") as response_1, pool.stream(
            "GET", "http://example.com/"
        ) as response_2:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
                "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 2 active, 0 queued | Connections: 2 active, 1 idle]>"
            )
            await response_1.aread()
            await response_2.aread()

        assert response_1.status == 200
        assert response_1.content == b"Hello, world!"
        assert response_2.status == 200
        assert response_2.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )


@pytest.mark.anyio
async def test_connection_pool_with_close():
    """
    HTTP/1.1 requests that include a 'Connection: Close' header should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream(
            "GET", "https://example.com/", headers={"Connection": "close"}
        ) as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_http2():
    """
    Test a connection pool with HTTP/2 requests.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"
        ]


@pytest.mark.anyio
async def test_connection_pool_with_http2_goaway():
    """
    Test a connection pool with HTTP/2 requests, that cleanly disconnects
    with a GoAway frame after the first request.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.GoAwayFrame(
                stream_id=0, error_code=0, last_stream_id=1
            ).serialize(),
            b"",
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will require a new connection.
        # The original connection has now been closed.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>",
        ]


@pytest.mark.anyio
async def test_trace_request():
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "https://example.com/", extensions={"trace": trace})

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.receive_response_body.started",
        "http11.receive_response_body.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_debug_request(caplog):
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    caplog.set_level(logging.DEBUG)

    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "http://example.com/")

    assert caplog.record_tuples == [
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.started host='example.com' port=80 local_address=None timeout=None socket_options=None",
        ),
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.complete return_value=<httpcore.AsyncMockStream>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_headers.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_headers.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_body.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.started request=<Request [b'GET']>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.complete return_value="
            "(b'HTTP/1.1', 200, b'OK', [(b'Content-Type', b'plain/text'), (b'Content-Length', b'13')])",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "receive_response_body.complete"),
        ("httpcore.http11", logging.DEBUG, "response_closed.started"),
        ("httpcore.http11", logging.DEBUG, "response_closed.complete"),
        ("httpcore.connection", logging.DEBUG, "close.started"),
        ("httpcore.connection", logging.DEBUG, "close.complete"),
    ]


@pytest.mark.anyio
async def test_connection_pool_with_http_exception():
    """
    HTTP/1.1 requests that result in an exception during the connection should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend([b"Wait, this isn't valid HTTP!"])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.failed",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_connect_exception():
    """
    HTTP/1.1 requests that result in an exception during connection should not
    be returned to the connection pool.
    """

    class FailedConnectBackend(httpcore.AsyncMockBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: typing.Optional[float] = None,
            local_address: typing.Optional[str] = None,
            socket_options: typing.Optional[
                typing.Iterable[httpcore.SOCKET_OPTION]
            ] = None,
        ) -> httpcore.AsyncNetworkStream:
            raise httpcore.ConnectError("Could not connect")

    network_backend = FailedConnectBackend([])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.failed",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_immediate_expiry():
    """
    Connection pools with keepalive_expiry=0.0 should immediately expire
    keep alive connections.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        keepalive_expiry=0.0,
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_no_keepalive_connections_allowed():
    """
    When 'max_keepalive_connections=0' is used, IDLE connections should not
    be returned to the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        max_keepalive_connections=0, network_backend=network_backend
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.trio
async def test_connection_pool_concurrency():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"http://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "b.com", "c.com", "d.com", "e.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Each connection was to a different host, and only sent a single
            # request on that connection.
            assert item[0] in [
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://b.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://c.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://d.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://e.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_closing():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"Connection: close\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Only a single request was sent on each connection.
            assert (
                item[0]
                == "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            )


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_keepalive():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
        * 5
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # The connection sent multiple requests.
            assert item[0] in [
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 3]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 4]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 5]>",
            ]

    assert (
        repr(pool)
        == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 0 idle]>"
    )


@pytest.mark.anyio
async def test_unsupported_protocol():
    async with httpcore.AsyncConnectionPool() as pool:
        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "ftp://www.example.com/")

        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "://www.example.com/")


@pytest.mark.anyio
async def test_connection_pool_closed_while_request_in_flight():
    """
    Closing a connection pool while a request/response is still in-flight
    should raise an error.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Send a request, and then close the connection pool while the
        # response has not yet been streamed.
        async with pool.stream("GET", "https://example.com/") as response:
            await pool.aclose()
            with pytest.raises(httpcore.ReadError):
                await response.aread()


@pytest.mark.anyio
async def test_connection_pool_timeout():
    """
    Ensure that exceeding max_connections can cause a request to timeout.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Send a request to a pool that is configured to only support a single
        # connection, and then ensure that a second concurrent request
        # fails with a timeout.
        async with pool.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                await pool.request("GET", "https://example.com/", extensions=extensions)


@pytest.mark.anyio
async def test_connection_pool_timeout_zero():
    """
    A pool timeout of 0 shouldn't raise a PoolTimeout if there's
    no need to wait on a new connection.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    # Use a pool timeout of zero.
    extensions = {"timeout": {"pool": 0}}

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two consecutive requests with a pool timeout of zero.
        # Both succeed without raising a timeout.
        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two concurrent requests with a pool timeout of zero.
        # Only the first will succeed without raising a timeout.
        async with pool.stream(
            "GET", "https://example.com/", extensions=extensions
        ) as response:
            # The first response hasn't yet completed.
            with pytest.raises(httpcore.PoolTimeout):
                # So a pool timeout occurs.
                await pool.request("GET", "https://example.com/", extensions=extensions)
            # The first response now completes.
            await response.aread()

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
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        async with pool.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
            extensions={"trace": trace},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = await network_stream.read(max_bytes=1024)
            assert content == b"..."

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_truncated_content_length():
    """
    A keep-alive response that truncates a 'Content-Length' body must
    surface the protocol error to the caller, and the broken connection
    must be discarded rather than returned to the pool as keep-alive.
    """
    network_backend = FragmentedMockBackend(
        [
            [  # First connection: the body is shorter than 'Content-Length'.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello,",
            ],
            [  # Second connection: a healthy response.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ],
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # The caller sees the original protocol error while reading the body.
        async with pool.stream("GET", "http://example.com/") as response:
            assert response.status == 200
            with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                await response.aread()

        exc = exc_info.value
        assert str(exc) == (
            "peer closed connection without sending complete message body "
            "(received 6 bytes, expected 13)"
        )
        # The raw 'h11' protocol error is preserved in the exception chain.
        assert exc.__cause__ is None
        assert exc.__suppress_context__
        assert isinstance(exc.__context__, h11.RemoteProtocolError)

        # The broken connection was closed exactly once, and discarded
        # from the pool instead of being returned as keep-alive.
        assert network_backend.streams[0].close_count == 1
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 0 idle]>"
        )

        # A follow-up request is eligible for a fresh connection.
        response = await pool.request("GET", "http://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(network_backend.streams) == 2
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 1 idle]>"
        )


@pytest.mark.anyio
async def test_connection_pool_with_truncated_chunked_body():
    """
    A chunked response that disconnects before the terminating chunk must
    surface the protocol error to the caller, and the broken connection
    must be discarded from the pool.
    """
    network_backend = FragmentedMockBackend(
        [
            [  # First connection: no terminating "0\r\n\r\n" chunk.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Transfer-Encoding: chunked\r\n",
                b"\r\n",
                b"5\r\nHello\r\n",
            ],
            [  # Second connection: a healthy response.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ],
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        async with pool.stream("GET", "http://example.com/") as response:
            assert response.status == 200
            chunks = []
            with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                async for part in response.aiter_stream():
                    chunks.append(part)

        # The complete chunks are delivered before the error is raised.
        assert b"".join(chunks) == b"Hello"
        exc = exc_info.value
        assert "incomplete chunked read" in str(exc)
        assert isinstance(exc.__context__, h11.RemoteProtocolError)

        # The broken connection was closed exactly once, and discarded.
        assert network_backend.streams[0].close_count == 1
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 0 idle]>"
        )

        # A follow-up request is eligible for a fresh connection.
        response = await pool.request("GET", "http://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(network_backend.streams) == 2


@pytest.mark.anyio
async def test_connection_pool_with_invalid_chunk():
    """
    A chunked response including an invalid chunk header must surface
    the protocol error to the caller, and the broken connection must be
    discarded from the pool.
    """
    network_backend = FragmentedMockBackend(
        [
            [  # First connection: an invalid chunk header.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Transfer-Encoding: chunked\r\n",
                b"\r\n",
                b"5\r\nHello\r\n",
                b"ZZZ\r\n",
            ],
            [  # Second connection: a healthy response.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ],
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        async with pool.stream("GET", "http://example.com/") as response:
            assert response.status == 200
            chunks = []
            with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                async for part in response.aiter_stream():
                    chunks.append(part)

        assert b"".join(chunks) == b"Hello"
        exc = exc_info.value
        assert "illegal chunk header" in str(exc)
        assert isinstance(exc.__context__, h11.RemoteProtocolError)

        # The broken connection was closed exactly once, and discarded.
        assert network_backend.streams[0].close_count == 1
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 0 idle]>"
        )

        # A follow-up request is eligible for a fresh connection.
        response = await pool.request("GET", "http://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(network_backend.streams) == 2


@pytest.mark.anyio
async def test_connection_pool_response_close_is_idempotent():
    """
    Closing a response before a protocol error would surface must be
    idempotent, and must not affect other in-flight requests that are
    using healthy connections on the same pool.
    """
    network_backend = FragmentedMockBackend(
        [
            [  # First connection: the body is shorter than 'Content-Length'.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello,",
            ],
            [  # Second connection: a healthy response.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ],
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        async with pool.stream(
            "GET", "http://example.com/"
        ) as broken_response, pool.stream(
            "GET", "http://other.com/"
        ) as healthy_response:
            # The caller closes the broken response before reading the body,
            # so the protocol error never surfaces. Closing is idempotent,
            # and racing against the context manager close is allowed.
            await broken_response.aclose()
            await broken_response.aclose()
            # Other in-flight requests on the pool are unaffected.
            await healthy_response.aread()

        assert healthy_response.status == 200
        assert healthy_response.content == b"Hello, world!"

        # The broken connection was closed exactly once, and discarded.
        assert network_backend.streams[0].close_count == 1
        # The healthy connection was not closed, and remains in the pool.
        assert network_backend.streams[1].close_count == 0
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 1 idle]>"
        )


@pytest.mark.trio
async def test_connection_pool_protocol_error_wakes_queued_requests():
    """
    When a response fails with a protocol error on a full connection pool,
    the broken connection must be discarded, and any requests queued
    waiting for a connection must be woken up and retried on a fresh
    connection.
    """
    network_backend = FragmentedMockBackend(
        [
            [  # First connection: the body is shorter than 'Content-Length'.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello,",
            ],
            [  # Second connection: a healthy response.
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ],
        ]
    )

    async def fetch_broken(pool, protocol_errors):
        async with pool.stream("GET", "http://example.com/") as response:
            # Wait until the queued request is waiting on the full pool.
            for _ in range(1000):
                if "1 queued" in repr(pool):
                    break
                await concurrency.sleep(0.001)
            assert "1 queued" in repr(pool)
            with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                await response.aread()
            protocol_errors.append(exc_info.value)

    async def fetch_queued(pool, responses):
        # Wait until the first request is holding the only connection.
        for _ in range(1000):
            if "1 active" in repr(pool):
                break
            await concurrency.sleep(0.001)
        response = await pool.request("GET", "http://example.com/")
        responses.append(response)

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        protocol_errors: typing.List[httpcore.RemoteProtocolError] = []
        responses: typing.List[httpcore.Response] = []
        async with concurrency.open_nursery() as nursery:
            nursery.start_soon(fetch_broken, pool, protocol_errors)
            nursery.start_soon(fetch_queued, pool, responses)

        # The first request saw the protocol error...
        assert len(protocol_errors) == 1
        assert isinstance(protocol_errors[0].__context__, h11.RemoteProtocolError)
        # ...and the queued request was woken up and retried
        # on a fresh connection.
        assert len(responses) == 1
        assert responses[0].status == 200
        assert responses[0].content == b"Hello, world!"

        # The broken connection was closed exactly once, and discarded.
        # The new connection remains idle in the pool.
        assert len(network_backend.streams) == 2
        assert network_backend.streams[0].close_count == 1
        assert network_backend.streams[1].close_count == 0
        assert (
            repr(pool) == "<AsyncConnectionPool [Requests: 0 active, 0 queued"
            " | Connections: 0 active, 1 idle]>"
        )


@pytest.mark.anyio
async def test_connection_pool_with_http2_stream_reset():
    """
    An HTTP/2 stream that is reset by the server mid-body must surface
    a protocol error to the caller, while the connection itself remains
    healthy and reusable for other requests.
    """
    network_backend = FragmentedMockBackend(
        [
            [
                hyperframe.frame.SettingsFrame().serialize(),
                hyperframe.frame.HeadersFrame(
                    stream_id=1,
                    data=hpack.Encoder().encode(
                        [
                            (b":status", b"200"),
                            (b"content-type", b"plain/text"),
                        ]
                    ),
                    flags=["END_HEADERS"],
                ).serialize(),
                hyperframe.frame.DataFrame(stream_id=1, data=b"Hello").serialize(),
                hyperframe.frame.RstStreamFrame(stream_id=1, error_code=8).serialize(),
                hyperframe.frame.HeadersFrame(
                    stream_id=3,
                    data=hpack.Encoder().encode(
                        [
                            (b":status", b"200"),
                            (b"content-type", b"plain/text"),
                        ]
                    ),
                    flags=["END_HEADERS"],
                ).serialize(),
                hyperframe.frame.DataFrame(
                    stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
                ).serialize(),
            ]
        ],
        http2=True,
        fragment_size=7,
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # The caller sees a protocol error when the stream is reset.
        async with pool.stream("GET", "https://example.com/") as response:
            assert response.status == 200
            with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                await response.aread()

        exc = exc_info.value
        assert exc.args[0].stream_id == 1
        assert exc.args[0].error_code == 8
        assert exc.__context__ is None

        # The connection itself is healthy: it was not closed,
        # and remains idle in the pool.
        assert network_backend.streams[0].close_count == 0
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # A follow-up request reuses the same healthy connection.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(network_backend.streams) == 1
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"
        ]
