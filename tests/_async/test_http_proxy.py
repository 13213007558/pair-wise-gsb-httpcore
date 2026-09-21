import ssl
import typing

import hpack
import hyperframe.frame
import pytest
import trio as concurrency

from httpcore import (
    SOCKET_OPTION,
    ConnectError,
    AsyncConnectionPool,
    AsyncMockBackend,
    AsyncMockStream,
    AsyncNetworkStream,
    Origin,
    Proxy,
    ProxyError,
    ReadTimeout,
)


@pytest.mark.anyio
async def test_proxy_forwarding():
    """
    Send an HTTP request via a proxy.
    """
    network_backend = AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        max_connections=10,
        network_backend=network_backend,
    ) as proxy:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with proxy.stream("GET", "http://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<AsyncForwardHTTPConnection ['http://localhost:8080', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == [
            "<AsyncForwardHTTPConnection ['http://localhost:8080', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a forwarding proxy can only handle HTTP requests to the same origin.
        assert proxy.connections[0].can_handle_request(
            Origin(b"http", b"example.com", 80)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"http", b"other.com", 80)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"https", b"example.com", 443)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"https", b"other.com", 443)
        )


@pytest.mark.anyio
async def test_proxy_tunneling():
    """
    Send an HTTPS request via a proxy.
    """
    network_backend = AsyncMockBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<AsyncTunnelHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == [
            "<AsyncTunnelHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(
            Origin(b"http", b"example.com", 80)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"http", b"other.com", 80)
        )
        assert proxy.connections[0].can_handle_request(
            Origin(b"https", b"example.com", 443)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"https", b"other.com", 443)
        )


# We need to adapt the mock backend here slightly in order to deal
# with the proxy case. We do not want the initial connection to the proxy
# to indicate an HTTP/2 connection, but we do want it to indicate HTTP/2
# once the SSL upgrade has taken place.
class HTTP1ThenHTTP2Stream(AsyncMockStream):
    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> AsyncNetworkStream:
        self._http2 = True
        return self


class HTTP1ThenHTTP2Backend(AsyncMockBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None,
    ) -> AsyncNetworkStream:
        return HTTP1ThenHTTP2Stream(list(self._buffer))


@pytest.mark.anyio
async def test_proxy_tunneling_http2():
    """
    Send an HTTP/2 request via a proxy.
    """
    network_backend = HTTP1ThenHTTP2Backend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
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
        ],
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
        http2=True,
    ) as proxy:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<AsyncTunnelHTTPConnection ['https://example.com:443', HTTP/2, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == [
            "<AsyncTunnelHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(
            Origin(b"http", b"example.com", 80)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"http", b"other.com", 80)
        )
        assert proxy.connections[0].can_handle_request(
            Origin(b"https", b"example.com", 443)
        )
        assert not proxy.connections[0].can_handle_request(
            Origin(b"https", b"other.com", 443)
        )


@pytest.mark.anyio
async def test_proxy_tunneling_with_403():
    """
    Send an HTTPS request via a proxy.
    """
    network_backend = AsyncMockBackend(
        [
            b"HTTP/1.1 403 Permission Denied\r\n" b"\r\n",
        ]
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError) as exc_info:
            await proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == "403 Permission Denied"
        assert not proxy.connections


@pytest.mark.anyio
async def test_proxy_tunneling_with_auth():
    """
    Send an authenticated HTTPS request via a proxy.
    """
    network_backend = AsyncMockBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with AsyncConnectionPool(
        proxy=Proxy(
            url="http://localhost:8080/",
            auth=("username", "password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"


def test_proxy_headers():
    proxy = Proxy(
        url="http://localhost:8080/",
        auth=("username", "password"),
    )
    assert proxy.headers == [
        (b"Proxy-Authorization", b"Basic dXNlcm5hbWU6cGFzc3dvcmQ=")
    ]


class SequenceBackend(AsyncMockBackend):
    def __init__(self, stream_factory: typing.Callable[[], AsyncNetworkStream]):
        self._stream_factory = stream_factory
        self.connect_count = 0
        self.streams: typing.List[AsyncNetworkStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None,
    ) -> AsyncNetworkStream:
        self.connect_count += 1
        stream = self._stream_factory()
        self.streams.append(stream)
        return stream


class SequenceStream(AsyncMockStream):
    def __init__(self, responses: typing.List[bytes]):
        super().__init__(responses)
        self.requests: typing.List[bytes] = []
        self._write_buffer = b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._write_buffer += buffer
        while self._write_buffer:
            header_end = self._write_buffer.find(b"\r\n\r\n")
            if header_end < 0:
                return

            raw_headers = self._write_buffer[: header_end + 4]
            headers = raw_headers.lower()
            if b"transfer-encoding: chunked" in headers:
                body = self._write_buffer[header_end + 4 :]
                if not body.endswith(b"0\r\n\r\n"):
                    return
                length = header_end + 4 + len(body)
            else:
                content_length = 0
                for line in raw_headers.split(b"\r\n")[1:-2]:
                    key, _, value = line.partition(b":")
                    if key.lower() == b"content-length":
                        content_length = int(value.strip())
                body_start = header_end + 4
                length = body_start + content_length
                if len(self._write_buffer) < length:
                    return

            self.requests.append(self._write_buffer[:length])
            self._write_buffer = self._write_buffer[length:]


def request_header(request: bytes, name: bytes) -> bytes | None:
    for line in request.split(b"\r\n")[1:]:
        key, separator, value = line.partition(b":")
        if separator and key.lower() == name.lower():
            return value.strip()
    return None


def remote_response() -> typing.List[bytes]:
    return [
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Type: plain/text\r\n",
        b"Content-Length: 13\r\n",
        b"\r\n",
        b"Hello, world!",
    ]


@pytest.mark.trio
async def test_proxy_tunneling_100_then_407_then_200():
    def make_stream() -> SequenceStream:
        return SequenceStream(
            [
                b"HTTP/1.1 100 Continue\r\n\r\n",
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Proxy-Authenticate: Basic realm=proxy\r\n",
                b"Content-Length: 0\r\n",
                b"\r\n",
                b"HTTP/1.1 200 Connection Established\r\n\r\n",
                *remote_response(),
            ]
        )

    network_backend = SequenceBackend(make_stream)
    challenges = []

    async def auth_callback(response):
        challenges.append(response)
        return [(b"Proxy-Authorization", b"Basic dXNlcjpwYXNz")]

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request(
            "GET",
            "https://example.com/path",
            extensions={"proxy_auth_callback": auth_callback},
        )

        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(challenges) == 1
        assert challenges[0].status == 407
        assert network_backend.connect_count == 1

        actual_stream: SequenceStream = proxy.connections[0]._connection._network_stream  # type: ignore[attr-defined]
        assert len(actual_stream.requests) == 3
        assert actual_stream.requests[0].startswith(
            b"CONNECT example.com:443 HTTP/1.1\r\n"
        )
        assert request_header(actual_stream.requests[0], b"proxy-authorization") is None
        assert request_header(actual_stream.requests[1], b"proxy-authorization") == (
            b"Basic dXNlcjpwYXNz"
        )
        assert actual_stream.requests[2].startswith(
            b"GET /path HTTP/1.1\r\n"
        )


@pytest.mark.trio
async def test_proxy_tunneling_407_body_is_drained_before_retry():
    def make_stream() -> SequenceStream:
        return SequenceStream(
            [
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 5\r\n",
                b"\r\n",
                b"error",
                b"HTTP/1.1 200 Connection Established\r\n\r\n",
                *remote_response(),
            ]
        )

    network_backend = SequenceBackend(make_stream)
    bodies = []

    async def auth_callback(response):
        bodies.append(response.content)
        return [(b"Proxy-Authorization", b"Basic dXNlcjpwYXNz")]

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request(
            "GET",
            "https://example.com/",
            extensions={"proxy_auth_callback": auth_callback},
        )

        assert response.status == 200
        assert bodies == [b"error"]
        assert network_backend.connect_count == 1


@pytest.mark.trio
async def test_proxy_tunneling_auth_callback_from_proxy_configuration():
    def make_stream() -> SequenceStream:
        return SequenceStream(
            [
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 0\r\n",
                b"\r\n",
                b"HTTP/1.1 200 Connection Established\r\n\r\n",
                *remote_response(),
            ]
        )

    network_backend = SequenceBackend(make_stream)

    async def auth_callback(response):
        assert response.status == 407
        return [(b"Proxy-Authorization", b"Bearer token")]

    async with AsyncConnectionPool(
        proxy=Proxy(
            url="http://localhost:8080/",
            auth_callback=auth_callback,
        ),
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request("GET", "https://example.com/")

        assert response.status == 200
        stream: SequenceStream = proxy.connections[0]._connection._network_stream  # type: ignore[attr-defined]
        assert request_header(stream.requests[1], b"proxy-authorization") == (
            b"Bearer token"
        )


@pytest.mark.trio
async def test_proxy_tunneling_auth_callback_retries_only_once():
    def make_stream() -> SequenceStream:
        return SequenceStream(
            [
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 0\r\n",
                b"\r\n",
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 0\r\n",
                b"\r\n",
            ]
        )

    network_backend = SequenceBackend(make_stream)
    challenges = []

    async def auth_callback(response):
        challenges.append(response.status)
        return [(b"Proxy-Authorization", b"Bearer token")]

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError, match="407"):
            await proxy.request(
                "GET",
                "https://example.com/",
                extensions={"proxy_auth_callback": auth_callback},
            )

        assert challenges == [407]
        assert not proxy.connections

    stream = typing.cast(SequenceStream, network_backend.streams[0])
    assert len(stream.requests) == 2


class AsyncOneShotBody:
    def __init__(self) -> None:
        self.iterations = 0

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        if self.iterations:
            raise StopAsyncIteration
        self.iterations += 1
        return b"request body"


@pytest.mark.trio
async def test_proxy_tunneling_does_not_replay_request_body_on_auth_retry():
    body = AsyncOneShotBody()

    def make_stream() -> SequenceStream:
        return SequenceStream(
            [
                b"HTTP/1.1 100 Continue\r\n\r\n",
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 0\r\n",
                b"\r\n",
                b"HTTP/1.1 200 Connection Established\r\n\r\n",
                *remote_response(),
            ]
        )

    network_backend = SequenceBackend(make_stream)

    async def auth_callback(response):
        return [(b"Proxy-Authorization", b"Basic dXNlcjpwYXNz")]

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request(
            "POST",
            "https://example.com/",
            content=body,
            extensions={"proxy_auth_callback": auth_callback},
        )

        assert response.status == 200
        assert body.iterations == 1
        actual_stream: SequenceStream = proxy.connections[0]._connection._network_stream  # type: ignore[attr-defined]
        final_request = actual_stream.requests[-1]
        assert final_request.count(b"request body") == 1
        assert b"request body" in final_request.split(b"\r\n\r\n", 1)[1]


class TimeoutStream(SequenceStream):
    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if not self._buffer:
            if timeout is None:
                await concurrency.sleep_forever()
            await concurrency.sleep(timeout)
            raise ReadTimeout("Timed out")
        return await super().read(max_bytes, timeout)


@pytest.mark.trio
async def test_proxy_tunneling_connect_timeout_covers_failure_body():
    def make_stream() -> TimeoutStream:
        return TimeoutStream(
            [
                b"HTTP/1.1 407 Proxy Authentication Required\r\n",
                b"Content-Length: 100\r\n",
                b"\r\n",
            ]
        )

    async def auth_callback(response):
        return []

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=SequenceBackend(make_stream),
    ) as proxy:
        with pytest.raises(ReadTimeout):
            await proxy.request(
                "GET",
                "https://example.com/",
                extensions={
                    "proxy_auth_callback": auth_callback,
                    "timeout": {"connect": 0.001},
                },
            )
        assert not proxy.connections


class TLSDeadlineStream(SequenceStream):
    def __init__(
        self,
        responses: typing.List[bytes],
        tls_timeouts: typing.List[float | None],
    ):
        super().__init__(responses)
        self.tls_timeouts = tls_timeouts

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._buffer:
            await concurrency.sleep(0.01)
        return await super().read(max_bytes, timeout)

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        self.tls_timeouts.append(timeout)
        raise ConnectError("TLS upgrade failed")


@pytest.mark.trio
async def test_proxy_tunneling_connect_timeout_covers_tls_upgrade():
    tls_timeouts: typing.List[float | None] = []
    network_backend = SequenceBackend(
        lambda: TLSDeadlineStream(
            [b"HTTP/1.1 200 Connection Established\r\n\r\n"],
            tls_timeouts,
        )
    )

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ConnectError):
            await proxy.request(
                "GET",
                "https://example.com/",
                extensions={"timeout": {"connect": 0.05}},
            )

        assert tls_timeouts[0] is not None
        assert tls_timeouts[0] < 0.05
        assert not proxy.connections


class TLSUpgradeFailureStream(SequenceStream):
    def __init__(
        self,
        active: typing.List[int],
        maximum: typing.List[int],
        gate: typing.Any,
        release_gate: typing.Any,
        arrival_gate: typing.Any | None = None,
    ) -> None:
        super().__init__([b"HTTP/1.1 200 Connection Established\r\n\r\n"])
        self.closed = False
        self._active = active
        self._maximum = maximum
        self._gate = gate
        self._release_gate = release_gate
        self._arrival_gate = arrival_gate

    async def aclose(self) -> None:
        self.closed = True
        await super().aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        self._active.append(1)
        self._maximum[0] = max(self._maximum[0], len(self._active))
        if len(self._active) == 2 and self._arrival_gate is not None:
            self._arrival_gate.set()
        await self._gate.wait()
        await self._release_gate.wait()
        self._active.pop()
        raise ConnectError("TLS upgrade failed")


@pytest.mark.trio
async def test_proxy_tunneling_tls_upgrade_failure_closes_stream():
    release_gate = concurrency.Event()
    release_gate.set()
    gate = concurrency.Event()
    gate.set()
    stream = TLSUpgradeFailureStream([], [0], gate, release_gate)

    with pytest.raises(ConnectError):
        async with AsyncConnectionPool(
            proxy=Proxy(url="http://localhost:8080/"),
            network_backend=SequenceBackend(lambda: stream),
        ) as proxy:
            await proxy.request("GET", "https://example.com/")

    assert stream.closed
    assert not proxy.connections


@pytest.mark.trio
async def test_proxy_tunneling_concurrent_failures_release_connection_slots():
    first_pair_gate = concurrency.Event()
    remaining_gate = concurrency.Event()
    first_pair_arrived = concurrency.Event()
    active: typing.List[int] = []
    maximum = [0]
    failures: typing.List[BaseException] = []

    def make_stream() -> TLSUpgradeFailureStream:
        stream_count = network_backend.connect_count + 1
        gate = first_pair_gate if stream_count <= 2 else remaining_gate
        release_gate = concurrency.Event()
        release_gate.set()
        return TLSUpgradeFailureStream(
            active, maximum, gate, release_gate, first_pair_arrived
        )

    network_backend = SequenceBackend(make_stream)

    async def fetch() -> None:
        try:
            await proxy.request("GET", "https://example.com/")
        except BaseException as exc:
            failures.append(exc)

    async with AsyncConnectionPool(
        proxy=Proxy(url="http://localhost:8080/"),
        network_backend=network_backend,
        max_connections=2,
    ) as proxy:
        async with concurrency.open_nursery() as nursery:
            for _ in range(5):
                nursery.start_soon(fetch)
            await first_pair_arrived.wait()
            first_pair_gate.set()
            remaining_gate.set()

    assert len(failures) == 5
    assert all(isinstance(exc, ConnectError) for exc in failures)
    assert maximum[0] == 2
    assert network_backend.connect_count == 5
