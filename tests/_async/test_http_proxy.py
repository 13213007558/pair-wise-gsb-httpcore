import ssl
import typing

import hpack
import hyperframe.frame
import pytest

from httpcore import (
    SOCKET_OPTION,
    AsyncConnectionPool,
    AsyncMockBackend,
    AsyncMockStream,
    AsyncNetworkStream,
    ConnectError,
    Origin,
    Proxy,
    ProxyError,
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


class FlakyTLSStream(AsyncMockStream):
    """
    A mock stream that fails the TLS handshake a set number of times
    before succeeding, and that records how many times it is closed.
    """

    def __init__(
        self, backend: "FlakyTLSBackend", buffer: typing.List[bytes], http2: bool
    ) -> None:
        super().__init__(buffer, http2=False)
        self._backend = backend
        self._alpn_http2 = http2
        self.close_count = 0

    async def aclose(self) -> None:
        self.close_count += 1
        await super().aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> AsyncNetworkStream:
        if self._backend.tls_failures > 0:
            self._backend.tls_failures -= 1
            raise ConnectError("Simulated TLS handshake failure")
        # The ALPN protocol is only known once the handshake completes.
        self._http2 = self._alpn_http2
        return self


class FlakyTLSBackend(AsyncMockBackend):
    """
    A mock backend returning streams with flaky TLS handshakes.

    The 'http2' argument may be a list, allowing the ALPN protocol that
    is negotiated to vary across connection attempts.
    """

    def __init__(
        self,
        buffer: typing.List[bytes],
        tls_failures: int = 0,
        http2: typing.Union[bool, typing.List[bool]] = False,
    ) -> None:
        super().__init__(buffer)
        self.tls_failures = tls_failures
        self._http2_modes = http2 if isinstance(http2, list) else [http2]
        self.streams: typing.List[FlakyTLSStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None,
    ) -> AsyncNetworkStream:
        index = min(len(self.streams), len(self._http2_modes) - 1)
        stream = FlakyTLSStream(
            self, list(self._buffer), http2=self._http2_modes[index]
        )
        self.streams.append(stream)
        return stream


class CountingBody:
    """
    A non-replayable request body that tracks how often it is consumed.
    """

    def __init__(self, chunks: typing.List[bytes]) -> None:
        self._chunks = chunks
        self.iter_count = 0
        self.chunks_read = 0

    async def stream(self) -> typing.AsyncIterator[bytes]:
        self.iter_count += 1
        for chunk in self._chunks:
            self.chunks_read += 1
            yield chunk


@pytest.mark.anyio
async def test_proxy_tunneling_tls_failure():
    """
    If the TLS handshake with the remote host fails once a CONNECT tunnel
    has been established, then the tunnelled connection is closed, and a
    later request reconnects over a fresh tunnel.
    """
    network_backend = FlakyTLSBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ],
        tls_failures=1,
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        body = CountingBody([b"Hello, world!"])
        with pytest.raises(ConnectError):
            await proxy.request("POST", "https://example.com/", content=body.stream())

        # The failed TLS handshake closed the tunnelled connection, which
        # was removed from the pool. The request body was never read.
        assert len(network_backend.streams) == 1
        assert network_backend.streams[0].close_count == 1
        assert proxy.connections == []
        assert body.iter_count == 0

        # A later request reconnects over a fresh tunnel.
        response = await proxy.request("POST", "https://example.com/", content=body.stream())
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert len(network_backend.streams) == 2
        assert network_backend.streams[1].close_count == 0
        assert body.iter_count == 1
        assert body.chunks_read == 1


@pytest.mark.anyio
async def test_proxy_tunneling_tls_failure_with_alpn_change():
    """
    A failed TLS handshake through a proxy tunnel is closed off, and the
    protocol of a later connection follows the ALPN protocol that is
    negotiated by the successful handshake.
    """
    network_backend = FlakyTLSBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server, over HTTP/2
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
        tls_failures=1,
        # The failed attempt negotiated 'http/1.1', the successful one 'h2'.
        http2=[False, True],
    )

    async with AsyncConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
        http2=True,
    ) as proxy:
        with pytest.raises(ConnectError):
            await proxy.request("GET", "https://example.com/")

        assert len(network_backend.streams) == 1
        assert network_backend.streams[0].close_count == 1
        assert proxy.connections == []

        response = await proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"
        assert response.extensions["http_version"] == b"HTTP/2"
        assert len(network_backend.streams) == 2
        assert network_backend.streams[1].close_count == 0
