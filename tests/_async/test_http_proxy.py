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


class RecordingStream(AsyncMockStream):
    """
    A mock stream that records everything that is written to it.
    """

    def __init__(self, buffer: typing.List[bytes]) -> None:
        super().__init__(buffer)
        self.written = b""

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        self.written += buffer


class RecordingBackend(AsyncMockBackend):
    """
    A mock backend that records connections and everything written to them.
    """

    def __init__(self, buffer: typing.List[bytes]) -> None:
        super().__init__(buffer)
        self.streams: typing.List[RecordingStream] = []
        self.connections: typing.List[typing.Tuple[str, int]] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None,
    ) -> AsyncNetworkStream:
        self.connections.append((host, port))
        stream = RecordingStream(list(self._buffer))
        self.streams.append(stream)
        return stream


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url,absolute_url,host_header",
    [
        ("http://example.com/path", b"http://example.com/path", b"example.com"),
        (
            "http://[fe80::1%25eth0]/path",
            b"http://[fe80::1%25eth0]/path",
            b"[fe80::1%25eth0]",
        ),
        (
            "http://[fe80::1%eth0]:8000/path",
            b"http://[fe80::1%25eth0]:8000/path",
            b"[fe80::1%25eth0]:8000",
        ),
    ],
)
async def test_proxy_forwarding_absolute_url_with_ipv6_zone_identifier(
    url, absolute_url, host_header
):
    """
    Requests sent via a forwarding proxy use the absolute URL as the request
    target, with a display form that encloses any IPv6 literal in brackets.
    """
    network_backend = RecordingBackend(
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
        network_backend=network_backend,
    ) as proxy:
        response = await proxy.request("GET", url)
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # The connection is made to the proxy, not to the remote host.
    assert network_backend.connections == [("localhost", 8080)]
    head = network_backend.streams[0].written.split(b"\r\n\r\n")[0]
    request_line, _, header_lines = head.partition(b"\r\n")
    headers = dict(line.split(b": ", 1) for line in header_lines.split(b"\r\n"))
    assert request_line == b"GET " + absolute_url + b" HTTP/1.1"
    assert headers[b"Host"] == host_header


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url,connect_target",
    [
        ("https://example.com/path", b"example.com:443"),
        ("https://[fe80::1%25eth0]/path", b"[fe80::1%25eth0]:443"),
        ("https://[fe80::1%eth0]:8443/path", b"[fe80::1%25eth0]:8443"),
    ],
)
async def test_proxy_tunneling_connect_target_with_ipv6_zone_identifier(
    url, connect_target
):
    """
    The CONNECT request for a tunneled proxy uses an authority-form target,
    with a display form that encloses any IPv6 literal in brackets.
    """
    network_backend = RecordingBackend(
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
        response = await proxy.request("GET", url)
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # The connection is made to the proxy, not to the remote host.
    assert network_backend.connections == [("localhost", 8080)]
    head = network_backend.streams[0].written.split(b"\r\n\r\n")[0]
    request_line, _, header_lines = head.partition(b"\r\n")
    headers = dict(line.split(b": ", 1) for line in header_lines.split(b"\r\n"))
    assert request_line == b"CONNECT " + connect_target + b" HTTP/1.1"
    assert headers[b"Host"] == connect_target
