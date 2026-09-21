from __future__ import annotations

import typing

import pytest

import httpcore

RESPONSE = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Length: 2\r\n",
    b"\r\n",
    b"ok",
]


class RecordingStream(httpcore.AsyncMockStream):
    def __init__(
        self,
        writes: list[bytes],
        connect_hosts: list[str],
        tls_hostnames: list[str | None],
        buffer: list[bytes],
        http2: bool = False,
    ) -> None:
        super().__init__(buffer, http2=http2)
        self.writes = writes
        self.connect_hosts = connect_hosts
        self.tls_hostnames = tls_hostnames

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.writes.append(buffer)

    async def start_tls(
        self,
        ssl_context: typing.Any,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> RecordingStream:
        self.tls_hostnames.append(server_hostname)
        return self


class RecordingBackend(httpcore.AsyncNetworkBackend):
    def __init__(
        self,
        buffer: list[bytes] | None = None,
        http2: bool = False,
        reuse_stream: bool = False,
    ) -> None:
        self.buffer = list(RESPONSE if buffer is None else buffer)
        self.http2 = http2
        self.reuse_stream = reuse_stream
        self.stream: RecordingStream | None = None
        self.writes: list[bytes] = []
        self.connect_hosts: list[str] = []
        self.connect_ports: list[int] = []
        self.tls_hostnames: list[str | None] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> RecordingStream:
        self.connect_hosts.append(host)
        self.connect_ports.append(port)
        if self.reuse_stream and self.stream is not None:
            return self.stream
        self.stream = RecordingStream(
            self.writes,
            self.connect_hosts,
            self.tls_hostnames,
            list(self.buffer),
            http2=self.http2,
        )
        return self.stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> RecordingStream:
        raise AssertionError("Expected a TCP connection")

    async def sleep(self, seconds: float) -> None:
        pass


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "raw_path", "host", "socket_host", "port", "tls_hostname"),
    [
        (
            "http://[fe80::1%25en0]/path?x=1",
            b"/path?x=1",
            b"[fe80::1%25en0]",
            "fe80::1%en0",
            80,
            None,
        ),
        (
            "https://[fe80::1%25en0]:8443/path",
            b"/path",
            b"[fe80::1%25en0]:8443",
            "fe80::1%en0",
            8443,
            "fe80::1",
        ),
    ],
)
async def test_ipv6_zone_http11_request_forms(
    url, raw_path, host, socket_host, port, tls_hostname
):
    network_backend = RecordingBackend()
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        response = await pool.request("GET", url)
        assert response.status == 200

    wire = b"".join(network_backend.writes)
    assert wire.startswith(b"GET " + raw_path + b" HTTP/1.1\r\n")
    assert b"\r\nHost: " + host + b"\r\n" in wire
    assert network_backend.connect_hosts == [socket_host]
    assert network_backend.connect_ports == [port]
    assert network_backend.tls_hostnames == (
        [tls_hostname] if tls_hostname is not None else []
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("urls", "expected_connections"),
    [
        (
            [
                "http://[fe80::1%25en0]/first",
                "http://[FE80::1%25en0]/second",
            ],
            1,
        ),
        (
            [
                "http://[fe80::1%25en0]/",
                "http://[fe80::1%25eth1]/",
            ],
            2,
        ),
        (
            [
                "http://[fe80::1%25en0]/",
                "http://[fe80::1%25en0]:8080/",
            ],
            2,
        ),
    ],
)
async def test_ipv6_zone_pool_keys(urls, expected_connections):
    network_backend = RecordingBackend(buffer=RESPONSE * len(urls), reuse_stream=True)
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
        max_connections=10,
        max_keepalive_connections=10,
    ) as pool:
        for url in urls:
            response = await pool.request("GET", url)
            assert response.status == 200
        assert len(pool.connections) == expected_connections
        assert len(network_backend.connect_hosts) == expected_connections


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "authority", "socket_host", "target_extension"),
    [
        ("http://[fe80::1%25en0]/", b"[fe80::1%25en0]", "fe80::1%en0", None),
        (
            "http://[fe80::1%25en0]:8080/path",
            b"[fe80::1%25en0]:8080",
            "fe80::1%en0",
            b"/absolute-target",
        ),
    ],
)
async def test_ipv6_zone_forward_proxy_uses_absolute_url(
    url, authority, socket_host, target_extension
):
    network_backend = RecordingBackend()
    async with httpcore.AsyncConnectionPool(
        proxy=httpcore.Proxy("http://127.0.0.1:8080"),
        network_backend=network_backend,
        max_connections=1,
    ) as pool:
        extensions = (
            {"target": target_extension} if target_extension is not None else None
        )
        response = await pool.request("GET", url, extensions=extensions)
        assert response.status == 200

    wire = b"".join(network_backend.writes)
    parsed = httpcore.URL(url)
    expected_target = target_extension or parsed.target
    expected_url = parsed.copy(raw_path=expected_target)
    assert wire.startswith(b"GET " + bytes(expected_url) + b" HTTP/1.1\r\n")
    assert b"\r\nHost: " + authority + b"\r\n" in wire
    assert network_backend.connect_hosts == ["127.0.0.1"]
    assert network_backend.connect_ports == [8080]
    assert socket_host not in network_backend.connect_hosts


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "connect_authority", "socket_host"),
    [
        (
            "https://[fe80::1%25en0]/path",
            b"[fe80::1%25en0]:443",
            "fe80::1%en0",
        ),
        (
            "https://[fe80::1%25en0]:8443/path",
            b"[fe80::1%25en0]:8443",
            "fe80::1%en0",
        ),
    ],
)
async def test_ipv6_zone_tunnel_proxy_targets(url, connect_authority, socket_host):
    network_backend = RecordingBackend(
        buffer=[
            b"HTTP/1.1 200 OK\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 2\r\n",
            b"\r\n",
            b"ok",
        ]
    )
    async with httpcore.AsyncConnectionPool(
        proxy=httpcore.Proxy("http://127.0.0.1:8080"),
        network_backend=network_backend,
        max_connections=1,
    ) as pool:
        response = await pool.request("GET", url)
        assert response.status == 200

    wire = b"".join(network_backend.writes)
    assert (
        b"CONNECT " + connect_authority + b" HTTP/1.1\r\n"
        b"Host: " + connect_authority + b"\r\n"
    ) in wire
    assert b"\r\n\r\nGET /path HTTP/1.1\r\n" in wire
    assert b"\r\nHost: " + connect_authority + b"\r\n" in wire
    assert network_backend.connect_hosts == ["127.0.0.1"]
    assert socket_host not in network_backend.connect_hosts
    assert network_backend.tls_hostnames == ["fe80::1"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "authority", "raw_path", "socket_host"),
    [
        (
            "https://[fe80::1%25en0]/path?x=1",
            b"[fe80::1%25en0]",
            b"/path?x=1",
            "fe80::1%en0",
        ),
        (
            "https://[fe80::1%25en0]:8443/path",
            b"[fe80::1%25en0]:8443",
            b"/path",
            "fe80::1%en0",
        ),
    ],
)
async def test_ipv6_zone_http2_pseudo_headers(
    monkeypatch, url, authority, raw_path, socket_host
):
    origin = httpcore.URL(url).origin
    connection = httpcore.AsyncHTTP2Connection(
        origin=origin, stream=httpcore.AsyncMockStream([], http2=True)
    )

    captured: dict[str, object] = {}
    original_send_headers = connection._h2_state.send_headers

    def send_headers(stream_id, headers, **kwargs):
        captured["headers"] = list(headers)
        return original_send_headers(stream_id, headers, **kwargs)

    monkeypatch.setattr(connection._h2_state, "send_headers", send_headers)

    async def write_outgoing_data(request: typing.Any) -> None:
        pass

    monkeypatch.setattr(connection, "_write_outgoing_data", write_outgoing_data)

    request = httpcore.Request(
        "GET", url, headers={"Host": httpcore.URL(url).authority}
    )
    connection._h2_state.initiate_connection()
    connection._sent_connection_init = True
    await connection._send_request_headers(request=request, stream_id=1)

    headers = dict(typing.cast(list[tuple[bytes, bytes]], captured["headers"]))
    assert headers[b":method"] == b"GET"
    assert headers[b":authority"] == authority
    assert headers[b":scheme"] == b"https"
    assert headers[b":path"] == raw_path
    assert b"host" not in headers
    assert origin.socket_host == socket_host
    assert origin.tls_hostname == "fe80::1"
