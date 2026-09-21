import base64
import threading
import time
import typing

import pytest
from tests import concurrency

import httpcore
from httpcore import ConnectError, ConnectTimeout, ProxyError
from httpcore._backends.mock import MockSSLObject


def _basic_auth(username: str, password: str) -> bytes:
    token = base64.b64encode(f"{username}:{password}".encode("ascii"))
    return b"Basic " + token


class ScriptedProxyStream(httpcore.NetworkStream):
    """
    A custom mock network stream that records everything written to it and
    serves scripted response chunks in order.
    """

    def __init__(
        self,
        buffer: typing.List[bytes],
        writes: typing.List[bytes],
        connections: typing.List["ScriptedProxyStream"],
        read_delay: float = 0.0,
        tls_error: typing.Optional[Exception] = None,
    ) -> None:
        self._buffer = list(buffer)
        self._writes = writes
        self._connections = connections
        self._read_delay = read_delay
        self._tls_error = tls_error
        self._closed = False
        self.tls_upgraded = False

    def read(self, max_bytes: int, timeout: typing.Optional[float] = None) -> bytes:
        if self._read_delay:
            # The mock enforces the remaining timeout budget handed down
            # from the CONNECT handshake.
            if timeout is not None and timeout < self._read_delay:
                raise ConnectTimeout("timed out")
            time.sleep(self._read_delay)
        if not self._buffer:
            return b""
        return self._buffer.pop(0)

    def write(self, buffer: bytes, timeout: typing.Optional[float] = None) -> None:
        self._writes.append(buffer)

    def close(self) -> None:
        self._closed = True

    def start_tls(
        self,
        ssl_context: typing.Any,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> httpcore.NetworkStream:
        if self._tls_error is not None:
            raise self._tls_error
        self.tls_upgraded = True
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return MockSSLObject(http2=False)
        return None


class ScriptedProxyBackend(httpcore.NetworkBackend):
    def __init__(
        self,
        buffer: typing.List[bytes],
        read_delay: float = 0.0,
        tls_error: typing.Optional[Exception] = None,
    ) -> None:
        self._buffer = buffer
        self._read_delay = read_delay
        self._tls_error = tls_error
        self.writes: typing.List[bytes] = []
        self.connections: typing.List[ScriptedProxyStream] = []

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.NetworkStream:
        stream = ScriptedProxyStream(
            buffer=self._buffer,
            writes=self.writes,
            connections=self.connections,
            read_delay=self._read_delay,
            tls_error=self._tls_error,
        )
        self.connections.append(stream)
        return stream


REMOTE_RESPONSE = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


def parse_requests(writes: typing.List[bytes]) -> typing.List[dict]:
    requests = []
    for raw in writes:
        lines = raw.split(b"\r\n")
        if b" " not in lines[0]:
            continue
        method, target, _version = lines[0].split(b" ", 2)
        if method not in (b"CONNECT", b"GET", b"POST"):
            continue
        headers = {}
        for line in lines[1:]:
            if not line:
                break
            key, value = line.split(b":", 1)
            headers[key.strip().lower()] = value.strip()
        requests.append({"method": method, "target": target, "headers": headers})
    return requests


def test_connect_100_then_407_then_200_with_auth_callback():
    """
    A proxy may emit `100 Continue` before the final CONNECT response.
    An initial 407 challenge must result in exactly one authenticated
    CONNECT retry on the same proxy connection, followed by a tunnel on 200.
    """
    network_backend = ScriptedProxyBackend(
        [
            # First CONNECT: interim 100, then a 407 challenge with a body.
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b'Proxy-Authenticate: Basic realm="proxy"\r\n',
            b"Content-Length: 5\r\n",
            b"\r\n",
            b"deny!",
            # Second, authenticated CONNECT succeeds.
            b"HTTP/1.1 200 Connection established\r\n\r\n",
            *REMOTE_RESPONSE,
        ]
    )
    challenges = []

    def auth_callback(challenge: typing.Optional[bytes]) -> tuple[str, str]:
        challenges.append(challenge)
        return ("username", "password")

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/", auth_callback=auth_callback),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

    assert challenges == [b'Basic realm="proxy"']

    connect_requests = [
        request
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert len(connect_requests) == 2
    assert connect_requests[0]["target"] == b"example.com:443"
    assert b"proxy-authorization" not in connect_requests[0]["headers"]
    assert connect_requests[1]["target"] == b"example.com:443"
    assert connect_requests[1]["headers"][b"proxy-authorization"] == _basic_auth(
        "username", "password"
    )

    # Only one physical connection to the proxy was opened and reused.
    assert len(network_backend.connections) == 1


def test_connect_407_with_body_is_drained_and_connection_reused():
    """
    A 407 CONNECT failure carries a response body which must be fully read
    so the keep-alive proxy connection can serve the authenticated retry.
    """
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Proxy-Authenticate: Basic\r\n",
            b"Content-Length: 11\r\n",
            b"\r\n",
            b"auth needed",
            b"HTTP/1.1 200 Connection established\r\n\r\n",
            *REMOTE_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: ("user", "pass"),
        ),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.content == b"Hello, world!"

    assert len(network_backend.connections) == 1
    connect_requests = [
        request
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert len(connect_requests) == 2


def test_established_tunnel_connection_is_pooled_and_reused():
    """
    After a 407 + authenticated 200 handshake, the resulting tunnel is a
    normal pooled connection: a second request to the same origin reuses
    it without another CONNECT or TCP connection.
    """
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Proxy-Authenticate: Basic\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
            b"HTTP/1.1 200 Connection established\r\n\r\n",
            *REMOTE_RESPONSE,
            *REMOTE_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: ("user", "pass"),
        ),
        network_backend=network_backend,
    ) as proxy:
        first = proxy.request("GET", "https://example.com/")
        assert first.status == 200
        second = proxy.request("GET", "https://example.com/")
        assert second.status == 200

    connect_requests = [
        request
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert len(connect_requests) == 2  # challenged + authenticated
    assert len(network_backend.connections) == 1


def test_connect_407_no_callback_raises_and_releases_stream():
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert "407" in str(exc_info.value)
        assert not proxy.connections

    assert network_backend.connections[0]._closed is True


def test_connect_407_callback_only_retries_once():
    """
    Even if the proxy keeps challenging, at most one authenticated retry
    is performed.
    """
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Proxy-Authenticate: Basic\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    callback_calls = []

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: (
                callback_calls.append(challenge) or ("user", "pass")
            ),
        ),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError):
            proxy.request("GET", "https://example.com/")

    connect_requests = [
        request
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert len(connect_requests) == 2
    assert len(callback_calls) == 1


def test_connect_407_callback_returning_none_fails_without_retry():
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Proxy-Authenticate: Negotiate\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    challenges = []

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: challenges.append(challenge) or None,
        ),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError):
            proxy.request("GET", "https://example.com/")

    assert challenges == [b"Negotiate"]
    connect_requests = [
        request
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert len(connect_requests) == 1


def test_non_replayable_request_body_is_sent_exactly_once():
    """
    The CONNECT handshake never consumes the request body. Even after a
    407 authenticated retry, a non-replayable streaming body is iterated
    exactly once when the real request is sent over the established tunnel.
    """
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Proxy-Authenticate: Basic\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
            b"HTTP/1.1 200 Connection established\r\n\r\n",
            *REMOTE_RESPONSE,
        ]
    )
    body_chunks = [b"chunk-1", b"chunk-2"]
    read_count = 0

    def body_stream():
        nonlocal read_count
        while body_chunks:
            read_count += 1
            yield body_chunks.pop(0)

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: ("user", "pass"),
        ),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request(
            "POST",
            "https://example.com/upload",
            content=body_stream(),
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # Both chunks were yielded once each; nothing was replayed for CONNECT.
    assert read_count == 2
    # The body only appears in the final POST sent through the tunnel.
    wire = b"".join(network_backend.writes)
    assert wire.count(b"chunk-1") == 1
    assert wire.count(b"chunk-2") == 1


def test_connect_timeout_budget_spans_handshake_and_tls():
    """
    The `connect` timeout budget accumulates across the CONNECT exchange,
    including reading the failed response body, and the subsequent TLS
    upgrade.
    """
    network_backend = ScriptedProxyBackend(
        [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
            b"HTTP/1.1 200 Connection established\r\n\r\n",
        ],
        read_delay=0.05,
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            "http://localhost:8080/",
            auth_callback=lambda challenge: ("user", "pass"),
        ),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ConnectTimeout):
            proxy.request(
                "GET",
                "https://example.com/",
                extensions={"timeout": {"connect": 0.02}},
            )

    assert network_backend.connections[0]._closed is True


def test_connect_tls_upgrade_failure_releases_stream():
    network_backend = ScriptedProxyBackend(
        [b"HTTP/1.1 200 Connection established\r\n\r\n"],
        tls_error=ConnectError("boom"),
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ConnectError):
            proxy.request("GET", "https://example.com/")
        assert not proxy.connections

    # The tunnel was established on this stream, then TLS failed: the
    # network stream must be released.
    assert network_backend.connections[0]._closed is True


def test_concurrent_requests_open_distinct_connections():
    """
    Concurrent requests to the same origin each establish their own proxy
    tunnel and connection; the pool limit is respected.
    """
    script = [
        b"HTTP/1.1 200 Connection established\r\n\r\n",
        *REMOTE_RESPONSE,
    ]

    class PerConnectionBackend(ScriptedProxyBackend):
        def __init__(self) -> None:
            super().__init__(list(script))

        def connect_tcp(self, host, port, **kwargs):
            # Give each new connection its own independent script.
            stream = ScriptedProxyStream(
                buffer=list(script),
                writes=self.writes,
                connections=self.connections,
            )
            self.connections.append(stream)
            return stream

    network_backend = PerConnectionBackend()

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"),
        network_backend=network_backend,
        max_connections=10,
    ) as proxy:

        def fetch(results):
            response = proxy.request("GET", "https://example.com/")
            results.append(response.status)

        results = []
        with concurrency.open_nursery() as nursery:
            for _ in range(5):
                nursery.start_soon(fetch, results)

    assert all(status == 200 for status in results)
    assert len(results) == 5
    assert len(network_backend.connections) == 5

    connect_targets = [
        request["target"]
        for request in parse_requests(network_backend.writes)
        if request["method"] == b"CONNECT"
    ]
    assert connect_targets == [b"example.com:443"] * 5


class HangingProxyStream(ScriptedProxyStream):
    def read(self, max_bytes: int, timeout: typing.Optional[float] = None) -> bytes:
        time.sleep(5)


class HangingProxyBackend(ScriptedProxyBackend):
    def connect_tcp(self, host, port, **kwargs):
        stream = HangingProxyStream(
            buffer=[], writes=self.writes, connections=self.connections
        )
        self.connections.append(stream)
        return stream


def test_connect_cancellation_releases_stream_and_semaphore():
    """
    A thread blocked while waiting for the CONNECT response holds a pool
    connection. Closing the pool must release the network stream.
    """
    network_backend = HangingProxyBackend([])

    proxy = httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"),
        network_backend=network_backend,
        max_connections=1,
    )
    thread = threading.Thread(
        target=proxy.request,
        args=("GET", "https://example.com/"),
        daemon=True,
    )
    thread.start()
    thread.join(0.05)
    assert thread.is_alive()
    assert len(network_backend.connections) == 1

    proxy.close()
    assert network_backend.connections[0]._closed is True
