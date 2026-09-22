import ssl
import typing

import hpack
import hyperframe.frame
import pytest

import httpcore


class OneShotRequestBody:
    def __init__(self) -> None:
        self.read_count = 0

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if self.read_count:
            raise RuntimeError("Request body cannot be replayed")
        self.read_count += 1
        yield b"request body"


class FakeSSLObject:
    def __init__(self, alpn_protocol: str) -> None:
        self._alpn_protocol = alpn_protocol

    def selected_alpn_protocol(self) -> str:
        return self._alpn_protocol


class RecordingSSLContext(ssl.SSLContext):
    def __init__(self, protocol: int = ssl.PROTOCOL_TLS_CLIENT) -> None:
        self.alpn_protocols: typing.Tuple[str, ...] = ()

    def set_alpn_protocols(self, alpn_protocols: typing.List[str]) -> None:
        self.alpn_protocols = tuple(alpn_protocols)
        super().set_alpn_protocols(alpn_protocols)


class TLSScriptedStream(httpcore.AsyncMockStream):
    def __init__(
        self,
        backend: "TLSScriptedBackend",
        buffer: typing.List[bytes],
        outcome: typing.Dict[str, typing.Any],
    ) -> None:
        super().__init__(buffer)
        self._backend = backend
        self._outcome = outcome
        self._tls_established = False
        self.failed_close_count = 0

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> httpcore.AsyncNetworkStream:
        self._backend.handshakes.append(
            {
                "ssl_context": ssl_context,
                "server_hostname": server_hostname,
                "timeout": timeout,
            }
        )
        if "error" in self._outcome:
            raise self._outcome["error"]
        self._tls_established = True
        self._alpn_protocol = self._outcome["alpn"]
        return self

    async def aclose(self) -> None:
        if not self._tls_established:
            self.failed_close_count += 1
        await super().aclose()

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return FakeSSLObject(self._alpn_protocol) if self._tls_established else None
        return super().get_extra_info(info)


class TLSScriptedBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, outcomes: typing.List[typing.Dict[str, typing.Any]], http2: bool, proxy: bool) -> None:
        self._outcomes = list(outcomes)
        self._http2 = http2
        self._proxy = proxy
        self.tcp_attempts: typing.List[typing.Tuple[str, int]] = []
        self.handshakes: typing.List[typing.Dict[str, typing.Any]] = []
        self.streams: typing.List[TLSScriptedStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        self.tcp_attempts.append((host, port))
        stream = TLSScriptedStream(
            self, http1_or_http2_buffer(self._http2, self._proxy), self._outcomes.pop(0)
        )
        self.streams.append(stream)
        return stream

    async def sleep(self, seconds: float) -> None:
        pass

    @property
    def failed_close_count(self) -> int:
        return sum(stream.failed_close_count for stream in self.streams)


def http1_buffer() -> typing.List[bytes]:
    return [
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Type: text/plain\r\n",
        b"Content-Length: 13\r\n",
        b"\r\n",
        b"Hello, world!",
    ]


def http2_buffer() -> typing.List[bytes]:
    return [
        hyperframe.frame.SettingsFrame().serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=1,
            data=hpack.Encoder().encode(
                [(b":status", b"200"), (b"content-type", b"text/plain")]
            ),
            flags=["END_HEADERS"],
        ).serialize(),
        hyperframe.frame.DataFrame(
            stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
        ).serialize(),
    ]


def http1_or_http2_buffer(http2: bool, proxy: bool) -> typing.List[bytes]:
    buffer = http2_buffer() if http2 else http1_buffer()
    if proxy:
        return [b"HTTP/1.1 200 OK\r\n\r\n", *buffer]
    return buffer


def make_pool(
    backend: TLSScriptedBackend, proxy: bool, retries: int, ssl_context: ssl.SSLContext
) -> httpcore.AsyncConnectionPool:
    return httpcore.AsyncConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/") if proxy else None,
        network_backend=backend,
        http2=True,
        retries=retries,
        max_connections=1,
        ssl_context=ssl_context,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
@pytest.mark.parametrize("alpn", ["http/1.1", "h2"])
async def test_tls_handshake_failures_then_success(
    alpn: str, proxy: bool
) -> None:
    ssl_context = RecordingSSLContext(ssl.PROTOCOL_TLS_CLIENT)
    backend = TLSScriptedBackend(
        [
            {"error": httpcore.ConnectError("certificate verify failed")},
            {"error": httpcore.ConnectTimeout("handshake timeout")},
            {"alpn": alpn},
        ],
        http2=alpn == "h2",
        proxy=proxy,
    )
    body = OneShotRequestBody()

    async with make_pool(backend, proxy, retries=2, ssl_context=ssl_context) as pool:
        extensions: typing.Dict[str, typing.Any] = {"timeout": {"connect": 5.0}}
        if not proxy:
            extensions["sni_hostname"] = "sni.example.com"
        response = await pool.request(
            "POST",
            "https://example.com/",
            content=body,
            extensions=extensions,
        )

    assert response.status == 200
    assert response.content == b"Hello, world!"
    assert response.extensions["http_version"] == (
        b"HTTP/2" if alpn == "h2" else b"HTTP/1.1"
    )
    assert len(backend.tcp_attempts) == 3
    assert len(backend.handshakes) == 3
    assert backend.failed_close_count == 2
    assert body.read_count == 1
    assert all(handshake["ssl_context"] is ssl_context for handshake in backend.handshakes)
    assert all(handshake["timeout"] == 5.0 for handshake in backend.handshakes)
    assert ssl_context.alpn_protocols == ("http/1.1", "h2")
    assert backend.handshakes[-1]["server_hostname"] == (
        "sni.example.com" if not proxy else "example.com"
    )
    if proxy:
        assert backend.tcp_attempts == [("localhost", 8080)] * 3
    else:
        assert backend.tcp_attempts == [("example.com", 443)] * 3


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
@pytest.mark.parametrize(
    "error", [httpcore.ConnectError("failed"), httpcore.ConnectTimeout("timed out")]
)
async def test_tls_handshake_retry_budget_exhausted(
    proxy: bool, error: Exception
) -> None:
    ssl_context = RecordingSSLContext(ssl.PROTOCOL_TLS_CLIENT)
    backend = TLSScriptedBackend(
        [{"error": error}, {"alpn": "http/1.1"}], http2=False, proxy=proxy
    )
    body = OneShotRequestBody()

    async with make_pool(backend, proxy, retries=0, ssl_context=ssl_context) as pool:
        with pytest.raises(type(error)):
            await pool.request(
                "POST", "https://example.com/", content=body
            )
        assert not pool.connections

    assert len(backend.tcp_attempts) == 1
    assert len(backend.handshakes) == 1
    assert backend.failed_close_count == 1
    assert body.read_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
async def test_tls_handshake_retries_do_not_exceed_budget(proxy: bool) -> None:
    ssl_context = RecordingSSLContext(ssl.PROTOCOL_TLS_CLIENT)
    backend = TLSScriptedBackend(
        [
            {"error": httpcore.ConnectError("first failure")},
            {"error": httpcore.ConnectError("second failure")},
            {"alpn": "http/1.1"},
        ],
        http2=False,
        proxy=proxy,
    )
    body = OneShotRequestBody()

    async with make_pool(backend, proxy, retries=1, ssl_context=ssl_context) as pool:
        with pytest.raises(httpcore.ConnectError):
            await pool.request(
                "POST", "https://example.com/", content=body
            )
        assert not pool.connections

    assert len(backend.tcp_attempts) == 2
    assert len(backend.handshakes) == 2
    assert backend.failed_close_count == 2
    assert body.read_count == 0
