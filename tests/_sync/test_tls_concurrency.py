import ssl
import threading
import typing

import hpack
import hyperframe.frame
import pytest

import httpcore


class SyncOneShotRequestBody:
    def __init__(self) -> None:
        self.read_count = 0

    def __iter__(self) -> typing.Iterator[bytes]:
        if self.read_count:
            raise RuntimeError("Request body cannot be replayed")
        self.read_count += 1
        yield b"request body"


class FakeSSLObject:
    def selected_alpn_protocol(self) -> str:
        return "h2"


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


class SyncConcurrentTLSStream(httpcore.MockStream):
    def __init__(
        self,
        backend: "SyncConcurrentTLSBackend",
        outcome: typing.Dict[str, typing.Any],
        started: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(http2_buffer())
        self._backend = backend
        self._outcome = outcome
        self._started = started
        self._release = release
        self._tls_established = False
        self.failed_close_count = 0

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> httpcore.NetworkStream:
        self._backend.handshake_starts += 1
        self._started.set()
        if self._outcome.get("wait"):
            self._release.wait(5)
        if "error" in self._outcome:
            raise self._outcome["error"]
        self._tls_established = True
        return self

    def close(self) -> None:
        if not self._tls_established:
            self.failed_close_count += 1
        super().close()

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return FakeSSLObject() if self._tls_established else None
        return super().get_extra_info(info)


class SyncConcurrentTLSBackend(httpcore.NetworkBackend):
    def __init__(
        self,
        outcomes: typing.List[typing.Dict[str, typing.Any]],
        proxy: bool = False,
    ) -> None:
        self._outcomes = list(outcomes)
        self._proxy = proxy
        self.handshake_starts = 0
        self.tcp_attempts: typing.List[typing.Tuple[str, int]] = []
        self.streams: typing.List[SyncConcurrentTLSStream] = []
        self.events: typing.List[
            typing.Tuple[threading.Event, threading.Event]
        ] = []

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.NetworkStream:
        self.tcp_attempts.append((host, port))
        started = threading.Event()
        release = threading.Event()
        self.events.append((started, release))
        stream = SyncConcurrentTLSStream(
            self, self._outcomes.pop(0), started, release
        )
        if self._proxy:
            stream._buffer = [b"HTTP/1.1 200 OK\r\n\r\n", *http2_buffer()]
        self.streams.append(stream)
        return stream

    def sleep(self, seconds: float) -> None:
        pass

    @property
    def failed_close_count(self) -> int:
        return sum(stream.failed_close_count for stream in self.streams)


def make_pool(
    backend: SyncConcurrentTLSBackend, proxy: bool
) -> httpcore.ConnectionPool:
    return httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/") if proxy else None,
        network_backend=backend,
        http2=True,
        max_connections=1,
    )


@pytest.mark.parametrize("proxy", [False, True])
def test_concurrent_requests_do_not_share_failing_handshake(proxy: bool) -> None:
    backend = SyncConcurrentTLSBackend(
        [{"wait": True, "error": httpcore.ConnectError("failed")}, {}], proxy
    )
    first_body = SyncOneShotRequestBody()
    second_body = SyncOneShotRequestBody()
    responses: typing.List[httpcore.Response] = []
    errors: typing.List[BaseException] = []

    def first_request(pool: httpcore.ConnectionPool) -> None:
        try:
            pool.request("POST", "https://example.com/", content=first_body)
        except BaseException as exc:
            errors.append(exc)

    def second_request(pool: httpcore.ConnectionPool) -> None:
        responses.append(
            pool.request("POST", "https://example.com/", content=second_body)
        )

    with make_pool(backend, proxy) as pool:
        first_thread = threading.Thread(target=first_request, args=(pool,))
        second_thread = threading.Thread(target=second_request, args=(pool,))
        first_thread.start()
        while not backend.events:
            threading.Event().wait(0.001)
        assert backend.events[0][0].wait(5)
        second_thread.start()
        assert backend.events[0][1].wait(0.1) is False
        assert backend.handshake_starts == 1
        assert len(backend.tcp_attempts) == 1
        backend.events[0][1].set()
        first_thread.join(5)
        second_thread.join(5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], httpcore.ConnectError)
    assert len(responses) == 1
    assert responses[0].status == 200
    assert responses[0].content == b"Hello, world!"
    assert backend.handshake_starts == 2
    assert len(backend.tcp_attempts) == 2
    assert backend.failed_close_count == 1
    assert first_body.read_count == 0
    assert second_body.read_count == 1


@pytest.mark.parametrize("proxy", [False, True])
def test_tls_handshake_timeout_returns_pool_capacity(proxy: bool) -> None:
    backend = SyncConcurrentTLSBackend([{"error": httpcore.ConnectTimeout()}], proxy)
    body = SyncOneShotRequestBody()

    with make_pool(backend, proxy) as pool:
        with pytest.raises(httpcore.ConnectTimeout):
            pool.request("POST", "https://example.com/", content=body)
        assert not pool.connections

    assert backend.handshake_starts == 1
    assert backend.failed_close_count == 1
    assert body.read_count == 0
