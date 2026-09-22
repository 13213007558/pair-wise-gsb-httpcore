import ssl
import typing

import anyio
import hpack
import hyperframe.frame
import pytest
import httpcore


class AsyncOneShotRequestBody:
    def __init__(self) -> None:
        self.read_count = 0

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
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


class ConcurrentTLSStream(httpcore.AsyncMockStream):
    def __init__(
        self,
        backend: "ConcurrentTLSBackend",
        outcome: typing.Dict[str, typing.Any],
        started: anyio.Event,
        release: anyio.Event,
    ) -> None:
        super().__init__(http2_buffer())
        self._backend = backend
        self._outcome = outcome
        self._started = started
        self._release = release
        self._tls_established = False
        self.failed_close_count = 0

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> httpcore.AsyncNetworkStream:
        self._backend.handshake_starts += 1
        self._started.set()
        if self._outcome.get("timeout"):
            try:
                with anyio.fail_after(timeout):
                    await anyio.sleep_forever()
            except TimeoutError:
                raise httpcore.ConnectTimeout("timed out")
        elif self._outcome.get("wait"):
            await self._release.wait()
        if "error" in self._outcome:
            raise self._outcome["error"]
        self._tls_established = True
        return self

    async def aclose(self) -> None:
        if not self._tls_established:
            self.failed_close_count += 1
        await super().aclose()

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return FakeSSLObject() if self._tls_established else None
        return super().get_extra_info(info)


class ConcurrentTLSBackend(httpcore.AsyncNetworkBackend):
    def __init__(
        self,
        outcomes: typing.List[typing.Dict[str, typing.Any]],
        proxy: bool = False,
    ) -> None:
        self._outcomes = list(outcomes)
        self._proxy = proxy
        self.handshake_starts = 0
        self.tcp_attempts: typing.List[typing.Tuple[str, int]] = []
        self.streams: typing.List[ConcurrentTLSStream] = []
        self.events: typing.List[typing.Tuple[anyio.Event, anyio.Event]] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        self.tcp_attempts.append((host, port))
        started = anyio.Event()
        release = anyio.Event()
        self.events.append((started, release))
        outcome = self._outcomes.pop(0)
        buffer = (
            [b"HTTP/1.1 200 OK\r\n\r\n", *http2_buffer()]
            if self._proxy
            else http2_buffer()
        )
        stream = ConcurrentTLSStream(self, outcome, started, release)
        stream._buffer = buffer
        self.streams.append(stream)
        return stream

    async def sleep(self, seconds: float) -> None:
        pass

    @property
    def failed_close_count(self) -> int:
        return sum(stream.failed_close_count for stream in self.streams)


def make_pool(backend: ConcurrentTLSBackend, proxy: bool) -> httpcore.AsyncConnectionPool:
    return httpcore.AsyncConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/") if proxy else None,
        network_backend=backend,
        http2=True,
        max_connections=1,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
async def test_concurrent_requests_do_not_share_failing_handshake(
    proxy: bool,
) -> None:
    backend = ConcurrentTLSBackend(
        [{"wait": True, "error": httpcore.ConnectError("failed")}, {}], proxy
    )
    first_body = AsyncOneShotRequestBody()
    second_body = AsyncOneShotRequestBody()
    responses: typing.List[httpcore.Response] = []
    errors: typing.List[BaseException] = []

    async def first_request(pool: httpcore.AsyncConnectionPool) -> None:
        try:
            await pool.request(
                "POST", "https://example.com/", content=first_body
            )
        except BaseException as exc:
            errors.append(exc)

    async def second_request(pool: httpcore.AsyncConnectionPool) -> None:
        response = await pool.request(
            "POST", "https://example.com/", content=second_body
        )
        responses.append(response)

    async with make_pool(backend, proxy) as pool:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(first_request, pool)
            while not backend.events:
                await anyio.sleep(0)
            await backend.events[0][0].wait()
            task_group.start_soon(second_request, pool)
            await anyio.sleep(0.05)
            assert backend.handshake_starts == 1
            assert len(backend.tcp_attempts) == 1
            backend.events[0][1].set()

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


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
async def test_tls_handshake_timeout_returns_pool_capacity(proxy: bool) -> None:
    backend = ConcurrentTLSBackend([{"timeout": True}], proxy)
    body = AsyncOneShotRequestBody()

    async with make_pool(backend, proxy) as pool:
        with pytest.raises(httpcore.ConnectTimeout):
            await pool.request(
                "POST",
                "https://example.com/",
                content=body,
                extensions={"timeout": {"connect": 0.001}},
            )

        assert not pool.connections

    assert backend.handshake_starts == 1
    assert backend.failed_close_count == 1
    assert body.read_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("proxy", [False, True])
async def test_cancelled_tls_handshake_returns_pool_capacity(proxy: bool) -> None:
    backend = ConcurrentTLSBackend([{"wait": True}], proxy)
    body = AsyncOneShotRequestBody()

    async with make_pool(backend, proxy) as pool:
        async def request() -> None:
            await pool.request(
                "POST", "https://example.com/", content=body
            )

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(request)
            while not backend.events:
                await anyio.sleep(0)
            await backend.events[0][0].wait()
            task_group.cancel_scope.cancel()

        assert not pool.connections

    assert backend.handshake_starts == 1
    assert backend.failed_close_count == 1
    assert body.read_count == 0
