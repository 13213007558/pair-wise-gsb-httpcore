import typing

import anyio as concurrency
import h2.events
import h11
import hpack
import hyperframe.frame
import pytest

import httpcore


class FragmentedStream(httpcore.AsyncMockStream):
    def __init__(
        self,
        buffer: list[bytes],
        read_gate: concurrency.Event | None = None,
    ) -> None:
        super().__init__(buffer)
        self.read_gate = read_gate or concurrency.Event()
        self.gate_reads = read_gate is not None
        self.close_count = 0

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self.gate_reads and len(self._buffer) == 1:
            await self.read_gate.wait()
        return await super().read(max_bytes, timeout)

    async def aclose(self) -> None:
        self.close_count += 1
        await super().aclose()


class HTTP2SSLObject:
    def selected_alpn_protocol(self) -> str:
        return "h2"


class HTTP2FragmentedStream(FragmentedStream):
    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return HTTP2SSLObject()
        return super().get_extra_info(info)


class CloseFailingStream(FragmentedStream):
    async def aclose(self) -> None:
        self.close_count += 1
        raise httpcore.ReadError("close failed")


class SequenceBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, streams: list[FragmentedStream]) -> None:
        self._streams = streams

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        return self._streams.pop(0)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        return self._streams.pop(0)

    async def sleep(self, seconds: float) -> None:
        pass


class HTTP2FragmentedBackend(SequenceBackend):
    pass


SHORT_BODY = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, ",
    b"wor",
    b"",
]

BAD_CHUNK = [
    b"HTTP/1.1 200 OK\r\n",
    b"Transfer-Encoding: chunked\r\n",
    b"\r\n",
    b"5\r\nhel",
    b"loX\r\n",
]

HEALTHY_BODY = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Length: 2\r\n",
    b"\r\n",
    b"ok",
]


@pytest.mark.anyio
@pytest.mark.parametrize("buffer", [SHORT_BODY, BAD_CHUNK])
async def test_http11_protocol_error_closes_connection_and_preserves_chain(buffer):
    stream = FragmentedStream(list(buffer))
    origin = httpcore.Origin(b"http", b"example.com", 80)
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.handle_async_request(
            httpcore.Request(
                b"GET", b"http://example.com/", headers=[(b"host", b"example.com")]
            )
        )
        stream.read_gate.set()

        with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
            async for _ in response.aiter_stream():
                pass

        assert isinstance(exc_info.value.__cause__, h11.RemoteProtocolError)
        assert conn.is_closed()

        await response.aclose()
        assert stream.close_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize("buffer", [SHORT_BODY, BAD_CHUNK])
async def test_http11_protocol_error_wakes_pool_waiter(buffer):
    bad_stream = FragmentedStream(list(buffer))
    healthy_stream = FragmentedStream(list(HEALTHY_BODY))
    backend = SequenceBackend([bad_stream, healthy_stream])
    waiter_error: BaseException | None = None

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=backend
    ) as pool:
        async with concurrency.create_task_group() as task_group:

            async def bad_request() -> None:
                response = await pool.handle_async_request(
                    httpcore.Request(
                        b"GET",
                        b"http://example.com/",
                        headers=[(b"host", b"example.com")],
                    )
                )
                with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                    async for _ in response.aiter_stream():
                        pass
                assert isinstance(exc_info.value.__cause__, h11.RemoteProtocolError)
                await response.aclose()

            async def waiting_request() -> None:
                nonlocal waiter_error
                try:
                    response = await pool.request("GET", "http://example.com/")
                    assert response.content == b"ok"
                except BaseException as exc:
                    waiter_error = exc

            task_group.start_soon(bad_request)
            await concurrency.wait_all_tasks_blocked()
            task_group.start_soon(waiting_request)
            await concurrency.wait_all_tasks_blocked()
            assert len(pool.connections) == 1
            bad_stream.read_gate.set()

        assert waiter_error is None
        assert bad_stream.close_count == 1
        assert healthy_stream.close_count == 0
        assert sum(connection.is_idle() for connection in pool.connections) == 1

    assert healthy_stream.close_count == 1


@pytest.mark.anyio
async def test_http11_close_error_does_not_mask_protocol_error():
    bad_stream = CloseFailingStream(list(SHORT_BODY))
    healthy_stream = FragmentedStream(list(HEALTHY_BODY))
    backend = SequenceBackend([bad_stream, healthy_stream])
    waiter_error: BaseException | None = None

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=backend
    ) as pool:
        async with concurrency.create_task_group() as task_group:

            async def bad_request() -> None:
                response = await pool.handle_async_request(
                    httpcore.Request(
                        b"GET",
                        b"http://example.com/",
                        headers=[(b"host", b"example.com")],
                    )
                )
                with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                    async for _ in response.aiter_stream():
                        pass
                assert isinstance(exc_info.value.__cause__, h11.RemoteProtocolError)
                await response.aclose()

            async def waiting_request() -> None:
                nonlocal waiter_error
                try:
                    response = await pool.request("GET", "http://example.com/")
                    assert response.content == b"ok"
                except BaseException as exc:
                    waiter_error = exc

            task_group.start_soon(bad_request)
            await concurrency.wait_all_tasks_blocked()
            task_group.start_soon(waiting_request)
            await concurrency.wait_all_tasks_blocked()
            bad_stream.read_gate.set()

        assert waiter_error is None
        assert bad_stream.close_count == 1
        assert healthy_stream.close_count == 0
        assert sum(connection.is_idle() for connection in pool.connections) == 1

    assert healthy_stream.close_count == 1


@pytest.mark.anyio
async def test_http11_close_race_is_idempotent_while_reading():
    read_gate = concurrency.Event()
    stream = FragmentedStream(list(SHORT_BODY), read_gate=read_gate)
    origin = httpcore.Origin(b"http", b"example.com", 80)
    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.handle_async_request(
            httpcore.Request(
                b"GET", b"http://example.com/", headers=[(b"host", b"example.com")]
            )
        )

        async def read_body() -> None:
            try:
                async for _ in response.aiter_stream():
                    pass
            except httpcore.ReadError:
                pass

        async with concurrency.create_task_group() as task_group:
            task_group.start_soon(read_body)
            await concurrency.wait_all_tasks_blocked()
            await response.aclose()
            await response.aclose()
            read_gate.set()

        assert stream.close_count == 1
        assert conn.is_closed()


@pytest.mark.anyio
async def test_http2_stream_reset_keeps_connection_idle():
    frames = [
        hyperframe.frame.SettingsFrame().serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=1,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS"],
        ).serialize(),
        hyperframe.frame.RstStreamFrame(stream_id=1, error_code=8).serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=3,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS"],
        ).serialize(),
        hyperframe.frame.DataFrame(
            stream_id=3, data=b"ok", flags=["END_STREAM"]
        ).serialize(),
    ]
    stream = HTTP2FragmentedStream(frames)
    backend = HTTP2FragmentedBackend([stream])

    async with httpcore.AsyncConnectionPool(
        max_connections=1, http2=True, http1=False, network_backend=backend
    ) as pool:
        with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
            await pool.request("GET", "http://example.com/")
        assert isinstance(exc_info.value.args[0], h2.events.StreamReset)

        response = await pool.request("GET", "http://example.com/")
        assert response.content == b"ok"

        connection = pool.connections[0]._connection
        assert connection.is_idle()
        assert not connection.is_closed()
        assert stream.close_count == 0

    assert stream.close_count == 1


@pytest.mark.anyio
async def test_http2_eof_closes_connection_and_wakes_waiter():
    bad_frames = [
        hyperframe.frame.SettingsFrame().serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=1,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS"],
        ).serialize(),
        b"",
    ]
    healthy_frames = [
        hyperframe.frame.SettingsFrame().serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=1,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS"],
        ).serialize(),
        hyperframe.frame.DataFrame(
            stream_id=1, data=b"ok", flags=["END_STREAM"]
        ).serialize(),
    ]
    bad_stream = HTTP2FragmentedStream(bad_frames)
    healthy_stream = HTTP2FragmentedStream(healthy_frames)
    backend = HTTP2FragmentedBackend([bad_stream, healthy_stream])
    waiter_error: BaseException | None = None

    async with httpcore.AsyncConnectionPool(
        max_connections=1, http2=True, http1=False, network_backend=backend
    ) as pool:
        async with concurrency.create_task_group() as task_group:

            async def bad_request() -> None:
                response = await pool.handle_async_request(
                    httpcore.Request(
                        b"GET",
                        b"http://example.com/",
                        headers=[(b"host", b"example.com")],
                    )
                )
                with pytest.raises(httpcore.RemoteProtocolError) as exc_info:
                    async for _ in response.aiter_stream():
                        pass
                assert str(exc_info.value) == "Server disconnected"
                await response.aclose()

            async def waiting_request() -> None:
                nonlocal waiter_error
                try:
                    response = await pool.request("GET", "http://example.com/")
                    assert response.content == b"ok"
                except BaseException as exc:
                    waiter_error = exc

            task_group.start_soon(bad_request)
            await concurrency.wait_all_tasks_blocked()
            task_group.start_soon(waiting_request)
            await concurrency.wait_all_tasks_blocked()
            bad_stream.read_gate.set()

        assert waiter_error is None
        assert bad_stream.close_count == 1
        assert healthy_stream.close_count == 0
        assert sum(connection.is_idle() for connection in pool.connections) == 1

    assert healthy_stream.close_count == 1
