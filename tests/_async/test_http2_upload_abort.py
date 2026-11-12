"""
Async regression tests for request-body uploads over HTTP/2.

Mirrors ``tests/_sync/test_http2_upload_abort.py`` and runs on both the
asyncio and trio backends via anyio. The in-memory h2 peer produces
backpressure, early rejections and resets deterministically, so passing does
not depend on any wall-clock timeout being shortened.
"""
from contextlib import suppress

import anyio
import hpack
import hyperframe.frame
import pytest

import httpcore

from tests.h2_peer import make_async

TOTAL = 1_000_000
HOST = (b"host", b"example.com")


class TrackedAsyncBody:
    """Async iterable upload that records consumption and finalization."""

    def __init__(self, log: "object", chunk_size: int = 8192) -> None:
        self.log = log
        self.chunk_size = chunk_size
        self.sent = 0
        self.exhausted = False
        self.closed = False

    def __aiter__(self) -> "TrackedAsyncBody":
        return self

    async def __anext__(self) -> bytes:
        if self.sent >= TOTAL:
            self.exhausted = True
            self.log.add("body", "exhausted")
            raise StopAsyncIteration
        n = min(self.chunk_size, TOTAL - self.sent)
        self.sent += n
        return b"x" * n

    async def aclose(self) -> None:
        self.closed = True
        self.log.add("body", "closed", f"at={self.sent}")


def _request(body: TrackedAsyncBody) -> httpcore.Request:
    return httpcore.Request(
        b"POST",
        "https://example.com/archive",
        headers=[HOST, (b"content-length", str(TOTAL).encode())],
        content=body,
    )


@pytest.mark.anyio
async def test_slow_upload_completes_under_backpressure():
    conn, server, log, stream = await make_async("drain")
    try:
        body = TrackedAsyncBody(log)
        with anyio.fail_after(5):
            response = await conn.handle_async_request(_request(body))
        assert response.status == 200
        assert await response.aread() == b"ok"

        assert body.exhausted is True
        assert body.closed is False
        assert server.bytes_received[1] == TOTAL
        assert log.order_of("server:request-end") is not None
        assert log.order_of("server:response-200") is not None
    finally:
        await conn.aclose()


@pytest.mark.anyio
async def test_early_rejection_without_window_aborts_upload():
    conn, server, log, stream = await make_async("reject")
    try:
        body = TrackedAsyncBody(log)
        with pytest.raises(httpcore.RequestAbortedError):
            with anyio.fail_after(5):
                await conn.handle_async_request(_request(body))

        assert body.sent < TOTAL
        assert body.exhausted is False
        assert body.closed is True
        assert log.contains("server:client-rst")
        assert log.order_of("server:response-401") < log.order_of("body:closed")
        assert conn.is_available()
    finally:
        await conn.aclose()


@pytest.mark.anyio
async def test_early_rejection_with_window_still_aborts_upload():
    conn, server, log, stream = await make_async("reject_then_window")
    try:
        body = TrackedAsyncBody(log)
        with pytest.raises(httpcore.RequestAbortedError):
            with anyio.fail_after(5):
                await conn.handle_async_request(_request(body))

        assert body.sent < TOTAL
        assert body.closed is True
        assert conn.is_available()
    finally:
        await conn.aclose()


@pytest.mark.anyio
async def test_rst_stream_mid_upload_raises_remote_error_and_reuses_connection():
    conn, server, log, stream = await make_async("rst")
    try:
        body = TrackedAsyncBody(log)
        with pytest.raises(httpcore.RemoteProtocolError):
            with anyio.fail_after(5):
                await conn.handle_async_request(_request(body))

        assert body.sent < TOTAL
        assert body.closed is True
        assert log.contains("server:rst")
        assert conn.is_available()

        headers = hyperframe.frame.HeadersFrame(
            stream_id=3,
            data=hpack.Encoder().encode(
                [(b":status", b"200"), (b"content-type", b"text/plain")]
            ),
            flags=["END_HEADERS"],
        ).serialize()
        data = hyperframe.frame.DataFrame(
            stream_id=3, data=b"second-ok", flags=["END_STREAM"]
        ).serialize()
        await stream.inject([headers, data])

        with anyio.fail_after(5):
            response = await conn.handle_async_request(
                httpcore.Request(
                    b"GET", "https://example.com/other", headers=[HOST]
                )
            )
        assert response.status == 200
        assert await response.aread() == b"second-ok"
        assert conn.is_available()
    finally:
        await conn.aclose()


@pytest.mark.anyio
async def test_goaway_raises_remote_protocol_error_and_closes_connection():
    conn, server, log, stream = await make_async("goaway")
    try:
        body = TrackedAsyncBody(log)
        with pytest.raises(httpcore.RemoteProtocolError):
            with anyio.fail_after(5):
                await conn.handle_async_request(_request(body))

        assert body.closed is True
        assert log.contains("server:goaway")
        assert not conn.is_available()
    finally:
        # GOAWAY closes the h2 state machine; the stream close may already be done.
        with suppress(Exception):
            await conn.aclose()


@pytest.mark.anyio
async def test_pure_backpressure_does_not_falsely_abort():
    conn, server, log, stream = await make_async("stall")
    try:
        body = TrackedAsyncBody(log)
        with pytest.raises(TimeoutError):
            with anyio.fail_after(1):
                await conn.handle_async_request(_request(body))

        # Timed out while waiting for flow control: nothing was aborted.
        assert body.closed is False
        assert not log.contains("server:rst")
        assert not any(
            label.startswith("server:response-") for label in log.labels()
        )
    finally:
        await conn.aclose()
