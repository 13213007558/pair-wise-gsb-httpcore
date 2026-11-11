"""
Security audit: resource handling when a ``trace`` extension callback fails.

A failing trace callback must be allowed to fail the current request (its
exception has to reach the caller), but it must never be able to strand the
resources involved in that request:

* an open network stream/socket from the connection pool,
* an HTTP/1.1 connection slot (the pool's ``max_connections`` capacity),
* an HTTP/2 stream's concurrency quota (the ``max_concurrent_streams`` slot).

The tests below inject failures at the connection, response-header and
response-close stages (and, for the async interface, cancellation of the
callback). They run requests through a deliberately small pool and assert that
a *normal* request served after a failed one does not deadlock or time out
waiting for a leaked resource.
"""

from __future__ import annotations

import typing

import hpack
import hyperframe.frame
import pytest
from tests import concurrency

import httpcore
from httpcore._backends.base import NetworkBackend, NetworkStream
from httpcore._backends.mock import MockStream


# Cancellation has no threaded equivalent. Under unasync this becomes
# `False`, so the generated synchronous tests skip instead of touching anyio.
_ASYNC_ONLY = False
async_only = pytest.mark.skipif(not _ASYNC_ONLY, reason="async-only cancellation")


# A short pool timeout used as a deadlock canary: a healthy follow-up request
# is served immediately by the mock backend, so reaching this timeout proves a
# resource (pool slot or H2 quota) was leaked rather than merely slow.
POOL_TIMEOUT = 2.0


class AuditError(RuntimeError):
    """Raised by the injected trace callback."""


class TrackingMockStream(MockStream):
    """A mock stream that records when its socket gets closed."""

    def __init__(
        self,
        buffer: list[bytes],
        registry: dict[str, typing.Any],
        http2: bool = False,
    ) -> None:
        super().__init__(buffer, http2=http2)
        self.registry = registry
        self.registry["streams"].append(self)
        self.closed_by: str | None = None

    def close(self) -> None:
        if not self._closed:
            self.closed_by = "close"
            self.registry["closed"].append(self)
        super().close()


class TrackingMockBackend(NetworkBackend):
    """A mock backend handing out tracked streams."""

    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        self._buffer = buffer
        self._http2 = http2
        self.registry: dict[str, typing.Any] = {"streams": [], "closed": []}

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> NetworkStream:
        return TrackingMockStream(
            list(self._buffer), self.registry, http2=self._http2
        )

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> NetworkStream:  # pragma: nocover
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:  # pragma: nocover
        pass


HTTP11_RESPONSE = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


def h11_buffer(responses: int) -> list[bytes]:
    buffer: list[bytes] = []
    for _ in range(responses):
        buffer.extend(HTTP11_RESPONSE)
    return buffer


def h2_buffer(responses: int) -> list[bytes]:
    buffer: list[bytes] = [hyperframe.frame.SettingsFrame().serialize()]
    encoder = hpack.Encoder()
    for stream_id in range(1, 1 + responses * 2, 2):
        buffer.append(
            hyperframe.frame.HeadersFrame(
                stream_id=stream_id,
                data=encoder.encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize()
        )
        buffer.append(
            hyperframe.frame.DataFrame(
                stream_id=stream_id,
                data=b"Hello, world!",
                flags=["END_STREAM"],
            ).serialize()
        )
    return buffer


def failing_trace(failed_event: str) -> typing.Callable[..., typing.Any]:
    def trace(name: str, kwargs: dict[str, typing.Any]) -> None:
        if name == failed_event:
            raise AuditError(f"injected trace failure at {name!r}")

    return trace


def semaphore_value(semaphore: httpcore.Semaphore) -> int:
    """Read the current permit count for asyncio, trio or the threaded sync port."""
    backend = getattr(semaphore, "_backend", "")
    if backend == "trio":
        return semaphore._trio_semaphore.value
    if backend == "asyncio":
        return semaphore._anyio_semaphore.value
    # The synchronous port wraps a plain threading.Semaphore.
    return semaphore._semaphore._value


# ---------------------------------------------------------------------------
# HTTP/1.1: failure at the connection establishment stages.
# ---------------------------------------------------------------------------



@pytest.mark.parametrize(
    "failed_event",
    [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
    ],
)
def test_h11_connect_trace_failure_closes_socket_and_frees_pool(
    failed_event: str,
) -> None:
    """
    A trace failure while establishing the connection must surface to the
    caller, close the half-open socket, and leave the single pool slot free
    for the next (healthy) request.
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))

    with httpcore.ConnectionPool(
        network_backend=backend,
        max_connections=1,
        max_keepalive_connections=1,
    ) as pool:
        with pytest.raises(AuditError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"trace": failing_trace(failed_event)},
            )

        # A `.started` failure happens before the socket is opened; a
        # `.complete` failure happens afterwards. Either way every stream that
        # did get opened is closed by the connection layer rather than leaked.
        assert backend.registry["closed"] == list(backend.registry["streams"])
        for stream in backend.registry["streams"]:
            assert stream.closed_by == "close"

        # Nothing remains booked against the pool.
        assert pool._connections == []
        assert pool._requests == []

        # The follow-up request is served on a brand new stream without
        # blocking on a leaked pool slot. The pool timeout is only a canary.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200
        # Only the `connect_tcp.started` event fires before the TCP socket is
        # opened; every other connect-stage event has a stream to clean up.
        expected_streams = (
            1 if failed_event == "connection.connect_tcp.started" else 2
        )
        assert len(backend.registry["streams"]) == expected_streams

    # On pool shutdown the healthy stream is closed too; nothing is left open.
    assert {stream.closed_by for stream in backend.registry["streams"]} == {"close"}


# ---------------------------------------------------------------------------
# HTTP/1.1: failure after the response headers were read.
# ---------------------------------------------------------------------------



@pytest.mark.parametrize(
    "failed_event",
    [
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
    ],
)
def test_h11_request_trace_failure_releases_connection(
    failed_event: str,
) -> None:
    """
    A trace failure during the request/response-header exchange still runs the
    shielded ``response_closed`` cleanup, so the connection is closed and the
    pool slot released before the caller sees the exception.
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))

    with httpcore.ConnectionPool(
        network_backend=backend,
        max_connections=1,
        max_keepalive_connections=1,
    ) as pool:
        with pytest.raises(AuditError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"trace": failing_trace(failed_event)},
            )

        assert len(backend.registry["closed"]) == 1
        assert pool._connections == []
        assert pool._requests == []

        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200



def test_h11_body_trace_failure_releases_connection() -> None:
    """
    A trace failure while streaming the response body closes the response and
    returns the pool to a clean state.
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1
    ) as pool:
        with pytest.raises(AuditError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={
                    "trace": failing_trace(
                        "http11.receive_response_body.started"
                    ),
                    "timeout": {"pool": POOL_TIMEOUT},
                },
            )

        assert pool._connections == []
        assert pool._requests == []

        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200


# ---------------------------------------------------------------------------
# HTTP/1.1: failure inside the response-close callback itself.
# ---------------------------------------------------------------------------



def test_h11_response_closed_trace_failure_does_not_leak_slot() -> None:
    """
    The worst case is the ``response_closed`` trace failing while the
    connection cleanup is running. The exception still reaches the caller, but
    the connection must be force-closed so its pool slot cannot be held
    forever (which previously deadlocked a size-1 pool on the next request).
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1
    ) as pool:
        with pytest.raises(AuditError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={
                    "trace": failing_trace("http11.response_closed.started"),
                    "timeout": {"pool": POOL_TIMEOUT},
                },
            )

        # The connection is marked closed and its socket shut down...
        assert len(backend.registry["closed"]) == 1
        [connection] = pool._connections
        assert connection.is_closed()

        # ...and the next request does not time out waiting for the slot.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200

        # Reassignment drops the dead connection from the pool.
        assert all(not c.is_closed() for c in pool._connections)


# ---------------------------------------------------------------------------
# HTTP/2: failure at the per-stream stages must only release that stream.
# ---------------------------------------------------------------------------



@pytest.mark.parametrize(
    "failed_event",
    [
        "http2.send_request_headers.started",
        "http2.send_request_headers.complete",
        "http2.receive_response_headers.started",
        "http2.receive_response_headers.complete",
        "http2.response_closed.started",
    ],
)
def test_h2_stream_trace_failure_keeps_shared_connection(
    failed_event: str,
) -> None:
    """
    On a multiplexed HTTP/2 connection a failed stream must release its own
    concurrency quota but must *not* close the connection, which may still be
    serving other streams.
    """
    backend = TrackingMockBackend(h2_buffer(responses=4), http2=True)

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1, http2=True
    ) as pool:
        with pytest.raises(AuditError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"trace": failing_trace(failed_event)},
            )

        # Only one physical connection/stream exists, and it stays open.
        assert len(backend.registry["streams"]) == 1
        assert backend.registry["closed"] == []

        [pool_connection] = pool._connections
        h2_connection = pool_connection._connection
        assert h2_connection.is_available()
        assert not h2_connection.is_closed()

        # The failed stream returned its concurrency permit, and its event
        # bookkeeping was removed.
        semaphore = h2_connection._max_streams_semaphore
        assert semaphore_value(semaphore) == h2_connection._max_streams
        assert h2_connection._events == {}

        # A follow-up stream multiplexes onto the same open connection.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200
        assert len(backend.registry["streams"]) == 1
        assert h2_connection._request_count == 2

    # The single shared connection is closed once, by the pool shutdown.
    assert len(backend.registry["closed"]) == 1
    assert backend.registry["closed"][0].closed_by == "close"



def test_h2_concurrent_streams_one_failing_trace() -> None:
    """
    With several streams sharing one HTTP/2 connection, a trace failure on a
    single stream releases that stream's quota while the other streams keep
    running on the same open connection.
    """
    backend = TrackingMockBackend(h2_buffer(responses=8), http2=True)

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1, http2=True
    ) as pool:
        # Establish and warm the shared connection (settings exchange etc.).
        warm = pool.request("GET", "https://example.com/")
        assert warm.status == 200
        assert len(backend.registry["streams"]) == 1

        healthy_trace = failing_trace("http2.no_such_event")  # never raises
        outcomes: list[str] = []

        def healthy_request() -> None:
            response = pool.request(
                "GET",
                "https://example.com/",
                extensions={
                    "trace": healthy_trace,
                    "timeout": {"pool": POOL_TIMEOUT},
                },
            )
            outcomes.append(f"ok-{response.status}")

        def failing_request() -> None:
            with pytest.raises(AuditError):
                pool.request(
                    "GET",
                    "https://example.com/",
                    extensions={
                        "trace": failing_trace(
                            "http2.receive_response_headers.complete"
                        ),
                        "timeout": {"pool": POOL_TIMEOUT},
                    },
                )
            outcomes.append("failed-cleanly")

        with concurrency.open_nursery() as task_group:
            task_group.start_soon(healthy_request)
            task_group.start_soon(healthy_request)
            task_group.start_soon(failing_request)
            task_group.start_soon(healthy_request)

        # Every branch finished: the three healthy requests succeeded and the
        # failing one raised without stranding its quota.
        assert sorted(outcomes) == ["failed-cleanly", "ok-200", "ok-200", "ok-200"]

        [pool_connection] = pool._connections
        h2_connection = pool_connection._connection

        # The shared connection survived and is still reusable.
        assert not h2_connection.is_closed()
        assert h2_connection.is_available()
        assert h2_connection._events == {}

        # All concurrency permits were returned.
        semaphore = h2_connection._max_streams_semaphore
        assert semaphore_value(semaphore) == h2_connection._max_streams

        # One more request still multiplexes over the same connection.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200
        assert len(backend.registry["streams"]) == 1

    # Exactly one physical connection, closed only by the pool shutdown.
    assert len(backend.registry["closed"]) == 1


# ---------------------------------------------------------------------------
# Cancellation: the async trace callback itself gets cancelled.
#
# Cancellation is an async-only concept; the threaded synchronous port has no
# equivalent, so these tests are not ported by unasync.
# ---------------------------------------------------------------------------


class CancellingTrace:  # pragma: no cover
    """
    A trace callback that requests cancellation of the surrounding scope on a
    chosen event. This models an async callback being cancelled while it runs.
    """

    def __init__(self, failed_event: str) -> None:
        self.failed_event = failed_event
        self.scope: anyio.CancelScope | None = None

    def __call__(
        self, name: str, kwargs: dict[str, typing.Any]
    ) -> None:
        if name == self.failed_event and self.scope is not None:
            self.scope.cancel()



@async_only
def test_h11_cancelled_connect_trace_closes_socket() -> None:
    """
    Cancellation of the trace callback during connection establishment must
    still close the opened socket and free the pool slot.
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))
    trace = CancellingTrace("connection.connect_tcp.complete")

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1
    ) as pool:
        with anyio.move_on_after(2) as cancel_scope:
            trace.scope = cancel_scope
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"trace": trace},
            )
        assert cancel_scope.cancel_called

        # The half-open socket is closed by the connection's cancellation
        # safety net, and no request remains queued against the pool.
        assert len(backend.registry["closed"]) == 1
        assert pool._requests == []
        assert all(c.is_closed() for c in pool._connections)

        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200

    # Every stream that was opened is closed by the time the pool shuts down.
    assert backend.registry["closed"] == list(backend.registry["streams"])



@async_only
def test_h11_cancelled_response_close_trace_releases_connection() -> None:
    """
    Cancellation delivered while the response-close trace runs must not leave
    the connection stuck in ACTIVE and blocking the size-1 pool. The cleanup
    runs inside a shield, so the connection is either returned to IDLE or
    force-closed; in both cases the next request is served without a timeout.
    """
    backend = TrackingMockBackend(h11_buffer(responses=3))
    trace = CancellingTrace("http11.response_closed.started")

    with httpcore.ConnectionPool(
        network_backend=backend, max_connections=1
    ) as pool:
        with anyio.move_on_after(2) as cancel_scope:
            trace.scope = cancel_scope
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"trace": trace},
            )
        assert cancel_scope.cancel_called

        # No request is left queued, and nothing remains in the ACTIVE state.
        assert pool._requests == []
        assert all(
            c.is_closed() or c.is_idle() for c in pool._connections
        )

        # The decisive contract: the size-1 pool still serves the next request.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"timeout": {"pool": POOL_TIMEOUT}},
        )
        assert response.status == 200

    assert backend.registry["closed"] == list(backend.registry["streams"])
