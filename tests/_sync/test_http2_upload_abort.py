"""
Regression tests for request-body uploads over HTTP/2.

A server is allowed to answer (401/413) or reset a stream after reading just
the request headers, without ever granting more flow-control window. The
client must not wait for the whole body to be sent before surfacing that
decision, must always finalize the upstream body generator, and must keep the
underlying connection usable for unrelated requests.

These scenarios are driven deterministically by an in-memory h2 peer
(see ``tests/h2_peer.py``); no wall-clock timeout is used to pass.
"""
import threading

import hpack
import hyperframe.frame
import pytest

import httpcore

from tests.h2_peer import make_sync

TOTAL = 1_000_000
HOST = (b"host", b"example.com")


class TrackedBody:
    """Sync iterable upload that records consumption and finalization."""

    def __init__(self, log: "object", chunk_size: int = 8192) -> None:
        self.log = log
        self.chunk_size = chunk_size
        self.sent = 0
        self.exhausted = False
        self.closed = False

    def __iter__(self) -> "TrackedBody":
        return self

    def __next__(self) -> bytes:
        if self.sent >= TOTAL:
            self.exhausted = True
            self.log.add("body", "exhausted")
            raise StopIteration
        n = min(self.chunk_size, TOTAL - self.sent)
        self.sent += n
        return b"x" * n

    def close(self) -> None:
        self.closed = True
        self.log.add("body", "closed", f"at={self.sent}")


def _request(body: TrackedBody) -> httpcore.Request:
    return httpcore.Request(
        b"POST",
        "https://example.com/archive",
        headers=[HOST, (b"content-length", str(TOTAL).encode())],
        content=body,
    )


def _run_with_watchdog(fn, *args):
    """Run a blocking call on a thread; fail the test if it hangs."""
    result: dict = {}

    def worker() -> None:
        try:
            result["value"] = fn(*args)
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), "upload call hung instead of returning"
    if "error" in result:
        raise result["error"]
    return result.get("value")


def test_slow_upload_completes_under_backpressure():
    """Server backpressure alone must still allow a full, successful upload."""
    conn, server, log, stream = make_sync("drain")
    body = TrackedBody(log)

    response = _run_with_watchdog(conn.handle_request, _request(body))
    assert response.status == 200
    assert response.read() == b"ok"

    assert body.exhausted is True
    assert body.closed is False  # fully consumed: nothing to cancel
    assert server.bytes_received[1] == TOTAL
    assert log.order_of("server:request-end") is not None
    assert log.order_of("server:response-200") is not None


def test_early_rejection_without_window_aborts_upload():
    """401 after headers + no window growth must surface without draining body."""
    conn, server, log, stream = make_sync("reject")
    body = TrackedBody(log)

    with pytest.raises(httpcore.RequestAbortedError):
        _run_with_watchdog(conn.handle_request, _request(body))

    # Only the initial window's worth of data was produced before aborting.
    assert body.sent < TOTAL
    assert body.exhausted is False
    assert body.closed is True
    # The client cancelled the abandoned stream.
    assert log.contains("server:client-rst")
    # Rejection (headers) was observed before the generator was closed.
    assert log.order_of("server:response-401") < log.order_of("body:closed")
    # The connection survives a stream-level abort.
    assert conn.is_available()


def test_early_rejection_with_window_still_aborts_upload():
    """Even if flow stays open, an early response ends the upload promptly."""
    conn, server, log, stream = make_sync("reject_then_window")
    body = TrackedBody(log)

    with pytest.raises(httpcore.RequestAbortedError):
        _run_with_watchdog(conn.handle_request, _request(body))

    assert body.sent < TOTAL
    assert body.closed is True
    assert conn.is_available()


def test_rst_stream_mid_upload_raises_remote_error_and_reuses_connection():
    conn, server, log, stream = make_sync("rst")
    body = TrackedBody(log)

    with pytest.raises(httpcore.RemoteProtocolError):
        _run_with_watchdog(conn.handle_request, _request(body))

    assert body.sent < TOTAL
    assert body.closed is True
    assert log.contains("server:rst")
    # Stream-level reset must not poison the whole connection.
    assert conn.is_available()

    # An unrelated request on the same connection still completes.
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
    stream._inbox.put(headers)
    stream._inbox.put(data)

    response = conn.handle_request(
        httpcore.Request(b"GET", "https://example.com/other", headers=[HOST])
    )
    assert response.status == 200
    assert response.read() == b"second-ok"
    assert conn.is_available()


def test_goaway_raises_remote_protocol_error_and_closes_connection():
    conn, server, log, stream = make_sync("goaway")
    body = TrackedBody(log)

    with pytest.raises(httpcore.RemoteProtocolError):
        _run_with_watchdog(conn.handle_request, _request(body))

    assert body.closed is True
    assert log.contains("server:goaway")
    assert not conn.is_available()


def test_pure_backpressure_does_not_falsely_abort():
    """
    A server that simply withholds the window (no response, no reset) blocks
    the upload but must not be misclassified as a rejection. The call stays
    parked in flow control: no RequestAbortedError/RemoteProtocolError is
    fabricated, and the generator is not closed. Whether the whole body has
    already been buffered into h2 does not matter -- what matters is that we
    are still waiting for a WINDOW_UPDATE rather than raising.
    """
    conn, server, log, stream = make_sync("stall")
    body = TrackedBody(log)

    result: dict = {}

    def worker() -> None:
        try:
            conn.handle_request(_request(body))
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=2)
    assert thread.is_alive(), "backpressure wait should continue while window is zero"
    assert "error" not in result
    # No rejection/reset was received, so nothing should have been aborted.
    assert body.closed is False
    assert not log.contains("server:rst")
    assert not any(
        label.startswith("server:response-") for label in log.labels()
    )
