"""
A controlled, in-memory HTTP/2 peer used by the request-upload regression
tests.

The peer runs a real ``h2`` server state machine behind httpcore's network
stream interfaces, so no sockets, TLS, wall-clock timeouts or external
processes are involved. Backpressure is produced deterministically by the
server withholding WINDOW_UPDATE frames.

The same server policy implementation backs both the synchronous stream
(threading based) and the asynchronous stream (anyio memory object stream,
which works identically on asyncio and trio).
"""
from __future__ import annotations

import threading
import typing

import anyio
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings

import httpcore
from httpcore._backends.base import AsyncNetworkStream, NetworkStream


class EventLog:
    """Append-only, thread-safe log of labelled events with monotonic ids."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[int, str, str]] = []
        self._seq = 0

    def add(self, who: str, name: str, detail: typing.Any = None) -> None:
        with self._lock:
            self._seq += 1
            text = name if detail is None else f"{name}:{detail}"
            self.events.append((self._seq, who, text))

    def labels(self) -> list[str]:
        with self._lock:
            return [f"{who}:{name}" for _, who, name in self.events]

    def order_of(self, label: str) -> int | None:
        with self._lock:
            for seq, who, name in self.events:
                logged = f"{who}:{name}"
                if logged == label or logged.startswith(label + ":"):
                    return seq
        return None

    def contains(self, label: str) -> bool:
        return self.order_of(label) is not None


class ScriptedH2Server:
    """
    An in-memory HTTP/2 server driven by ``h2``.

    policy:
      "drain"
        Consume the whole body, opening the window as needed, then reply 200.
      "reject"
        Reply ``status`` (default 401) with END_STREAM on request HEADERS and
        never open the request window further (models an auth/size rejection).
      "reject_then_window"
        Reply 401 END_STREAM immediately, but keep granting WINDOW_UPDATE. Used
        to prove the client still aborts the upload even when flow is available.
      "rst"
        Reset the stream (RST_STREAM) after the first request DATA frame.
      "goaway"
        Reply 413 END_STREAM then send GOAWAY, tearing the connection down.
      "stall"
        Never reply and never open the window: pure server-side backpressure.
    """

    def __init__(
        self,
        log: EventLog,
        policy: str,
        *,
        initial_window: int = 256 * 1024,
        status: int = 401,
        rst_error: int = h2.exceptions.ErrorCodes.CANCEL,
    ) -> None:
        self.log = log
        self.policy = policy
        self.status = status
        self.rst_error = rst_error
        self.config = h2.config.H2Configuration(
            client_side=False, validate_inbound_headers=False
        )
        self.conn = h2.connection.H2Connection(config=self.config)
        self.conn.local_settings = h2.settings.Settings(
            client=False,
            initial_values={
                h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: initial_window,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100,
            },
        )
        self._handled: set[int] = set()
        self._reset: set[int] = set()
        self.bytes_received: dict[int, int] = {}

    def initial_bytes(self) -> bytes:
        self.conn.initiate_connection()
        self.log.add("server", "settings")
        return self.conn.data_to_send()

    def feed(self, data: bytes) -> bytes:
        if not data:
            return b""
        try:
            events = self.conn.receive_data(data)
        except h2.exceptions.ProtocolError as exc:
            self.log.add("server", "protocol-error", str(exc))
            return self.conn.data_to_send()
        out = bytearray()
        for event in events:
            out += self._handle(event)
        out += self.conn.data_to_send()
        return bytes(out)

    def _handle(self, event: h2.events.Event) -> bytes:
        out = bytearray()
        if isinstance(event, h2.events.RequestReceived):
            sid = event.stream_id
            self.log.add("server", "request-headers", f"stream={sid}")
            if self.policy in ("reject", "reject_then_window"):
                self._reject(sid, self.status)
            elif self.policy == "goaway":
                self._reject(sid, 413)
                self.conn.close_connection()
                self.log.add("server", "goaway")
            elif self.policy == "stall":
                self.log.add("server", "stall", f"stream={sid}")
            self._handled.add(sid)
        elif isinstance(event, h2.events.DataReceived):
            sid = event.stream_id
            n = event.flow_controlled_length or 0
            self.bytes_received[sid] = self.bytes_received.get(sid, 0) + n
            self.log.add("server", "request-data", f"stream={sid} bytes={n}")
            self.conn.acknowledge_received_data(n, sid)
            if self.policy == "rst" and sid not in self._reset:
                self._reset.add(sid)
                self.conn.reset_stream(sid, self.rst_error)
                self.log.add("server", "rst", f"stream={sid}")
            elif self.policy in ("drain", "reject_then_window") and n > 0:
                self.conn.increment_flow_control_window(n, sid)
        elif isinstance(event, h2.events.StreamEnded):
            sid = event.stream_id
            self.log.add("server", "request-end", f"stream={sid}")
            if self.policy == "drain":
                self.conn.send_headers(sid, [(b":status", b"200")])
                self.conn.send_data(sid, b"ok", end_stream=True)
                self.log.add("server", "response-200", f"stream={sid}")
        elif isinstance(event, h2.events.StreamReset):
            self.log.add("server", "client-rst", f"stream={event.stream_id}")
        out += self.conn.data_to_send()
        return out

    def _reject(self, sid: int, status: int) -> None:
        self.conn.send_headers(
            sid, [(b":status", str(status).encode())], end_stream=True
        )
        self.log.add("server", f"response-{status}", f"stream={sid}")


class _SyncQueue:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._chunks: list[bytes] = []
        self._closed = False

    def put(self, data: bytes) -> None:
        with self._cond:
            self._chunks.append(data)
            self._cond.notify_all()

    def get(self) -> bytes:
        with self._cond:
            while not self._chunks and not self._closed:
                self._cond.wait(timeout=1.0)
            if not self._chunks:
                return b""
            return self._chunks.pop(0)

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class SyncMemoryStream(NetworkStream):
    """Client-facing sync stream bridged to the scripted in-memory server."""

    def __init__(self, server: ScriptedH2Server, log: EventLog) -> None:
        self._server = server
        self.log = log
        self._inbox = _SyncQueue()
        self._inbox.put(server.initial_bytes())

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._inbox.get()

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if buffer:
            reply = self._server.feed(buffer)
            if reply:
                self._inbox.put(reply)

    def close(self) -> None:
        self._inbox.close()

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return httpcore._backends.mock.MockSSLObject(http2=True)
        return None


class AsyncMemoryStream(AsyncNetworkStream):
    """Client-facing async stream bridged to the scripted in-memory server."""

    def __init__(self, server: ScriptedH2Server, log: EventLog) -> None:
        self._server = server
        self.log = log
        self._sender, self._receiver = anyio.create_memory_object_stream[bytes](
            max_buffer_size=1000
        )
        self._sender.send_nowait(server.initial_bytes())

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        try:
            return await self._receiver.receive()
        except anyio.EndOfStream:
            return b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return
        reply = self._server.feed(buffer)
        if reply:
            await self._sender.send(reply)

    async def aclose(self) -> None:
        await self._sender.aclose()
        await self._receiver.aclose()

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return httpcore._backends.mock.MockSSLObject(http2=True)
        return None

    async def inject(self, frames: list[bytes]) -> None:
        """Inject raw server frames, e.g. a second request's response."""
        await self._sender.send(b"".join(frames))


def make_sync(policy: str, **kwargs: typing.Any):
    log = EventLog()
    server = ScriptedH2Server(log, policy, **kwargs)
    stream = SyncMemoryStream(server, log)
    origin = httpcore.Origin(b"https", b"example.com", 443)
    conn = httpcore.HTTP2Connection(origin=origin, stream=stream)
    return conn, server, log, stream


async def make_async(policy: str, **kwargs: typing.Any):
    log = EventLog()
    server = ScriptedH2Server(log, policy, **kwargs)
    stream = AsyncMemoryStream(server, log)
    origin = httpcore.Origin(b"https", b"example.com", 443)
    conn = httpcore.AsyncHTTP2Connection(origin=origin, stream=stream)
    return conn, server, log, stream
