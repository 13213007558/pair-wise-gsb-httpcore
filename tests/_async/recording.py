"""
Helpers for tests that need to inspect the exact bytes sent over the wire,
and the connection attempts made by the connection pool.
"""

import typing

import httpcore


class RecordingStream(httpcore.AsyncMockStream):
    """
    A mock network stream that records everything written to it,
    so that tests can inspect the exact bytes sent on the wire.
    """

    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        super().__init__(buffer, http2=http2)
        self.written = b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.written += buffer


class RecordingBackend(httpcore.AsyncMockBackend):
    """
    A mock network backend that records the host and port of each TCP
    connection, and hands out recording streams.
    """

    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        super().__init__(buffer, http2=http2)
        self.connections: list[tuple[str, int]] = []
        self.streams: list[RecordingStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> RecordingStream:
        self.connections.append((host, port))
        stream = RecordingStream(list(self._buffer), http2=self._http2)
        self.streams.append(stream)
        return stream
