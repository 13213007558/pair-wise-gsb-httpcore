from __future__ import annotations

import ssl
import typing

from .._exceptions import ReadError
from .base import (
    SOCKET_OPTION,
    AsyncNetworkBackend,
    AsyncNetworkStream,
    NetworkBackend,
    NetworkStream,
)


class MockSSLObject:
    def __init__(self, http2: bool):
        self._http2 = http2

    def selected_alpn_protocol(self) -> str:
        return "h2" if self._http2 else "http/1.1"


class MockStream(NetworkStream):
    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        self._buffer = buffer
        self._http2 = http2
        self._closed = False
        self._writes: list[bytes] = []
        self._start_tls_count = 0

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._closed:
            raise ReadError("Connection closed")
        if not self._buffer:
            return b""
        return self._buffer.pop(0)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._writes.append(buffer)

    def close(self) -> None:
        self._closed = True

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> NetworkStream:
        self._start_tls_count += 1
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return MockSSLObject(http2=self._http2) if info == "ssl_object" else None

    @property
    def writes(self) -> list[bytes]:
        return self._writes

    @property
    def start_tls_count(self) -> int:
        return self._start_tls_count

    def is_closed(self) -> bool:
        return self._closed

    def __repr__(self) -> str:
        return "<httpcore.MockStream>"


class MockBackend(NetworkBackend):
    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        self._buffer = buffer
        self._http2 = http2
        self._streams: list[MockStream] = []

    @property
    def streams(self) -> list[MockStream]:
        return self._streams

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> NetworkStream:
        stream = MockStream(list(self._buffer), http2=self._http2)
        self._streams.append(stream)
        return stream

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> NetworkStream:
        stream = MockStream(list(self._buffer), http2=self._http2)
        self._streams.append(stream)
        return stream

    def sleep(self, seconds: float) -> None:
        pass


class AsyncMockStream(AsyncNetworkStream):
    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        self._buffer = buffer
        self._http2 = http2
        self._closed = False
        self._writes: list[bytes] = []
        self._start_tls_count = 0

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._closed:
            raise ReadError("Connection closed")
        if not self._buffer:
            return b""
        return self._buffer.pop(0)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._writes.append(buffer)

    async def aclose(self) -> None:
        self._closed = True

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        self._start_tls_count += 1
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return MockSSLObject(http2=self._http2) if info == "ssl_object" else None

    @property
    def writes(self) -> list[bytes]:
        return self._writes

    @property
    def start_tls_count(self) -> int:
        return self._start_tls_count

    def is_closed(self) -> bool:
        return self._closed

    def __repr__(self) -> str:
        return "<httpcore.AsyncMockStream>"


class AsyncMockBackend(AsyncNetworkBackend):
    def __init__(self, buffer: list[bytes], http2: bool = False) -> None:
        self._buffer = buffer
        self._http2 = http2
        self._streams: list[AsyncMockStream] = []

    @property
    def streams(self) -> list[AsyncMockStream]:
        return self._streams

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        stream = AsyncMockStream(list(self._buffer), http2=self._http2)
        self._streams.append(stream)
        return stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        stream = AsyncMockStream(list(self._buffer), http2=self._http2)
        self._streams.append(stream)
        return stream

    async def sleep(self, seconds: float) -> None:
        pass
