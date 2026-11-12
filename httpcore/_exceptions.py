import contextlib
import typing

ExceptionMapping = typing.Mapping[typing.Type[Exception], typing.Type[Exception]]


@contextlib.contextmanager
def map_exceptions(map: ExceptionMapping) -> typing.Iterator[None]:
    try:
        yield
    except Exception as exc:  # noqa: PIE786
        for from_exc, to_exc in map.items():
            if isinstance(exc, from_exc):
                raise to_exc(exc) from exc
        raise  # pragma: nocover


class ConnectionNotAvailable(Exception):
    pass


class ProxyError(Exception):
    pass


class UnsupportedProtocol(Exception):
    pass


class ProtocolError(Exception):
    pass


class RemoteProtocolError(ProtocolError):
    pass


class LocalProtocolError(ProtocolError):
    pass


class RequestAbortedError(ProtocolError):
    """
    Raised when the server stops accepting a request body before it has been
    fully sent, for example by responding early (an ``END_STREAM`` on the
    response) while the upload is still blocked on HTTP/2 flow control.

    Unlike a connection failure, this only terminates the individual request
    stream, so the underlying connection may still be reused.
    """

    pass


# Timeout errors


class TimeoutException(Exception):
    pass


class PoolTimeout(TimeoutException):
    pass


class ConnectTimeout(TimeoutException):
    pass


class ReadTimeout(TimeoutException):
    pass


class WriteTimeout(TimeoutException):
    pass


# Network errors


class NetworkError(Exception):
    pass


class ConnectError(NetworkError):
    pass


class ReadError(NetworkError):
    pass


class WriteError(NetworkError):
    pass
