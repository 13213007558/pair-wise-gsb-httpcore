from __future__ import annotations

import base64
import ipaddress
import ssl
import typing
import urllib.parse
from urllib.parse import urljoin

from ._exceptions import InvalidURL

# Functions for typechecking...


ByteOrStr = typing.Union[bytes, str]
HeadersAsSequence = typing.Sequence[typing.Tuple[ByteOrStr, ByteOrStr]]
HeadersAsMapping = typing.Mapping[ByteOrStr, ByteOrStr]
HeaderTypes = typing.Union[HeadersAsSequence, HeadersAsMapping, None]

Extensions = typing.MutableMapping[str, typing.Any]


def enforce_bytes(value: bytes | str, *, name: str) -> bytes:
    """
    Any arguments that are ultimately represented as bytes can be specified
    either as bytes or as strings.

    However we enforce that any string arguments must only contain characters in
    the plain ASCII range. chr(0)...chr(127). If you need to use characters
    outside that range then be precise, and use a byte-wise argument.
    """
    if isinstance(value, str):
        try:
            return value.encode("ascii")
        except UnicodeEncodeError:
            raise TypeError(f"{name} strings may not include unicode characters.")
    elif isinstance(value, bytes):
        return value

    seen_type = type(value).__name__
    raise TypeError(f"{name} must be bytes or str, but got {seen_type}.")


def enforce_url(value: URL | bytes | str, *, name: str) -> URL:
    """
    Type check for URL parameters.
    """
    if isinstance(value, (bytes, str)):
        return URL(value)
    elif isinstance(value, URL):
        return value

    seen_type = type(value).__name__
    raise TypeError(f"{name} must be a URL, bytes, or str, but got {seen_type}.")


def enforce_headers(
    value: HeadersAsMapping | HeadersAsSequence | None = None, *, name: str
) -> list[tuple[bytes, bytes]]:
    """
    Convienence function that ensure all items in request or response headers
    are either bytes or strings in the plain ASCII range.
    """
    if value is None:
        return []
    elif isinstance(value, typing.Mapping):
        return [
            (
                enforce_bytes(k, name="header name"),
                enforce_bytes(v, name="header value"),
            )
            for k, v in value.items()
        ]
    elif isinstance(value, typing.Sequence):
        return [
            (
                enforce_bytes(k, name="header name"),
                enforce_bytes(v, name="header value"),
            )
            for k, v in value
        ]

    seen_type = type(value).__name__
    raise TypeError(
        f"{name} must be a mapping or sequence of two-tuples, but got {seen_type}."
    )


def enforce_stream(
    value: bytes | typing.Iterable[bytes] | typing.AsyncIterable[bytes] | None,
    *,
    name: str,
) -> typing.Iterable[bytes] | typing.AsyncIterable[bytes]:
    if value is None:
        return ByteStream(b"")
    elif isinstance(value, bytes):
        return ByteStream(value)
    return value


# * https://tools.ietf.org/html/rfc3986#section-3.2.3
# * https://url.spec.whatwg.org/#url-miscellaneous
# * https://url.spec.whatwg.org/#scheme-state
DEFAULT_PORTS = {
    b"ftp": 21,
    b"http": 80,
    b"https": 443,
    b"ws": 80,
    b"wss": 443,
}

ZONE_ID_CHARACTERS = set(
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
)
HEX_DIGITS = set(b"0123456789abcdefABCDEF")


def _is_ipv6_host(host: bytes) -> bool:
    return b":" in host


def _is_valid_pct_encoded(value: bytes, index: int) -> bool:
    return (
        index + 2 < len(value)
        and value[index + 1] in HEX_DIGITS
        and value[index + 2] in HEX_DIGITS
    )


def _normalize_ipv6_host(host: bytes) -> bytes:
    address, separator, zone = host.partition(b"%")
    try:
        normalized_address = str(ipaddress.IPv6Address(address.decode("ascii"))).encode(
            "ascii"
        )
    except (UnicodeDecodeError, ipaddress.AddressValueError) as exc:
        raise InvalidURL(f"Invalid IPv6 address: {host!r}") from exc

    if not separator:
        return normalized_address

    if not zone.startswith(b"25"):
        raise InvalidURL("IPv6 zone identifiers must use the '%25' delimiter.")
    zone = zone[2:]
    if not zone:
        raise InvalidURL(f"Invalid IPv6 zone identifier: {host!r}")

    index = 0
    while index < len(zone):
        char = zone[index]
        if char == ord(b"%"):
            if not _is_valid_pct_encoded(zone, index):
                raise InvalidURL(f"Invalid IPv6 zone identifier: {host!r}")
            index += 3
        else:
            if char not in ZONE_ID_CHARACTERS:
                raise InvalidURL(f"Invalid IPv6 zone identifier: {host!r}")
            index += 1

    return normalized_address + b"%25" + zone


def _validate_host(host: bytes) -> bytes:
    if not host:
        return host

    if host.startswith(b"[") or host.endswith(b"]"):
        if not (host.startswith(b"[") and host.endswith(b"]")):
            raise InvalidURL(f"Invalid IPv6 address: {host!r}")
        host = host[1:-1]

    if _is_ipv6_host(host):
        return _normalize_ipv6_host(host)
    return host


def _validate_port(port: int | None) -> int | None:
    if port is not None and (
        not isinstance(port, int) or isinstance(port, bool) or not (0 <= port <= 65535)
    ):
        raise InvalidURL(f"Invalid port: {port!r}")
    return port


def _host_authority(host: bytes, port: int | None = None) -> bytes:
    authority = b"[" + host + b"]" if _is_ipv6_host(host) else host
    if port is not None:
        authority += b":%d" % port
    return authority


def _socket_host(host: bytes) -> str:
    return host.replace(b"%25", b"%", 1).decode("ascii")


def _tls_hostname(host: bytes) -> str | None:
    hostname = _socket_host(host)
    if _is_ipv6_host(host):
        hostname = hostname.partition("%")[0]
    return hostname or None


def _parse_url(value: bytes) -> tuple[bytes, bytes, int | None, bytes]:
    scheme, separator, remainder = value.partition(b"://")
    if not separator or b"[" not in remainder:
        try:
            parsed = urllib.parse.urlparse(value)
        except ValueError as exc:
            raise InvalidURL(str(exc)) from exc
        try:
            port = _validate_port(parsed.port)
        except ValueError as exc:
            raise InvalidURL(str(exc)) from exc
        target = (parsed.path or b"/") + (b"?" + parsed.query if parsed.query else b"")
        return parsed.scheme, _validate_host(parsed.hostname or b""), port, target

    authority_end_candidates = [
        position
        for position in (
            remainder.find(b"/"),
            remainder.find(b"?"),
            remainder.find(b"#"),
        )
        if position >= 0
    ]
    authority_end = min(authority_end_candidates, default=len(remainder))
    authority = remainder[:authority_end]
    request_target = remainder[authority_end:].partition(b"#")[0] or b"/"

    host_port = authority.rsplit(b"@", 1)[-1]
    if not host_port.startswith(b"["):
        raise InvalidURL(f"Invalid IPv6 address: {host_port!r}")
    closing_bracket = host_port.find(b"]")
    if closing_bracket < 0:
        raise InvalidURL(f"Invalid IPv6 address: {host_port!r}")
    host = _validate_host(host_port[1:closing_bracket])
    port_bytes = host_port[closing_bracket + 1 :]
    port = None
    if port_bytes:
        if not port_bytes.startswith(b":"):
            raise InvalidURL(f"Invalid port: {port_bytes[1:]!r}")
        try:
            port = _validate_port(int(port_bytes[1:]))
        except ValueError as exc:
            raise InvalidURL(f"Invalid port: {port_bytes[1:]!r}") from exc

    return scheme, host, port, request_target


def include_request_headers(
    headers: list[tuple[bytes, bytes]],
    *,
    url: "URL",
    content: None | bytes | typing.Iterable[bytes] | typing.AsyncIterable[bytes],
) -> list[tuple[bytes, bytes]]:
    headers_set = set(k.lower() for k, v in headers)

    if b"host" not in headers_set:
        default_port = DEFAULT_PORTS.get(url.scheme)
        port = None if url.port is None or url.port == default_port else url.port
        header_value = _host_authority(url.host, port)
        headers = [(b"Host", header_value)] + headers

    if (
        content is not None
        and b"content-length" not in headers_set
        and b"transfer-encoding" not in headers_set
    ):
        if isinstance(content, bytes):
            content_length = str(len(content)).encode("ascii")
            headers += [(b"Content-Length", content_length)]
        else:
            headers += [(b"Transfer-Encoding", b"chunked")]  # pragma: nocover

    return headers


# Interfaces for byte streams...


class ByteStream:
    """
    A container for non-streaming content, and that supports both sync and async
    stream iteration.
    """

    def __init__(self, content: bytes) -> None:
        self._content = content

    def __iter__(self) -> typing.Iterator[bytes]:
        yield self._content

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield self._content

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{len(self._content)} bytes]>"


class Origin:
    def __init__(self, scheme: bytes, host: bytes, port: int) -> None:
        self.scheme = enforce_bytes(scheme, name="scheme")
        self.host = _validate_host(enforce_bytes(host, name="host"))
        _validate_port(port)
        self.port = port

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, Origin)
            and self.scheme == other.scheme
            and self.host == other.host
            and self.port == other.port
        )

    def __str__(self) -> str:
        scheme = self.scheme.decode("ascii")
        host = _host_authority(self.host).decode("ascii")
        port = str(self.port)
        return f"{scheme}://{host}:{port}"

    def __hash__(self) -> int:
        return hash((self.scheme, self.host, self.port))

    @property
    def authority(self) -> bytes:
        return _host_authority(self.host, self.port)

    @property
    def socket_host(self) -> str:
        return _socket_host(self.host)

    @property
    def tls_hostname(self) -> str | None:
        return _tls_hostname(self.host)


class URL:
    """
    Represents the URL against which an HTTP request may be made.

    The URL may either be specified as a plain string, for convienence:

    ```python
    url = httpcore.URL("https://www.example.com/")
    ```

    Or be constructed with explicitily pre-parsed components:

    ```python
    url = httpcore.URL(scheme=b'https', host=b'www.example.com', port=None, target=b'/')
    ```

    Using this second more explicit style allows integrations that are using
    `httpcore` to pass through URLs that have already been parsed in order to use
    libraries such as `rfc-3986` rather than relying on the stdlib. It also ensures
    that URL parsing is treated identically at both the networking level and at any
    higher layers of abstraction.

    The four components are important here, as they allow the URL to be precisely
    specified in a pre-parsed format. They also allow certain types of request to
    be created that could not otherwise be expressed.

    For example, an HTTP request to `http://www.example.com/` forwarded via a proxy
    at `http://localhost:8080`...

    ```python
    # Constructs an HTTP request with a complete URL as the target:
    # GET https://www.example.com/ HTTP/1.1
    url = httpcore.URL(
        scheme=b'http',
        host=b'localhost',
        port=8080,
        target=b'https://www.example.com/'
    )
    request = httpcore.Request(
        method="GET",
        url=url
    )
    ```

    Another example is constructing an `OPTIONS *` request...

    ```python
    # Constructs an 'OPTIONS *' HTTP request:
    # OPTIONS * HTTP/1.1
    url = httpcore.URL(scheme=b'https', host=b'www.example.com', target=b'*')
    request = httpcore.Request(method="OPTIONS", url=url)
    ```

    This kind of request is not possible to formulate with a URL string,
    because the `/` delimiter is always used to demark the target from the
    host/port portion of the URL.

    For convenience, string-like arguments may be specified either as strings or
    as bytes. However, once a request is being issue over-the-wire, the URL
    components are always ultimately required to be a bytewise representation.

    In order to avoid any ambiguity over character encodings, when strings are used
    as arguments, they must be strictly limited to the ASCII range `chr(0)`-`chr(127)`.
    If you require a bytewise representation that is outside this range you must
    handle the character encoding directly, and pass a bytes instance.
    """

    def __init__(
        self,
        url: bytes | str = "",
        *,
        scheme: bytes | str = b"",
        host: bytes | str = b"",
        port: int | None = None,
        target: bytes | str = b"",
        raw_path: bytes | str | None = None,
    ) -> None:
        """
        Parameters:
            url: The complete URL as a string or bytes.
            scheme: The URL scheme as a string or bytes.
                Typically either `"http"` or `"https"`.
            host: The URL host as a string or bytes. Such as `"www.example.com"`.
            port: The port to connect to. Either an integer or `None`.
            target: The target of the HTTP request. Such as `"/items?search=red"`.
            raw_path: Deprecated alias for the `target` parameter.
        """
        if url:
            self.scheme, self.host, self.port, self.target = _parse_url(
                enforce_bytes(url, name="url")
            )
        else:
            self.scheme = enforce_bytes(scheme, name="scheme")
            self.host = _validate_host(enforce_bytes(host, name="host"))
            self.port = _validate_port(port)
            self.target = enforce_bytes(
                raw_path if raw_path is not None else target, name="target"
            )

    @property
    def origin(self) -> Origin:
        default_port = {
            b"http": 80,
            b"https": 443,
            b"ws": 80,
            b"wss": 443,
            b"socks5": 1080,
            b"socks5h": 1080,
        }[self.scheme]
        return Origin(
            scheme=self.scheme, host=self.host, port=self.port or default_port
        )

    @property
    def raw_path(self) -> bytes:
        return self.target

    @property
    def authority(self) -> bytes:
        default_port = DEFAULT_PORTS.get(self.scheme)
        port = None if self.port is None or self.port == default_port else self.port
        return _host_authority(self.host, port)

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, URL)
            and other.scheme == self.scheme
            and other.host == self.host
            and other.port == self.port
            and other.target == self.target
        )

    def __bytes__(self) -> bytes:
        return (
            self.scheme + b"://" + _host_authority(self.host, self.port) + self.target
        )

    def __str__(self) -> str:
        return bytes(self).decode("ascii", errors="replace")

    def __hash__(self) -> int:
        return hash(bytes(self))

    def copy(
        self,
        *,
        scheme: bytes | str | None = None,
        host: bytes | str | None = None,
        port: int | None = -1,
        target: bytes | str | None = None,
        raw_path: bytes | str | None = None,
    ) -> "URL":
        if target is not None and raw_path is not None:
            raise TypeError("Cannot specify both 'target' and 'raw_path'.")
        copied_target = self.target if target is None else target
        if raw_path is not None:
            copied_target = raw_path
        return URL(
            scheme=self.scheme if scheme is None else scheme,
            host=self.host if host is None else host,
            port=self.port if port == -1 else port,
            target=copied_target,
        )

    def copy_with(self, **kwargs: typing.Any) -> "URL":
        return self.copy(**kwargs)

    def join(self, url: "URL | bytes | str") -> "URL":
        reference = (
            bytes(url) if isinstance(url, URL) else enforce_bytes(url, name="url")
        )
        return URL(urljoin(bytes(self), reference))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(scheme={self.scheme!r}, "
            f"host={self.host!r}, port={self.port!r}, target={self.target!r})"
        )


class Request:
    """
    An HTTP request.
    """

    def __init__(
        self,
        method: bytes | str,
        url: URL | bytes | str,
        *,
        headers: HeaderTypes = None,
        content: bytes
        | typing.Iterable[bytes]
        | typing.AsyncIterable[bytes]
        | None = None,
        extensions: Extensions | None = None,
    ) -> None:
        """
        Parameters:
            method: The HTTP request method, either as a string or bytes.
                For example: `GET`.
            url: The request URL, either as a `URL` instance, or as a string or bytes.
                For example: `"https://www.example.com".`
            headers: The HTTP request headers.
            content: The content of the request body.
            extensions: A dictionary of optional extra information included on
                the request. Possible keys include `"timeout"`, and `"trace"`.
        """
        self.method: bytes = enforce_bytes(method, name="method")
        self.url: URL = enforce_url(url, name="url")
        self.headers: list[tuple[bytes, bytes]] = enforce_headers(
            headers, name="headers"
        )
        self.stream: typing.Iterable[bytes] | typing.AsyncIterable[bytes] = (
            enforce_stream(content, name="content")
        )
        self.extensions = {} if extensions is None else extensions

        if "target" in self.extensions:
            self.url = URL(
                scheme=self.url.scheme,
                host=self.url.host,
                port=self.url.port,
                target=self.extensions["target"],
            )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{self.method!r}]>"


class Response:
    """
    An HTTP response.
    """

    def __init__(
        self,
        status: int,
        *,
        headers: HeaderTypes = None,
        content: bytes
        | typing.Iterable[bytes]
        | typing.AsyncIterable[bytes]
        | None = None,
        extensions: Extensions | None = None,
    ) -> None:
        """
        Parameters:
            status: The HTTP status code of the response. For example `200`.
            headers: The HTTP response headers.
            content: The content of the response body.
            extensions: A dictionary of optional extra information included on
                the responseself.Possible keys include `"http_version"`,
                `"reason_phrase"`, and `"network_stream"`.
        """
        self.status: int = status
        self.headers: list[tuple[bytes, bytes]] = enforce_headers(
            headers, name="headers"
        )
        self.stream: typing.Iterable[bytes] | typing.AsyncIterable[bytes] = (
            enforce_stream(content, name="content")
        )
        self.extensions = {} if extensions is None else extensions

        self._stream_consumed = False

    @property
    def content(self) -> bytes:
        if not hasattr(self, "_content"):
            if isinstance(self.stream, typing.Iterable):
                raise RuntimeError(
                    "Attempted to access 'response.content' on a streaming response. "
                    "Call 'response.read()' first."
                )
            else:
                raise RuntimeError(
                    "Attempted to access 'response.content' on a streaming response. "
                    "Call 'await response.aread()' first."
                )
        return self._content

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{self.status}]>"

    # Sync interface...

    def read(self) -> bytes:
        if not isinstance(self.stream, typing.Iterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to read an asynchronous response using 'response.read()'. "
                "You should use 'await response.aread()' instead."
            )
        if not hasattr(self, "_content"):
            self._content = b"".join([part for part in self.iter_stream()])
        return self._content

    def iter_stream(self) -> typing.Iterator[bytes]:
        if not isinstance(self.stream, typing.Iterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to stream an asynchronous response using 'for ... in "
                "response.iter_stream()'. "
                "You should use 'async for ... in response.aiter_stream()' instead."
            )
        if self._stream_consumed:
            raise RuntimeError(
                "Attempted to call 'for ... in response.iter_stream()' more than once."
            )
        self._stream_consumed = True
        for chunk in self.stream:
            yield chunk

    def close(self) -> None:
        if not isinstance(self.stream, typing.Iterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to close an asynchronous response using 'response.close()'. "
                "You should use 'await response.aclose()' instead."
            )
        if hasattr(self.stream, "close"):
            self.stream.close()

    # Async interface...

    async def aread(self) -> bytes:
        if not isinstance(self.stream, typing.AsyncIterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to read an synchronous response using "
                "'await response.aread()'. "
                "You should use 'response.read()' instead."
            )
        if not hasattr(self, "_content"):
            self._content = b"".join([part async for part in self.aiter_stream()])
        return self._content

    async def aiter_stream(self) -> typing.AsyncIterator[bytes]:
        if not isinstance(self.stream, typing.AsyncIterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to stream an synchronous response using 'async for ... in "
                "response.aiter_stream()'. "
                "You should use 'for ... in response.iter_stream()' instead."
            )
        if self._stream_consumed:
            raise RuntimeError(
                "Attempted to call 'async for ... in response.aiter_stream()' "
                "more than once."
            )
        self._stream_consumed = True
        async for chunk in self.stream:
            yield chunk

    async def aclose(self) -> None:
        if not isinstance(self.stream, typing.AsyncIterable):  # pragma: nocover
            raise RuntimeError(
                "Attempted to close a synchronous response using "
                "'await response.aclose()'. "
                "You should use 'response.close()' instead."
            )
        if hasattr(self.stream, "aclose"):
            await self.stream.aclose()


class Proxy:
    def __init__(
        self,
        url: URL | bytes | str,
        auth: tuple[bytes | str, bytes | str] | None = None,
        headers: HeadersAsMapping | HeadersAsSequence | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ):
        self.url = enforce_url(url, name="url")
        self.headers = enforce_headers(headers, name="headers")
        self.ssl_context = ssl_context

        if auth is not None:
            username = enforce_bytes(auth[0], name="auth")
            password = enforce_bytes(auth[1], name="auth")
            userpass = username + b":" + password
            authorization = b"Basic " + base64.b64encode(userpass)
            self.auth: tuple[bytes, bytes] | None = (username, password)
            self.headers = [(b"Proxy-Authorization", authorization)] + self.headers
        else:
            self.auth = None
