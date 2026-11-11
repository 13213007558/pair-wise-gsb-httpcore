import pytest

import httpcore


# A complete, successful no-auth negotiation followed by an HTTP response.
NO_AUTH_HANDSHAKE = [
    # Method selection: VER=5, NO AUTHENTICATION REQUIRED
    b"\x05\x00",
    # CONNECT reply: VER=5, SUCCEEDED, RSV=0, IPv4 127.0.0.1:80
    b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
]

HTTP_RESPONSE = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


def _assert_negotiation_failed(network_backend: httpcore.MockBackend) -> None:
    """
    A failed negotiation must close the raw proxy connection without ever
    starting TLS against the target or sending any HTTP request bytes.
    """
    assert len(network_backend.streams) == 1
    stream = network_backend.streams[0]
    assert stream.is_closed()
    assert stream.start_tls_count == 0
    # Only SOCKS5 negotiation bytes may have been written, never HTTP.
    for chunk in stream.writes:
        assert chunk[0:1] == b"\x05" or chunk[0:1] == b"\x01"



def test_socks5_request():
    """
    Send an HTTP request via a SOCKS proxy.
    """
    network_backend = httpcore.MockBackend(
        [
            # The initial socks CONNECT
            #   v5 NOAUTH
            b"\x05\x00",
            #   v5 SUC RSV IP4 127  .0  .0  .1     :80
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<Socks5Connection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == [
            "<Socks5Connection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(
            httpcore.Origin(b"http", b"example.com", 80)
        )
        assert not proxy.connections[0].can_handle_request(
            httpcore.Origin(b"http", b"other.com", 80)
        )
        assert proxy.connections[0].can_handle_request(
            httpcore.Origin(b"https", b"example.com", 443)
        )
        assert not proxy.connections[0].can_handle_request(
            httpcore.Origin(b"https", b"other.com", 443)
        )



def test_authenticated_socks5_request():
    """
    Send an HTTP request via a SOCKS proxy.
    """
    network_backend = httpcore.MockBackend(
        [
            # The initial socks CONNECT
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
            #   v1 VALID USERNAME/PASSWORD
            b"\x01\x00",
            #   v5 SUC RSV IP4 127  .0  .0  .1     :80
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"username", b"password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<Socks5Connection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == [
            "<Socks5Connection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()



def test_socks5_request_connect_failed():
    """
    Attempt to send an HTTP request via a SOCKS proxy, resulting in a connect failure.
    """
    network_backend = httpcore.MockBackend(
        [
            # The initial socks CONNECT
            #   v5 NOAUTH
            b"\x05\x00",
            #   v5  NO RSV IP4   0  .0  .0  .0     :00
            b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value) == "Proxy Server could not connect: Connection refused."
        )

        assert not proxy.connections



def test_socks5_request_failed_to_provide_auth():
    """
    Attempt to send an HTTP request via an authenticated SOCKS proxy,
    without providing authentication credentials.
    """
    network_backend = httpcore.MockBackend(
        [
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value)
            == "Requested NO AUTHENTICATION REQUIRED from proxy server, but got USERNAME/PASSWORD."
        )

        assert not proxy.connections



def test_socks5_request_incorrect_auth():
    """
    Attempt to send an HTTP request via an authenticated SOCKS proxy,
    wit incorrect authentication credentials.
    """
    network_backend = httpcore.MockBackend(
        [
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
            #   v1 INVALID USERNAME/PASSWORD
            b"\x01\x01",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"invalid", b"invalid"),
        ),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == "Invalid username/password"

        assert not proxy.connections



def test_authenticated_socks5_rejects_no_auth_method():
    """
    When username/password credentials are configured the client must not
    accept a NO AUTHENTICATION REQUIRED answer: that would silently skip the
    configured credentials and establish an unauthenticated tunnel.
    """
    network_backend = httpcore.MockBackend(
        [
            # The proxy offers no-auth even though the client asked for
            # username/password authentication.
            b"\x05\x00",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"username", b"password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value)
            == "Requested USERNAME/PASSWORD from proxy server, "
            "but got NO AUTHENTICATION REQUIRED."
        )

        assert not proxy.connections
        _assert_negotiation_failed(network_backend)



def test_socks5_rejects_no_acceptable_methods():
    """
    A 0xFF method selection reply means the proxy supports none of the
    client's methods; the negotiation must fail rather than proceed.
    """
    network_backend = httpcore.MockBackend(
        [
            # VER=5, NO ACCEPTABLE METHODS
            b"\x05\xff",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value)
            == "Requested NO AUTHENTICATION REQUIRED from proxy server, "
            "but got NO ACCEPTABLE METHODS."
        )

        assert not proxy.connections
        _assert_negotiation_failed(network_backend)



def test_socks5_missing_auth_subnegotiation_reply():
    """
    If the proxy selects username/password but closes the connection
    without sending the sub-negotiation reply, the client must surface a
    ProxyError instead of treating the missing reply as success.
    """
    network_backend = httpcore.MockBackend(
        [
            # VER=5, USERNAME/PASSWORD, then nothing further.
            b"\x05\x02",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"username", b"password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == (
            "Proxy Server closed the connection unexpectedly."
        )

        assert not proxy.connections
        _assert_negotiation_failed(network_backend)



def test_socks5_fragmented_method_reply():
    """
    A method selection reply split across multiple network reads must be
    reassembled before being parsed.
    """
    network_backend = httpcore.MockBackend(
        [
            b"\x05",
            b"\x00",
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            *HTTP_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_socks5_fragmented_connect_reply():
    """
    A CONNECT reply split across several reads, including a one-byte read,
    must be framed according to its address fields rather than assuming a
    single read returns the whole message.
    """
    network_backend = httpcore.MockBackend(
        [
            b"\x05\x00",
            # CONNECT reply delivered one fragment at a time.
            b"\x05\x00",
            b"\x00\x01",
            b"\xff\x00\x00\x01",
            b"\x00\x50",
            *HTTP_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_socks5_fragmented_auth_subnegotiation_reply():
    """
    The two-byte username/password sub-negotiation reply may itself be
    fragmented; both bytes must be collected before checking success.
    """
    network_backend = httpcore.MockBackend(
        [
            b"\x05\x02",
            b"\x01",
            b"\x00",
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            *HTTP_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"username", b"password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_socks5_coalesced_reply_and_target_data():
    """
    A single read may carry the final SOCKS5 reply together with bytes from
    the tunneled target. The surplus bytes must be buffered and consumed as
    the HTTP response rather than parsed as another SOCKS5 message.
    """
    network_backend = httpcore.MockBackend(
        [
            b"\x05\x00",
            # CONNECT reply plus the beginning of the target's HTTP response
            # arriving in the same network read.
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50"
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_socks5_connect_failure_closes_connection_without_tls():
    """
    A failed CONNECT must close the connection and must never start TLS or
    send HTTP traffic to the target.
    """
    network_backend = httpcore.MockBackend(
        [
            # VER=5, NO AUTHENTICATION REQUIRED
            b"\x05\x00",
            # VER=5, CONNECTION REFUSED, RSV=0, IPv4 0.0.0.0:0
            b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value)
            == "Proxy Server could not connect: Connection refused."
        )

        assert not proxy.connections
        _assert_negotiation_failed(network_backend)



@pytest.mark.parametrize(
    ("target", "atype"),
    [
        # IPv4 literal
        ("https://127.0.0.1/", b"\x01"),
        # IPv6 literal
        ("https://[2001:db8::1]/", b"\x04"),
        # Domain name, which the proxy resolves remotely
        ("https://example.com/", b"\x03"),
    ],
)
def test_socks5_connect_request_encoding(target, atype):
    """
    The CONNECT request must encode IPv4, IPv6 and domain name targets with
    the correct SOCKS5 address type. Domain names are sent to the proxy for
    remote resolution rather than being resolved locally first.
    """
    network_backend = httpcore.MockBackend(
        [
            *NO_AUTH_HANDSHAKE,
            *HTTP_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", target)
        assert response.status == 200
        assert response.content == b"Hello, world!"

        # The stream must still be open after a successful negotiation.
        assert not network_backend.streams[0].is_closed()

    # The second write is the CONNECT request: VER, CMD=CONNECT, RSV, ATYP...
    connect_request = network_backend.streams[0].writes[1]
    assert connect_request[0:3] == b"\x05\x01\x00"
    assert connect_request[3:4] == atype



def test_socks5_domain_name_encoding_is_not_locally_resolved():
    """
    Domain targets must be encoded with the domain address type (0x03),
    prefixed by the one-byte length, so resolution happens at the proxy.
    """
    network_backend = httpcore.MockBackend(
        [
            *NO_AUTH_HANDSHAKE,
            *HTTP_RESPONSE,
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200

    connect_request = network_backend.streams[0].writes[1]
    # VER=5 CMD=1 RSV=0 ATYP=3 LEN=11 example.com PORT=443
    assert connect_request == (
        b"\x05\x01\x00\x03\x0b" + b"example.com" + b"\x01\xbb"
    )



def test_socks5_malformed_reply_closes_connection():
    """
    Bytes that cannot be parsed as the expected SOCKS5 message must raise a
    ProxyError and close the connection rather than being mistaken for a
    successful negotiation.
    """
    network_backend = httpcore.MockBackend(
        [
            # A parseable method reply, followed by garbage where the CONNECT
            # reply is expected.
            b"\x05\x00",
            b"\x02\x00\x00\x01\xff\x00\x00\x01\x00\x50",
        ]
    )

    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(httpcore.ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert "Malformed SOCKS5 reply" in str(exc_info.value)

        assert not proxy.connections
        _assert_negotiation_failed(network_backend)
