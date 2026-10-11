use super::*;
use std::net::TcpListener;
use std::time::Duration;

fn fixture_socket(listener: &TcpListener) -> std::net::TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match listener.accept() {
            Ok((socket, _)) => {
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                return socket;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "fixture accept exceeded three seconds"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(_) => panic!("fixture listener failed"),
        }
    }
}

fn fixture_pem(label: &str, der: &[u8]) -> Vec<u8> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let mut output = format!("-----BEGIN {label}-----\n").into_bytes();
    for line in encoded.as_bytes().chunks(64) {
        output.extend_from_slice(line);
        output.push(b'\n');
    }
    output.extend_from_slice(format!("-----END {label}-----\n").as_bytes());
    output
}

fn tls_fixture(name: &str) -> (native_tls::TlsAcceptor, native_tls::TlsConnector) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).unwrap();
    let mut params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::KeyCertSign,
    ];
    let certificate = params.self_signed(&key).unwrap();
    let identity = native_tls::Identity::from_pkcs8(
        &fixture_pem("CERTIFICATE", certificate.der().as_ref()),
        &fixture_pem("PRIVATE KEY", &key.serialize_der()),
    )
    .unwrap();
    let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
    // This root applies only to this connector. No system certificate store is changed.
    let connector = native_tls::TlsConnector::builder()
        .disable_built_in_roots(true)
        .add_root_certificate(
            native_tls::Certificate::from_der(certificate.der().as_ref()).unwrap(),
        )
        .build()
        .unwrap();
    (acceptor, connector)
}

fn assert_no_application_bytes<S: Read + Write>(
    result: Result<native_tls::TlsStream<S>, native_tls::HandshakeError<S>>,
) {
    // Backends may finish the server handshake before client-side trust refusal.
    // In either case no CONNECT or authenticated WebSocket bytes may follow.
    if let Ok(mut stream) = result {
        let mut byte = [0];
        if let Ok(count) = stream.read(&mut byte) {
            assert_eq!(count, 0, "trust-refused connection sent application bytes");
        }
    }
}

#[test]
#[allow(clippy::result_large_err)] // tungstenite's required server callback error type
fn given_two_trusted_tls_hops_when_connecting_then_credentials_stay_at_their_own_boundary() {
    let (proxy_server, proxy_client) = tls_fixture("127.0.0.1");
    let (relay_server, relay_client) = tls_fixture("relay-fixture.invalid");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = ConnectorSecret::new(format!(
        "https://user:proxy-secret@{}",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let server = std::thread::spawn(move || {
        let mut proxy_tls = proxy_server.accept(fixture_socket(&listener)).unwrap();
        let mut connect_header = Vec::new();
        while !connect_header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            proxy_tls.read_exact(&mut byte).unwrap();
            connect_header.push(byte[0]);
            assert!(connect_header.len() <= MAX_PROXY_HEADERS_BYTES);
        }
        let header = String::from_utf8(connect_header).unwrap();
        assert!(header.starts_with("CONNECT relay-fixture.invalid:443 HTTP/1.1\r\n"));
        assert!(header.contains("Proxy-Authorization: Basic "));
        assert!(!header.contains("source-secret"));
        proxy_tls
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        let relay_tls = relay_server.accept(proxy_tls).unwrap();
        let mut websocket = tungstenite::accept_hdr(
            relay_tls,
            |request: &tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers()["Authorization"], "Bearer source-secret");
                assert!(request.headers().get("Proxy-Authorization").is_none());
                Ok(response)
            },
        )
        .unwrap();
        assert!(matches!(
            websocket.read().unwrap(),
            tungstenite::Message::Close(_)
        ));
    });
    let mut websocket = connect_with_tls(
        "wss://relay-fixture.invalid/stem",
        Some(&proxy),
        Some("source-secret"),
        None,
        Instant::now() + Duration::from_secs(2),
        TlsConnectors {
            proxy: Some(&proxy_client),
            relay: Some(relay_client),
        },
    )
    .unwrap();
    websocket.close(None).unwrap();
    server.join().unwrap();
}

#[test]
fn given_untrusted_proxy_certificate_when_using_public_defaults_then_proxy_tls_refuses() {
    let (acceptor, _) = tls_fixture("127.0.0.1");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = ConnectorSecret::new(format!(
        "https://user:proxy-secret@{}",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let server = std::thread::spawn(move || {
        assert_no_application_bytes(acceptor.accept(fixture_socket(&listener)))
    });
    let error = connect(
        "wss://relay-fixture.invalid/stem",
        Some(&proxy),
        Some("source-secret"),
        None,
        Instant::now() + Duration::from_secs(2),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.downcast_ref::<ProxyConnectError>(),
        Some(ProxyConnectError::ProxyTlsHandshake)
    ));
    assert!(!error.to_string().contains("secret"));
    server.join().unwrap();
}

#[test]
fn given_trusted_proxy_certificate_for_another_host_when_connecting_then_hostname_refuses() {
    let (acceptor, connector) = tls_fixture("another-proxy.invalid");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy =
        ConnectorSecret::new(format!("https://{}", listener.local_addr().unwrap())).unwrap();
    let server = std::thread::spawn(move || {
        assert_no_application_bytes(acceptor.accept(fixture_socket(&listener)))
    });
    let error = connect_with_tls(
        "wss://relay-fixture.invalid/stem",
        Some(&proxy),
        None,
        None,
        Instant::now() + Duration::from_secs(2),
        TlsConnectors {
            proxy: Some(&connector),
            relay: None,
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.downcast_ref::<ProxyConnectError>(),
        Some(ProxyConnectError::ProxyTlsHandshake)
    ));
    server.join().unwrap();
}

#[test]
fn given_trusted_proxy_and_wrong_relay_hostname_when_connecting_then_target_tls_is_independently_refused(
) {
    let (proxy_server, proxy_client) = tls_fixture("127.0.0.1");
    let (relay_server, relay_client) = tls_fixture("another-relay.invalid");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy =
        ConnectorSecret::new(format!("https://{}", listener.local_addr().unwrap())).unwrap();
    let server = std::thread::spawn(move || {
        let mut stream = proxy_server.accept(fixture_socket(&listener)).unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
            assert!(header.len() <= MAX_PROXY_HEADERS_BYTES);
        }
        stream
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        assert_no_application_bytes(relay_server.accept(stream));
    });
    let error = connect_with_tls(
        "wss://relay-fixture.invalid/stem",
        Some(&proxy),
        Some("source-secret"),
        None,
        Instant::now() + Duration::from_secs(2),
        TlsConnectors {
            proxy: Some(&proxy_client),
            relay: Some(relay_client),
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.downcast_ref::<ProxyConnectError>(),
        Some(ProxyConnectError::RelayTlsHandshake)
    ));
    assert!(!error.to_string().contains("secret"));
    server.join().unwrap();
}

#[test]
fn given_ipv6_authorities_when_connecting_then_brackets_and_proxy_exclusions_are_unambiguous() {
    let target = Url::parse("wss://[::1]:8443/path").unwrap();
    assert_eq!(authority(&target).unwrap(), "[::1]:8443");
    assert!(bypass_proxy(&target, "::1"));
    assert!(bypass_proxy(&target, "[::1]"));
    assert!(bypass_proxy(&target, "[::1]:8443"));
    assert!(!bypass_proxy(&target, "[::1]:443"));
}

#[test]
fn given_partial_upgrade_bytes_when_connecting_then_the_absolute_deadline_bounds_startup() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = ConnectorSecret::new(format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        socket
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .unwrap();
        header.clear();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        for byte in
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        {
            if socket.write_all(&[*byte]).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    });
    let started = Instant::now();
    let error = connect(
        "ws://unresolved.invalid/path",
        Some(&proxy),
        None,
        None,
        started + Duration::from_millis(300),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(error.contains("deadline"));
    server.join().unwrap();
}

#[test]
fn given_unsupported_proxy_when_validating_then_errors_do_not_expose_credentials() {
    for raw in [
        "socks5://user:secret@proxy.example",
        "http://user:secret@proxy.example/path",
        "http://user:secret@proxy.example?query=secret",
    ] {
        let error = checked_proxy(raw).unwrap_err().to_string();
        assert!(!error.contains("secret"));
        assert!(!error.contains("user"));
    }
}

#[test]
fn given_https_proxy_origin_when_validating_then_tls_origin_and_credentials_are_retained() {
    let proxy = checked_proxy("https://user:p%40ss@proxy.example:8443/").unwrap();
    assert_eq!(proxy.scheme(), "https");
    assert_eq!(authority(&proxy).unwrap(), "proxy.example:8443");
    assert_eq!(decode_userinfo(proxy.password().unwrap()).unwrap(), "p@ss");
}

#[test]
fn given_stalled_proxy_tls_when_connecting_then_deadline_closes_tls_before_credentials_are_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = ConnectorSecret::new(format!(
        "https://user:proxy-secret@{}",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut hello = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            match socket.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => {
                    hello.extend_from_slice(&chunk[..count]);
                    assert!(hello.len() <= MAX_PROXY_HEADERS_BYTES);
                }
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(error) => panic!("unexpected fixture read category: {:?}", error.kind()),
            }
        }
        assert_eq!(hello.first(), Some(&0x16)); // TLS handshake record, before CONNECT.
        for secret in [b"CONNECT".as_slice(), b"proxy-secret", b"source-secret"] {
            assert!(!hello.windows(secret.len()).any(|bytes| bytes == secret));
        }
    });
    let started = Instant::now();
    let error = connect(
        "wss://unresolved.invalid/path",
        Some(&proxy),
        Some("source-secret"),
        None,
        started + Duration::from_millis(300),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(error.contains("deadline"));
    assert!(!error.contains("proxy-secret"));
    assert!(!error.contains("source-secret"));
    server.join().unwrap();
}

#[test]
fn given_rejected_proxy_when_target_is_reachable_then_no_direct_connection_is_attempted() {
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    let proxy_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = ConnectorSecret::new(format!(
        "http://user:proxy-secret@{}",
        proxy_listener.local_addr().unwrap()
    ))
    .unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = proxy_listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
            assert!(header.len() <= MAX_PROXY_HEADERS_BYTES);
        }
        assert!(!header
            .windows(b"source-secret".len())
            .any(|bytes| bytes == b"source-secret"));
        socket
            .write_all(b"HTTP/1.1 407 private-body-secret\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
    });
    let error = connect(
        &format!("wss://{}/path", target.local_addr().unwrap()),
        Some(&proxy),
        Some("source-secret"),
        None,
        Instant::now() + Duration::from_secs(2),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.downcast_ref::<ProxyConnectError>(),
        Some(ProxyConnectError::ConnectRejected(407))
    ));
    assert!(!error.to_string().contains("secret"));
    assert!(
        matches!(target.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    server.join().unwrap();
}

#[test]
fn given_typed_or_opaque_error_causes_when_classifying_then_only_io_is_transport() {
    #[derive(Debug)]
    struct WrappedIo(std::io::Error);
    impl std::fmt::Display for WrappedIo {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("fixture transport category")
        }
    }
    impl std::error::Error for WrappedIo {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    assert!(has_transport_cause(&WrappedIo(std::io::Error::from(
        std::io::ErrorKind::ConnectionReset
    ))));
    assert!(!has_transport_cause(&std::io::Error::from(
        std::io::ErrorKind::InvalidData
    )));
    assert!(!has_transport_cause(&std::fmt::Error));
}
