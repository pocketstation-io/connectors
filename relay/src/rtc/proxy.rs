//! Finite WebSocket connections through an HTTP or HTTPS CONNECT proxy.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc;
use std::time::Instant;

use base64::Engine;
use pocketstation::connector::ConnectorSecret;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::protocol::WebSocketConfig;
use url::Url;

use super::handshake::resolve_before_deadline;
use super::signaling::Ws;
use tungstenite::stream::MaybeTlsStream;

pub(crate) type ProxyStream = MaybeTlsStream<TcpStream>;

const MAX_PROXY_HEADERS_BYTES: usize = 16_384;
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProxyConnectError {
    #[error("Relay denied the source capability")]
    Authorization,
    #[error("Relay WebSocket upgrade or proxy negotiation was rejected")]
    Rejected,
    #[error("Relay proxy rejected CONNECT with HTTP {0}; direct fallback is disabled")]
    ConnectRejected(u16),
    #[error("Relay proxy TLS handshake failed")]
    ProxyTlsHandshake,
    #[error("Relay TLS handshake failed")]
    RelayTlsHandshake,
    #[error("Relay WebSocket upgrade connection failed")]
    Transport,
}

pub(crate) fn connect(
    ws_url: &str,
    explicit_proxy: Option<&ConnectorSecret>,
    bearer_token: Option<&str>,
    subprotocol: Option<&str>,
    deadline: Instant,
) -> Result<Ws, Box<dyn std::error::Error>> {
    connect_with_tls(
        ws_url,
        explicit_proxy,
        bearer_token,
        subprotocol,
        deadline,
        TlsConnectors::default(),
    )
}

// The public path always uses the platform trust store for each independent TLS hop.
// Private tests provide roots on the two connector values without changing system trust.
#[derive(Default)]
struct TlsConnectors<'a> {
    proxy: Option<&'a native_tls::TlsConnector>,
    relay: Option<native_tls::TlsConnector>,
}

fn connect_with_tls(
    ws_url: &str,
    explicit_proxy: Option<&ConnectorSecret>,
    bearer_token: Option<&str>,
    subprotocol: Option<&str>,
    deadline: Instant,
    tls: TlsConnectors<'_>,
) -> Result<Ws, Box<dyn std::error::Error>> {
    let target = Url::parse(ws_url).map_err(|_| "Relay WebSocket URL is invalid")?;
    let target_authority = authority(&target)?;
    let proxy = match explicit_proxy {
        Some(proxy) => Some(proxy.expose_secret().to_owned()),
        None => environment_proxy(&target),
    }
    .map(|value| checked_proxy(&value))
    .transpose()?;
    let tcp = connect_tcp(
        &proxy
            .as_ref()
            .map_or_else(|| Ok(target_authority.clone()), authority)?,
        deadline,
    )?;
    before_deadline(tcp.try_clone()?, deadline, || {
        let mut stream = connect_proxy_stream(tcp, proxy.as_ref(), deadline, tls.proxy)?;
        if let Some(proxy) = &proxy {
            establish_tunnel(&mut stream, &target_authority, proxy, deadline)?;
        }
        set_deadline(tcp_socket(&stream)?, deadline)?;
        let mut request = ws_url
            .into_client_request()
            .map_err(|_| "Relay WebSocket request is invalid")?;
        if let Some(token) = bearer_token {
            let authorization = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| "Relay source capability is invalid")?;
            request.headers_mut().insert("Authorization", authorization);
        }
        if let Some(protocol) = subprotocol {
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", HeaderValue::from_str(protocol)?);
        }
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(MAX_WEBSOCKET_MESSAGE_BYTES)
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_WEBSOCKET_MESSAGE_BYTES)
            .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES));
        let (ws, response) = tungstenite::client_tls_with_config(
            request,
            stream,
            Some(websocket_config),
            tls.relay.map(tungstenite::Connector::NativeTls),
        )
        .map_err(|error| match error {
            tungstenite::HandshakeError::Failure(tungstenite::Error::Http(response))
                if matches!(response.status().as_u16(), 401 | 403) =>
            {
                ProxyConnectError::Authorization
            }
            tungstenite::HandshakeError::Failure(tungstenite::Error::Io(_))
            | tungstenite::HandshakeError::Interrupted(_) => ProxyConnectError::Transport,
            tungstenite::HandshakeError::Failure(tungstenite::Error::Tls(error)) => {
                classify_relay_tls_error(&error)
            }
            tungstenite::HandshakeError::Failure(_) => ProxyConnectError::Rejected,
        })?;
        if let Some(protocol) = subprotocol {
            if response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|value| value.to_str().ok())
                != Some(protocol)
            {
                return Err(ProxyConnectError::Rejected.into());
            }
        }
        Ok(ws)
    })
}

fn tcp_socket(stream: &ProxyStream) -> Result<&TcpStream, Box<dyn std::error::Error>> {
    match stream {
        MaybeTlsStream::Plain(socket) => Ok(socket),
        MaybeTlsStream::NativeTls(tls) => Ok(tls.get_ref()),
        _ => Err("Relay proxy transport cannot expose its deadline socket".into()),
    }
}

pub(crate) fn socket(ws: &Ws) -> Result<&TcpStream, Box<dyn std::error::Error>> {
    let stream = match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::NativeTls(tls) => tls.get_ref(),
        _ => return Err("Relay WebSocket transport cannot expose its deadline socket".into()),
    };
    tcp_socket(stream)
}

pub(crate) fn clone_socket(ws: &Ws) -> Result<TcpStream, Box<dyn std::error::Error>> {
    Ok(socket(ws)?.try_clone()?)
}

fn connect_proxy_stream(
    tcp: TcpStream,
    proxy: Option<&Url>,
    deadline: Instant,
    connector: Option<&native_tls::TlsConnector>,
) -> Result<ProxyStream, Box<dyn std::error::Error>> {
    let Some(proxy) = proxy.filter(|proxy| proxy.scheme() == "https") else {
        return Ok(MaybeTlsStream::Plain(tcp));
    };
    set_deadline(&tcp, deadline)?;
    let host = proxy.host_str().ok_or("Relay proxy URL has no host")?;
    let default_connector;
    let connector = if let Some(connector) = connector {
        connector
    } else {
        default_connector =
            native_tls::TlsConnector::new().map_err(|_| ProxyConnectError::ProxyTlsHandshake)?;
        &default_connector
    };
    let tls = connector
        .connect(host, tcp)
        .map_err(|error| classify_proxy_tls_error(&error))?;
    Ok(MaybeTlsStream::NativeTls(tls))
}

fn classify_proxy_tls_error(error: &native_tls::HandshakeError<TcpStream>) -> ProxyConnectError {
    if matches!(error, native_tls::HandshakeError::WouldBlock(_)) {
        return ProxyConnectError::Transport;
    }
    if let native_tls::HandshakeError::Failure(error) = error {
        if has_transport_cause(error) {
            return ProxyConnectError::Transport;
        }
    }
    // native-tls does not expose a portable certificate-vs-protocol reason.
    ProxyConnectError::ProxyTlsHandshake
}

fn classify_relay_tls_error(error: &tungstenite::error::TlsError) -> ProxyConnectError {
    if let tungstenite::error::TlsError::Native(error) = error {
        if has_transport_cause(error.as_ref()) {
            return ProxyConnectError::Transport;
        }
    }
    // An opaque backend error is terminal; its text cannot prove a trust cause.
    ProxyConnectError::RelayTlsHandshake
}

fn has_transport_cause(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..8 {
        let Some(current) = cause else { break };
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
            ) {
                return true;
            }
        }
        cause = current.source();
    }
    false
}

pub(crate) fn before_deadline<T>(
    deadline_socket: TcpStream,
    deadline: Instant,
    operation: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or("Relay connection exceeded its startup deadline")?;
    let (finished_tx, finished_rx) = mpsc::sync_channel(1);
    // Socket timeouts restart after partial reads; closing this clone enforces
    // the same absolute deadline across CONNECT, TLS and Upgrade negotiation.
    let watchdog = std::thread::Builder::new()
        .name("pks-ws-startup".into())
        .spawn(move || {
            if finished_rx.recv_timeout(remaining).is_err() {
                let _ = deadline_socket.shutdown(Shutdown::Both);
            }
        })?;
    let result = operation();
    let _ = finished_tx.try_send(());
    watchdog
        .join()
        .map_err(|_| "Relay connection deadline worker failed")?;
    if Instant::now() >= deadline {
        return Err("Relay connection exceeded its startup deadline".into());
    }
    result
}

pub(crate) fn checked_proxy(raw: &str) -> Result<Url, Box<dyn std::error::Error>> {
    let proxy = Url::parse(raw).map_err(|_| "Relay proxy URL is invalid")?;
    if !matches!(proxy.scheme(), "http" | "https")
        || proxy.host_str().is_none()
        || !matches!(proxy.path(), "" | "/")
        || proxy.query().is_some()
        || proxy.fragment().is_some()
    {
        return Err(
            "Relay proxy must be an HTTP or HTTPS CONNECT proxy origin; SOCKS is unsupported"
                .into(),
        );
    }
    Ok(proxy)
}

fn authority(url: &Url) -> Result<String, Box<dyn std::error::Error>> {
    let host = url.host().ok_or("WebSocket or proxy URL has no host")?;
    let port = url
        .port_or_known_default()
        .ok_or("WebSocket or proxy URL has no port")?;
    Ok(format!("{host}:{port}"))
}

fn connect_tcp(
    authority: &str,
    deadline: Instant,
) -> Result<TcpStream, Box<dyn std::error::Error>> {
    for address in resolve_before_deadline(authority.to_owned(), deadline)? {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("Relay connection exceeded its startup deadline")?;
        if let Ok(stream) = TcpStream::connect_timeout(&address, remaining) {
            stream.set_nodelay(true)?;
            set_deadline(&stream, deadline)?;
            return Ok(stream);
        }
    }
    Err("Relay connection failed within its startup deadline".into())
}

fn set_deadline(stream: &TcpStream, deadline: Instant) -> Result<(), Box<dyn std::error::Error>> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or("Relay connection exceeded its startup deadline")?;
    stream.set_read_timeout(Some(remaining))?;
    stream.set_write_timeout(Some(remaining))?;
    Ok(())
}

fn establish_tunnel(
    stream: &mut ProxyStream,
    target: &str,
    proxy: &Url,
    deadline: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
    if !proxy.username().is_empty() || proxy.password().is_some() {
        let username = decode_userinfo(proxy.username())?;
        let password = decode_userinfo(proxy.password().unwrap_or_default())?;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        request.push_str("Proxy-Authorization: Basic ");
        request.push_str(&encoded);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    set_deadline(tcp_socket(stream)?, deadline)?;
    stream
        .write_all(request.as_bytes())
        .map_err(|_| "Relay proxy CONNECT write failed")?;
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        if response.len() >= MAX_PROXY_HEADERS_BYTES {
            return Err("Relay proxy headers exceeded 16384 bytes".into());
        }
        set_deadline(tcp_socket(stream)?, deadline)?;
        let mut byte = [0];
        stream
            .read_exact(&mut byte)
            .map_err(|_| "Relay proxy CONNECT response failed")?;
        response.push(byte[0]);
    }
    let status = std::str::from_utf8(&response)
        .ok()
        .and_then(|response| response.lines().next())
        .and_then(|line| {
            let mut words = line.split_whitespace();
            let protocol = words.next()?;
            if !matches!(protocol, "HTTP/1.0" | "HTTP/1.1") {
                return None;
            }
            words.next()?.parse::<u16>().ok()
        });
    match status {
        Some(200..=299) => Ok(()),
        Some(status) => Err(ProxyConnectError::ConnectRejected(status).into()),
        None => Err("Relay proxy CONNECT response is invalid".into()),
    }
}

fn decode_userinfo(raw: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut result = Vec::with_capacity(raw.len());
    let mut bytes = raw.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes
                .next()
                .and_then(|value| char::from(value).to_digit(16))
                .ok_or("Relay proxy credentials are invalid")?;
            let low = bytes
                .next()
                .and_then(|value| char::from(value).to_digit(16))
                .ok_or("Relay proxy credentials are invalid")?;
            result.push(u8::try_from(high * 16 + low)?);
        } else {
            result.push(byte);
        }
    }
    String::from_utf8(result).map_err(|_| "Relay proxy credentials are invalid".into())
}

fn environment_proxy(target: &Url) -> Option<String> {
    let exclusion = std::env::var("no_proxy")
        .or_else(|_| std::env::var("NO_PROXY"))
        .unwrap_or_default();
    if bypass_proxy(target, &exclusion) {
        return None;
    }
    ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
}

fn bypass_proxy(target: &Url, exclusions: &str) -> bool {
    let Some(host) = target.host_str() else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);
    exclusions
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .any(|entry| {
            if entry == "*" {
                return true;
            }
            let with_port = if entry.starts_with('[') {
                entry
                    .rsplit_once("]:")
                    .map(|(host, port)| (host.trim_start_matches('['), port))
            } else if entry.matches(':').count() == 1 {
                entry.rsplit_once(':')
            } else {
                None
            };
            let (entry_host, entry_port) = with_port
                .filter(|(_, port)| port.parse::<u16>().is_ok())
                .map_or((entry, None), |(host, port)| {
                    (host, port.parse::<u16>().ok())
                });
            if entry_port.is_some() && entry_port != target.port_or_known_default() {
                return false;
            }
            let entry_host = entry_host.trim_start_matches('.').trim_matches(['[', ']']);
            host.eq_ignore_ascii_case(entry_host)
                || host
                    .to_ascii_lowercase()
                    .ends_with(&format!(".{}", entry_host.to_ascii_lowercase()))
        })
}

#[cfg(test)]
#[path = "proxy_test.rs"]
mod deadline_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    #[allow(clippy::result_large_err)] // tungstenite's required callback error type
    fn given_explicit_proxy_when_target_dns_is_unavailable_then_connect_uses_proxy_authority_only()
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_url = format!("http://user:p%40ss@{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("CONNECT no-direct-resolution.invalid:80 HTTP/1.1\r\n"));
            assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwQHNz\r\n"));
            assert!(!request.contains("source-secret"));
            stream
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .unwrap();
            let mut ws = tungstenite::accept_hdr(
                stream,
                |request: &tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.headers()["Authorization"], "Bearer source-secret");
                    assert!(request.headers().get("Proxy-Authorization").is_none());
                    Ok(response)
                },
            )
            .unwrap();
            assert!(matches!(ws.read().unwrap(), tungstenite::Message::Close(_)));
        });
        let proxy = ConnectorSecret::new(proxy_url).unwrap();
        let mut ws = connect(
            "ws://no-direct-resolution.invalid/path",
            Some(&proxy),
            Some("source-secret"),
            None,
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        ws.close(None).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn given_proxy_bypass_rules_when_matching_then_domain_boundaries_and_ports_are_respected() {
        let target = Url::parse("wss://api.example.com:443/path").unwrap();
        assert!(bypass_proxy(&target, ".example.com:443"));
        assert!(!bypass_proxy(&target, "example.com:80"));
        assert!(!bypass_proxy(
            &Url::parse("wss://badexample.com/").unwrap(),
            "example.com"
        ));
        assert!(bypass_proxy(&target, "*"));
    }

    #[test]
    fn given_rejected_proxy_when_connecting_then_error_is_bounded_redacted_and_no_direct_fallback_occurs(
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy =
            ConnectorSecret::new(format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            stream
                .write_all(b"HTTP/1.1 407 source-secret\r\n\r\n")
                .unwrap();
        });
        let error = connect(
            "wss://no-direct-resolution.invalid/path",
            Some(&proxy),
            None,
            None,
            Instant::now() + Duration::from_secs(2),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(error.contains("407"));
        assert!(error.contains("direct fallback is disabled"));
        assert!(!error.contains("source-secret"));
        server.join().unwrap();
    }
}
