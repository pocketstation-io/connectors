//! Shared ICE/DTLS handshake performed by every source type.

use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use url::Url;

use crate::configuration::RelayIceServer;
use crate::rtc::signaling::{
    send_leave, wait_for_sdp_answer, ws_set_read_timeout, ws_set_write_timeout, PublishBusBinding,
    PublishMsg, ServerMsg, Ws, WS_POLL_TIMEOUT, WS_WRITE_TIMEOUT,
};
use crate::rtc::types::drain_all_outputs;

/// UDP socket read timeout — short enough to pace the RTC event loop without starving it.
const UDP_READ_TIMEOUT_MS: u64 = 5;
/// Timeout for the STUN binding request. 2 s is generous; local NAT round-trip is <100 ms.
const STUN_TIMEOUT_MS: u64 = 2_000;

// Handshake result

/// Everything a source function needs after the ICE/DTLS handshake succeeds.
pub(crate) struct HandshakeResult {
    pub(crate) rtc: Rtc,
    pub(crate) ws: Ws,
    pub(crate) mids: Vec<Mid>,
    pub(crate) bound_addr: SocketAddr,
    pub(crate) udp: UdpSocket,
}

#[allow(dead_code)]
pub(crate) fn cancel_handshake(
    mut handshake: HandshakeResult,
    session_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let leave_result = send_leave(&mut handshake.ws, session_id);
    let close_result = handshake.ws.close(None);
    leave_result?;
    close_result?;
    Ok(())
}

/// 1 kbps → bits/s for the SDP `maxaveragebitrate` parameter. Matches the
/// historical hard-coded 131072 (= 128 × 1024) so the default path is unchanged.
const SDP_BITS_PER_KBPS: u32 = 1024;

/// Opus media parameters carried in the SDP offer: channel layout, the
/// receiver-side average-bitrate hint, and the WebRTC stream/track id (the
/// `AudioBus` label). A mono profile must not advertise stereo, and a high-bitrate
/// profile must not be capped at the music default (Corrected Audit §4).
pub(crate) struct PublishMedia {
    /// Advertise `stereo=1` / `sprop-stereo=1` (true) or `=0` (mono profiles).
    pub(crate) stereo: bool,
    /// `maxaveragebitrate` hint, in kbps; converted to bits/s in the SDP.
    pub(crate) max_avg_bitrate_kbps: u32,
    /// Exact WebRTC msid / track id bound to one Relay `AudioBus`.
    pub(crate) stream_id: String,
}

// ICE local-IP probe
//
// Returns the IP the OS would actually use to reach the relay.
// Binds a temporary UDP socket and calls connect() — no packet is sent;
// UDP connect just sets routing state so local_addr() returns the real
// outbound interface address instead of 0.0.0.0.

pub(crate) fn probe_local_ip(
    relay_url: &str,
    deadline: Instant,
) -> Result<IpAddr, Box<dyn std::error::Error>> {
    let host_part = relay_url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("wss://")
        .trim_start_matches("ws://");

    let host_and_port = host_part.split('/').next().unwrap_or(host_part);

    let addr_str = if host_and_port.contains(':') {
        host_and_port.to_string()
    } else {
        format!("{host_and_port}:80")
    };

    // Prefer IPv4: macOS resolves "localhost" to ::1 first, but pion's default
    // ICE generates IPv4 host candidates.  If the local candidate is IPv6 and
    // the only remote candidates are IPv4, str0m forms no pairs and ICE never
    // checks.  Fall back to the first address of any family if no IPv4 exists.
    let all_addrs = resolve_before_deadline(addr_str, deadline)?;
    let relay_addr = all_addrs
        .iter()
        .find(|address| address.is_ipv4())
        .or_else(|| all_addrs.first())
        .copied()
        .ok_or("could not resolve relay host for ICE IP probe")?;

    let bind_str = match relay_addr {
        SocketAddr::V4(_) => "0.0.0.0:0",
        SocketAddr::V6(_) => "[::]:0",
    };
    let probe = UdpSocket::bind(bind_str)?;
    probe.connect(relay_addr)?;
    let ip = probe.local_addr()?.ip();

    // On macOS the loopback route does not set a concrete source address, so
    // local_addr() returns the unspecified address (0.0.0.0 / ::) when the
    // relay is on the same machine.  In that case the source and relay share
    // the host; use the relay's own address as the ICE host candidate.
    if ip.is_unspecified() {
        return Ok(relay_addr.ip());
    }

    Ok(ip)
}

// STUN Binding Request → server-reflexive (srflx) candidate discovery
//
// Sends a minimal STUN Binding Request from the caller's UDP socket to
// stun.l.google.com and parses the XOR-MAPPED-ADDRESS from the response.
// Returns None when STUN is unreachable or the response is malformed.
//
// Using the *same* socket as ICE is required: the STUN outbound packet
// creates a NAT mapping that makes the ephemeral port reachable from the
// relay. Without this NAT mapping, the relay's connectivity checks directed
// at the srflx address have nowhere to go.

fn probe_srflx(
    udp: &UdpSocket,
    base_addr: SocketAddr,
    ice_servers: &[RelayIceServer],
    deadline: Instant,
) -> Option<SocketAddr> {
    if base_addr.ip().is_loopback() {
        return None;
    }
    let result = ice_servers
        .iter()
        .flat_map(RelayIceServer::stun_authorities)
        .find_map(|authority| probe_srflx_inner(udp, authority, deadline));
    // Restore the ICE loop timeout on every success and failure path. Leaving
    // the two-second STUN timeout installed makes each ICE poll block and can
    // turn an ordinary unavailable STUN server into a 30-second handshake.
    let _ = udp.set_read_timeout(Some(Duration::from_millis(UDP_READ_TIMEOUT_MS)));
    result
}

fn probe_srflx_inner(
    udp: &UdpSocket,
    stun_authority: &str,
    deadline: Instant,
) -> Option<SocketAddr> {
    let stun_endpoint = if stun_authority.rsplit_once(':').is_some() {
        stun_authority.to_owned()
    } else {
        format!("{stun_authority}:3478")
    };
    let stun_addresses = resolve_before_deadline(stun_endpoint, deadline).ok()?;
    let stun_addr = stun_addresses
        .iter()
        .find(|address| address.is_ipv4())
        .or_else(|| stun_addresses.first())
        .copied()?;

    // STUN Binding Request: 20-byte header, no attributes.
    // Magic cookie: 0x2112A442 (RFC 5389 §6).
    let mut req = [0u8; 20];
    req[0] = 0x00; // Type high: Binding Request
    req[1] = 0x01; // Type low
    req[2] = 0x00; // Length high (no attributes)
    req[3] = 0x00; // Length low
    req[4] = 0x21; // Magic cookie
    req[5] = 0x12;
    req[6] = 0xA4;
    req[7] = 0x42;
    // Transaction ID: bytes 8-19 (12 bytes, chosen arbitrarily here).
    req[8..20].copy_from_slice(b"pks-stun-prb");

    // Apply the longer timeout only during this optional probe.
    let remaining = deadline.checked_duration_since(Instant::now())?;
    udp.set_read_timeout(Some(remaining.min(Duration::from_millis(STUN_TIMEOUT_MS))))
        .ok()?;

    udp.send_to(&req, stun_addr).ok()?;

    let mut buf = [0u8; 256];
    let (n, _) = udp.recv_from(&mut buf).ok()?;

    if n < 20 || buf[8..20] != req[8..20] {
        return None;
    }
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    // 0x0101 = Binding Success Response
    if msg_type != 0x0101 {
        return None;
    }
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    if n < 20 + msg_len {
        return None;
    }

    // Walk attributes looking for XOR-MAPPED-ADDRESS (0x0020).
    let mut pos = 20usize;
    while pos + 4 <= 20 + msg_len {
        let attr_type = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let attr_len = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if attr_type == 0x0020 && attr_len >= 8 {
            // XOR-MAPPED-ADDRESS: family(1) reserved(1) x-port(2) x-addr(4)
            let family = buf[pos + 1];
            if family == 0x01 {
                // IPv4
                let x_port = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) ^ 0x2112;
                let x_addr = u32::from_be_bytes([
                    buf[pos + 4] ^ 0x21,
                    buf[pos + 5] ^ 0x12,
                    buf[pos + 6] ^ 0xA4,
                    buf[pos + 7] ^ 0x42,
                ]);
                let ip = std::net::Ipv4Addr::from(x_addr);
                return Some(SocketAddr::new(IpAddr::V4(ip), x_port));
            }
        }
        // Attributes are 32-bit aligned.
        let padded = (attr_len + 3) & !3;
        pos += padded;
    }
    None
}

fn connect_signaling(
    ws_url: &str,
    deadline: Instant,
) -> Result<(Ws, tungstenite::handshake::client::Response), Box<dyn std::error::Error>> {
    let url = Url::parse(ws_url)?;
    let host = url.host_str().ok_or("relay signaling URL has no host")?;
    let port = url
        .port_or_known_default()
        .ok_or("relay signaling URL has no usable port")?;
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let addresses = resolve_before_deadline(authority, deadline)?;
    if addresses.is_empty() {
        return Err("relay signaling host resolved to no addresses".into());
    }

    let mut last_error = None;
    for address in addresses {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match TcpStream::connect_timeout(&address, remaining) {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(remaining))?;
                stream.set_write_timeout(Some(remaining))?;
                return tungstenite::client_tls(ws_url, stream)
                    .map_err(|error| format!("relay signaling handshake failed: {error}").into());
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error
        .map_or_else(
            || "relay signaling connection exceeded its startup deadline".to_owned(),
            |error| format!("relay signaling connection failed: {error}"),
        )
        .into())
}

fn resolve_before_deadline(
    authority: String,
    deadline: Instant,
) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or("relay name resolution exceeded its startup deadline")?;
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("pks-relay-resolver".to_owned())
        .spawn(move || {
            let result = authority.to_socket_addrs().map(Iterator::collect::<Vec<_>>);
            let _ = result_tx.send(result);
        })?;
    match result_rx.recv_timeout(remaining) {
        Ok(Ok(addresses)) if !addresses.is_empty() => Ok(addresses),
        Ok(Ok(_)) => Err("relay name resolved to no addresses".into()),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            Err("relay name resolution exceeded its startup deadline".into())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("relay name resolver exited without a result".into())
        }
    }
}

#[cfg(test)]
mod network_deadline_tests {
    use super::resolve_before_deadline;
    use std::time::{Duration, Instant};

    #[test]
    fn expired_resolution_deadline_fails_before_starting_network_work() {
        let error = resolve_before_deadline("localhost:80".to_owned(), Instant::now())
            .expect_err("expired DNS budget must fail");

        assert!(error.to_string().contains("startup deadline"));
    }

    #[test]
    fn loopback_resolution_completes_inside_a_finite_budget() {
        let addresses = resolve_before_deadline(
            "localhost:80".to_owned(),
            Instant::now() + Duration::from_secs(2),
        )
        .expect("loopback name must resolve");

        assert!(!addresses.is_empty());
    }
}

// Shared ICE/DTLS handshake
//
// Performs steps 1-7 that are identical for every source type:
//   1. Bind a local UDP socket.
//   2. Build a str0m Rtc with Opus only.
//   3. Add a SendOnly audio track and generate the SDP offer.
//   4. Open a WebSocket to the relay signaling endpoint.
//   5. Send PUBLISH with the SDP offer.
//   6. Wait for SDP_ANSWER and apply it.
//   7. Run the ICE/DTLS handshake loop until connected or timed out.
//
// Returns a `HandshakeResult` that the caller uses to start streaming.

#[allow(clippy::too_many_lines)]
pub(crate) fn run_handshake(
    relay_url: &str,
    room_id: &str,
    token: &str,
    media: &[PublishMedia],
    ice_servers: &[RelayIceServer],
    startup_timeout: Duration,
) -> Result<HandshakeResult, Box<dyn std::error::Error>> {
    if media.is_empty() {
        return Err("relay handshake requires at least one AudioBus".into());
    }
    let startup_deadline = Instant::now()
        .checked_add(startup_timeout)
        .ok_or("relay startup deadline overflow")?;

    // 1. Bind a local UDP socket for SRTP/ICE traffic.
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    udp.set_read_timeout(Some(Duration::from_millis(UDP_READ_TIMEOUT_MS)))?;
    let bound_port = udp.local_addr()?.port();

    // Resolve the real outbound IP — 0.0.0.0 is not a valid ICE candidate.
    let candidate_ip = probe_local_ip(relay_url, startup_deadline)?;
    let bound_addr = SocketAddr::new(candidate_ip, bound_port);

    // 2. Build str0m Rtc instance with Opus only, no video codecs.
    let now = Instant::now();
    let mut rtc = Rtc::builder().clear_codecs().enable_opus(true).build(now);

    let local_candidate = Candidate::host(bound_addr, "udp")
        .map_err(|e| format!("failed to create ICE host candidate: {e:?}"))?;
    rtc.add_local_candidate(local_candidate);

    // Probe STUN to discover the public (srflx) candidate. This creates a
    // NAT mapping on the client's router and on Fly.io's shared IPv4 edge
    // so the relay can send ICE connectivity checks back to this port.
    if let Some(srflx_addr) = probe_srflx(&udp, bound_addr, ice_servers, startup_deadline) {
        if srflx_addr != bound_addr {
            if let Ok(srflx_cand) = Candidate::server_reflexive(srflx_addr, bound_addr, "udp") {
                rtc.add_local_candidate(srflx_cand);
            }
        }
    }

    // 3. Add a SendOnly audio track and generate the SDP offer.
    let mut change = rtc.sdp_api();
    // The bus label travels as the WebRTC stream id and track id (msid), so the
    // relay and browser can identify which AudioBus this RTP stream carries.
    let mids = media
        .iter()
        .map(|item| {
            change.add_media(
                MediaKind::Audio,
                Direction::SendOnly,
                Some(item.stream_id.clone()),
                Some(item.stream_id.clone()),
                None,
            )
        })
        .collect::<Vec<_>>();
    let (offer, pending) = change
        .apply()
        .ok_or("SDP apply returned None — unexpected for a new SendOnly track")?;

    // str0m enables Opus but does not expose fmtp configuration, so munge the
    // generated offer to declare the profile's channel layout and bitrate hint
    // (the standard approach, identical to browser-side SDP munging). A stereo
    // source advertises sprop-stereo=1 ("I will send stereo") per RFC 7587 so the
    // relay and browser keep music stereo; a mono profile advertises 0 so a voice
    // stream is not mis-signalled as stereo.
    let offer_sdp = munge_opus_params(
        &offer.to_sdp_string(),
        media.iter().any(|item| item.stereo),
        media
            .iter()
            .map(|item| item.max_avg_bitrate_kbps)
            .max()
            .unwrap_or(128)
            * SDP_BITS_PER_KBPS,
    );

    // 4. Open WebSocket to relay signaling endpoint.
    let ws_base = relay_url
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let ws_url = format!("{ws_base}/v1/signal");

    let (mut ws, _) = connect_signaling(&ws_url, startup_deadline)?;

    // 5. Send PUBLISH with SDP offer. The bus label travels in the signaling
    //    bus_id field (what the relay routes on), not just the SDP msid.
    let publish_buses = media
        .iter()
        .map(|item| PublishBusBinding {
            stream_id: &item.stream_id,
            bus_id: &item.stream_id,
        })
        .collect::<Vec<_>>();
    let publish = PublishMsg {
        msg_type: "PUBLISH",
        room_id,
        token,
        sdp_offer: &offer_sdp,
        bus_id: None,
        publish_buses: &publish_buses,
    };
    ws.send(tungstenite::Message::Text(
        serde_json::to_string(&publish)?.into(),
    ))?;

    // 6. Wait (blocking) for SDP_ANSWER from relay.
    let remaining = startup_deadline
        .checked_duration_since(Instant::now())
        .ok_or("relay startup timed out before the SDP answer")?;
    ws_set_read_timeout(&ws, Some(remaining))?;
    ws_set_write_timeout(&ws, Some(remaining))?;
    let signaling_answer = wait_for_sdp_answer(&mut ws, room_id)?;
    let sdp_answer = SdpAnswer::from_sdp_string(&signaling_answer.sdp_answer)
        .map_err(|e| format!("failed to parse SDP answer: {e:?}"))?;

    // Apply the answer to str0m.
    let change2 = rtc.sdp_api();
    change2
        .accept_answer(pending, sdp_answer)
        .map_err(|e| format!("accept_answer failed: {e:?}"))?;
    for candidate_sdp in signaling_answer.early_candidates {
        let candidate = Candidate::from_sdp_string(&candidate_sdp)
            .map_err(|error| format!("invalid buffered relay ICE candidate: {error:?}"))?;
        rtc.add_remote_candidate(candidate);
    }

    // Drain initial transmits after accepting the answer (str0m requirement).
    let _ = drain_all_outputs(&mut rtc, &udp)?;

    // 7. ICE + DTLS handshake loop.
    //
    // One poll_output() call per iteration so no event can be consumed
    // and dropped by a helper that only pattern-matches on Transmit.
    ws_set_read_timeout(&ws, Some(WS_POLL_TIMEOUT))?;

    let mut connected = false;
    let mut udp_buf = [0u8; 2048];

    'handshake: while Instant::now() < startup_deadline {
        match rtc
            .poll_output()
            .map_err(|e| format!("poll_output: {e:?}"))?
        {
            Output::Transmit(t) => {
                if let Err(e) = udp.send_to(&t.contents, t.destination) {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            // Transient — socket send buffer full; packet dropped, not fatal.
                        }
                        _ => return Err(e.into()),
                    }
                }
            }
            Output::Event(Event::Connected) => {
                connected = true;
                break 'handshake;
            }
            Output::Event(_) => {}
            Output::Timeout(_) => {
                // Advance str0m's clock.
                rtc.handle_input(Input::Timeout(Instant::now()))
                    .map_err(|e| format!("handle_input Timeout: {e:?}"))?;

                // Receive one incoming UDP datagram (STUN / DTLS).
                match udp.recv_from(&mut udp_buf) {
                    Ok((n, src)) => {
                        let input = Input::Receive(
                            Instant::now(),
                            Receive::new(Protocol::Udp, src, bound_addr, &udp_buf[..n])
                                .map_err(|e| format!("Receive::new: {e:?}"))?,
                        );
                        rtc.handle_input(input)
                            .map_err(|e| format!("handle_input: {e:?}"))?;
                    }
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => return Err(e.into()),
                }

                // Read WebSocket for trickle ICE candidates from the relay.
                match ws.read() {
                    Ok(tungstenite::Message::Text(txt)) => {
                        if let Ok(msg) = serde_json::from_str::<ServerMsg>(&txt) {
                            if msg.msg_type == "ICE" && !msg.candidate.is_empty() {
                                if let Ok(candidate) = Candidate::from_sdp_string(&msg.candidate) {
                                    rtc.add_remote_candidate(candidate);
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(tungstenite::Error::Io(error))
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(error) => {
                        return Err(format!("relay signaling connection failed: {error}").into());
                    }
                }
            }
        }
    }

    if !connected {
        let _ = send_leave(&mut ws, room_id);
        return Err(format!(
            "ICE/DTLS handshake exceeded the configured {} ms startup deadline",
            startup_timeout.as_millis()
        )
        .into());
    }

    // Publisher I/O remains deadline-bounded for its complete lifetime.
    ws_set_read_timeout(&ws, Some(WS_POLL_TIMEOUT))?;
    ws_set_write_timeout(&ws, Some(WS_WRITE_TIMEOUT))?;

    Ok(HandshakeResult {
        rtc,
        ws,
        mids,
        bound_addr,
        udp,
    })
}

/// Inject stereo Opus fmtp parameters with the music defaults (stereo, 128 kbps).
/// Thin wrapper over [`munge_opus_params`] kept for the existing characterization
/// tests; the default path is byte-identical to the pre-profile CLI.
#[cfg(test)]
fn munge_opus_stereo(sdp: &str) -> String {
    munge_opus_params(sdp, true, 128 * SDP_BITS_PER_KBPS)
}

/// Inject Opus fmtp parameters into an SDP offer string.
///
/// str0m enables Opus but does not expose per-codec fmtp configuration, so we
/// rewrite the generated SDP — the same technique browsers use ("SDP munging").
/// Per RFC 7587 these parameters MUST appear in the `a=fmtp` line: `sprop-stereo`
/// and `stereo` follow the profile's channel layout, and `maxaveragebitrate` is
/// the profile's bitrate hint in bits/s. Missing parameters are added; existing
/// ones are preserved. If the offer has no Opus codec, the SDP is returned
/// unchanged.
fn munge_opus_params(sdp: &str, stereo: bool, max_avg_bitrate_bits: u32) -> String {
    let opus_pt = sdp.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("a=rtpmap:")?;
        let (pt, codec) = rest.split_once(' ')?;
        codec
            .to_ascii_lowercase()
            .starts_with("opus/")
            .then(|| pt.to_string())
    });
    let Some(pt) = opus_pt else {
        return sdp.to_string();
    };

    let stereo_val = if stereo { "1" } else { "0" };
    let bitrate_str = max_avg_bitrate_bits.to_string();
    let wanted = [
        ("minptime", "10"),
        ("useinbandfec", "0"),
        ("sprop-stereo", stereo_val),
        ("stereo", stereo_val),
        ("maxaveragebitrate", bitrate_str.as_str()),
    ];
    let fmtp_prefix = format!("a=fmtp:{pt} ");
    let rtpmap_prefix = format!("a=rtpmap:{pt} ");

    let mut have_fmtp = false;
    let mut out = String::with_capacity(sdp.len() + 80);
    for line in sdp.split_inclusive('\n') {
        let body = line.trim_end_matches(['\r', '\n']);
        let eol = &line[body.len()..];
        if let Some(existing) = body.strip_prefix(&fmtp_prefix) {
            have_fmtp = true;
            out.push_str(&fmtp_prefix);
            out.push_str(&merge_params(existing, &wanted));
            out.push_str(eol);
        } else {
            out.push_str(line);
        }
    }

    if have_fmtp {
        return out;
    }

    // No fmtp line existed — insert one immediately after the Opus rtpmap line.
    let params = wanted
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";");
    let mut rebuilt = String::with_capacity(out.len() + 80);
    for line in out.split_inclusive('\n') {
        rebuilt.push_str(line);
        let body = line.trim_end_matches(['\r', '\n']);
        if body.starts_with(&rtpmap_prefix) {
            let eol = if line.ends_with("\r\n") { "\r\n" } else { "\n" };
            let _ = write!(rebuilt, "{fmtp_prefix}{params}{eol}");
        }
    }
    rebuilt
}

/// Merge `wanted` `key=value` params into a semicolon-list, replacing existing
/// values for those keys. Existing unrelated params are preserved.
fn merge_params(existing: &str, wanted: &[(&str, &str)]) -> String {
    let mut params: Vec<(String, String)> = existing
        .split(';')
        .filter_map(|p| {
            let trimmed = p.trim();
            if trimmed.is_empty() {
                return None;
            }
            let (key, value) = trimmed.split_once('=').unwrap_or((trimmed, ""));
            Some((key.to_owned(), value.to_owned()))
        })
        .collect();
    for (k, v) in wanted {
        match params.iter_mut().find(|(key, _)| key == k) {
            Some((_, value)) => (*v).clone_into(value),
            None => params.push(((*k).to_owned(), (*v).to_owned())),
        }
    }
    params
        .into_iter()
        .map(|(k, v)| if v.is_empty() { k } else { format!("{k}={v}") })
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(test)]
mod sdp_munge_tests {
    use super::munge_opus_stereo;

    #[test]
    fn given_opus_with_existing_fmtp_when_munged_then_stereo_params_added_and_existing_kept() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
                   a=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\n";
        let out = munge_opus_stereo(sdp);
        assert!(
            out.contains("sprop-stereo=1"),
            "must declare sprop-stereo: {out}"
        );
        assert!(out.contains("stereo=1"), "must declare stereo: {out}");
        assert!(
            out.contains("maxaveragebitrate=131072"),
            "must cap bitrate: {out}"
        );
        assert!(
            out.contains("minptime=10"),
            "must keep existing params: {out}"
        );
        assert!(
            out.contains("useinbandfec=0"),
            "must disable FEC for low-latency live receive: {out}"
        );
    }

    #[test]
    fn given_opus_without_fmtp_when_munged_then_fmtp_line_inserted() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n";
        let out = munge_opus_stereo(sdp);
        assert!(
            out.contains("a=fmtp:111 "),
            "must insert an fmtp line: {out}"
        );
        assert!(out.contains("sprop-stereo=1"), "{out}");
    }

    #[test]
    fn given_no_opus_when_munged_then_unchanged() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 8\r\na=rtpmap:8 PCMA/8000\r\n";
        assert_eq!(munge_opus_stereo(sdp), sdp);
    }

    #[test]
    fn given_already_stereo_when_munged_twice_then_idempotent() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
                   a=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10\r\n";
        let once = munge_opus_stereo(sdp);
        let twice = munge_opus_stereo(&once);
        assert_eq!(once, twice, "munge must be idempotent");
        assert_eq!(
            twice.matches("sprop-stereo=1").count(),
            1,
            "no duplicate params: {twice}"
        );
    }
}
