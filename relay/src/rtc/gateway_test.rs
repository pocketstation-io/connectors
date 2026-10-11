//! Loopback HTTP CONNECT fixtures exercise the real client socket path.

use super::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use pocketstation::connector::ConnectorSecret;
use pocketstation::{AudioInputConfig, SampleFormat, SampleSpec, Session};
use str0m::media::{Frequency, MediaTime};
use tungstenite::handshake::server::{Request, Response};

const SESSION_ID: &str = "socket-test-session";
const CAPABILITY: &str = "socket-test-source-capability";
type ReceivedPackets = Vec<(String, Vec<Vec<u8>>)>;

fn accept_tunnel(listener: &TcpListener) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "client connection did not arrive"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut connect = Vec::new();
    while !connect.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        connect.push(byte[0]);
        assert!(connect.len() < 8192);
    }
    let connect = String::from_utf8(connect).unwrap();
    assert!(connect.starts_with("CONNECT 127.0.0.1:"));
    assert!(!connect.contains(CAPABILITY));
    stream
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .unwrap();
    stream
}

#[allow(clippy::result_large_err)] // tungstenite's required callback error type
fn upgrade(stream: TcpStream, bus: &str) -> tungstenite::WebSocket<TcpStream> {
    tungstenite::accept_hdr(stream, |request: &Request, mut response: Response| {
        assert_eq!(
            request.uri().path(),
            format!("/v1/sessions/{SESSION_ID}/audio")
        );
        assert_eq!(request.uri().query(), Some(format!("bus={bus}").as_str()));
        assert_eq!(
            request.headers()["Authorization"],
            format!("Bearer {CAPABILITY}")
        );
        assert_eq!(request.headers()["Sec-WebSocket-Protocol"], SUBPROTOCOL);
        response
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
        Ok(response)
    })
    .unwrap()
}

fn configuration(origin: &str, bus: &str) -> RelayRouteConfiguration {
    RelayRouteConfiguration::new(
        origin,
        SESSION_ID,
        ConnectorSecret::new(CAPABILITY).unwrap(),
        bus,
    )
    .unwrap()
    .with_websocket_ingress()
    .unwrap()
    .with_http_proxy(ConnectorSecret::new(origin).unwrap())
    .unwrap()
}

fn socket_fixture(
    buses: Vec<(&'static str, u64)>,
) -> (String, std::thread::JoinHandle<ReceivedPackets>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for (bus, generation) in buses {
            let stream = accept_tunnel(&listener);
            workers.push(std::thread::spawn(move || {
                let mut ws = upgrade(stream, bus);
                ws.send(Message::Text(serde_json::json!({"type":"READY", "bus_id":bus, "source_generation":generation}).to_string().into())).unwrap();
                let mut packets = Vec::new();
                loop {
                    match ws.read().unwrap() {
                        Message::Binary(packet) => packets.push(packet.to_vec()),
                        Message::Close(_) => { let _ = ws.flush(); break; }
                        other => panic!("unexpected message: {other:?}"),
                    }
                }
                (bus.to_owned(), packets)
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    });
    (origin, server)
}

fn frame(sequence: u64, payload: u8) -> EncodedAudioFrame {
    EncodedAudioFrame {
        payload: vec![payload],
        wallclock: Instant::now(),
        rtp_time: MediaTime::new(sequence * 960, Frequency::FORTY_EIGHT_KHZ),
        duration_samples: 960,
        audio_level: None,
        capture_timestamp_ns: 1_000 + sequence * 20_000_000,
        capture_age_ns: 0,
        output_generation: None,
    }
}

fn stream(
    connection: IngressConnection,
    frames: Vec<EncodedAudioFrame>,
) -> (IngressStream, Arc<EncoderCounters>) {
    let (sender, receiver) = mpsc::sync_channel(frames.len().max(1));
    for frame in frames {
        sender.send(frame).unwrap();
    }
    drop(sender);
    let counters = Arc::new(EncoderCounters::default());
    (
        IngressStream {
            connection,
            encoded_rx: receiver,
            counters: counters.clone(),
        },
        counters,
    )
}

#[test]
fn given_two_admitted_buses_when_publishing_then_authenticated_frames_and_receipts_remain_separate()
{
    let (origin, server) = socket_fixture(vec![("application", 31), ("microphone", 32)]);
    let deadline = Instant::now() + Duration::from_secs(2);
    let application = prepare(&configuration(&origin, "application"), 2, deadline).unwrap();
    let microphone = prepare(&configuration(&origin, "microphone"), 1, deadline).unwrap();
    let (application, _) = stream(application, vec![frame(0, 11), frame(2, 12)]);
    let (microphone, _) = stream(microphone, vec![frame(0, 21)]);
    let outcome = run(vec![application, microphone]);
    assert!(outcome.error.is_none());
    let receipt = outcome.statistics;
    let received = server.join().unwrap();
    assert_eq!(receipt.streams[0].ingress_source_generation, 31);
    assert_eq!(receipt.streams[1].ingress_source_generation, 32);
    assert_eq!(receipt.streams[0].ingress_frames_sent, 2);
    assert_eq!(receipt.streams[1].ingress_payload_bytes_sent, 1);
    assert_eq!(receipt.streams[0].rtp_sent, 0);
    assert_eq!(received[0].0, "application");
    assert_eq!(received[1].0, "microphone");
    assert_eq!(received[0].1[0][5], 2);
    assert_eq!(received[1].1[0][5], 1);
    assert_eq!(&received[0].1[1][40..], &[12]);
    assert_eq!(&received[1].1[0][40..], &[21]);
    assert_eq!(
        u64::from_be_bytes(received[0].1[1][8..16].try_into().unwrap()),
        2
    );
}

#[test]
fn given_stale_and_cancelled_output_when_publishing_then_only_active_fresh_audio_crosses_the_socket(
) {
    let (origin, server) = socket_fixture(vec![("microphone", 4)]);
    let connection = prepare(
        &configuration(&origin, "microphone"),
        1,
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    let session = Session::builder()
        .sample_spec(SampleSpec::new(48_000, 1, SampleFormat::F32Interleaved))
        .build();
    let input = session
        .audio_input(
            AudioInputConfig::new(
                SampleSpec::new(48_000, 1, SampleFormat::F32Interleaved),
                2,
                960,
            )
            .unwrap(),
        )
        .unwrap();
    let generation = input.begin_output_generation().unwrap();
    let mut cancelled = frame(1, 1);
    cancelled.output_generation = Some(generation.clone());
    generation.cancel();
    let mut stale = frame(2, 2);
    stale.wallclock = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    let (stream, counters) = stream(connection, vec![cancelled, stale, frame(3, 3)]);
    let outcome = run(vec![stream]);
    assert!(outcome.error.is_none());
    let receipt = outcome.statistics;
    let packets = server.join().unwrap();
    assert_eq!(packets[0].1.len(), 1);
    assert_eq!(&packets[0].1[0][40..], &[3]);
    assert_eq!(receipt.streams[0].ingress_frames_sent, 1);
    assert_eq!(counters.cancelled_output_frames.load(Ordering::Relaxed), 1);
    assert_eq!(counters.publisher_stale_drops.load(Ordering::Relaxed), 1);
}

#[test]
fn given_mismatched_admission_when_preparing_then_bus_identity_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut ws = upgrade(accept_tunnel(&listener), "application");
        ws.send(Message::Text(
            r#"{"type":"READY","bus_id":"microphone","source_generation":2}"#.into(),
        ))
        .unwrap();
    });
    let error = prepare(
        &configuration(&origin, "application"),
        2,
        Instant::now() + Duration::from_secs(2),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("identity does not match"));
    assert!(!error.contains(CAPABILITY));
    server.join().unwrap();
}

#[test]
fn given_partial_admission_bytes_when_preparing_then_absolute_startup_deadline_closes_the_connection(
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut ws = upgrade(accept_tunnel(&listener), "application");
        let body = br#"{"type":"READY","bus_id":"application","source_generation":2}"#;
        ws.get_mut()
            .write_all(&[0x81, u8::try_from(body.len()).unwrap()])
            .unwrap();
        for byte in body {
            if ws.get_mut().write_all(&[*byte]).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    });
    let started = Instant::now();
    assert!(prepare(
        &configuration(&origin, "application"),
        2,
        started + Duration::from_millis(300)
    )
    .is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}

#[test]
fn given_remote_close_when_publishing_then_failure_requires_a_fresh_attachment() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut ws = upgrade(accept_tunnel(&listener), "application");
        ws.send(Message::Text(
            r#"{"type":"READY","bus_id":"application","source_generation":2}"#.into(),
        ))
        .unwrap();
        assert!(matches!(ws.read().unwrap(), Message::Binary(_)));
        ws.close(None).unwrap();
    });
    let connection = prepare(
        &configuration(&origin, "application"),
        2,
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    sender.send(frame(0, 42)).unwrap();
    let outcome = run(vec![IngressStream {
        connection,
        encoded_rx: receiver,
        counters: Arc::new(EncoderCounters::default()),
    }]);
    assert_eq!(outcome.statistics.streams[0].ingress_frames_sent, 1);
    assert_eq!(outcome.statistics.streams[0].ingress_source_generation, 2);
    let error = outcome.error.unwrap().to_string();
    assert!(error.contains("fresh attachment"));
    server.join().unwrap();
}

#[test]
fn given_unread_tcp_peer_when_flushing_then_one_queued_frame_has_an_absolute_write_deadline() {
    use tungstenite::protocol::{Role, WebSocketConfig};
    use tungstenite::stream::MaybeTlsStream;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_unread_peer, _) = listener.accept().unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut ws = tungstenite::WebSocket::from_raw_socket(
        MaybeTlsStream::Plain(MaybeTlsStream::Plain(socket)),
        Role::Client,
        Some(
            WebSocketConfig::default()
                .write_buffer_size(0)
                .max_write_buffer_size(4096),
        ),
    );
    let packet = tungstenite::Bytes::from(vec![0_u8; 1275]);
    let started = Instant::now();
    let mut blocked = None;
    for _ in 0..65536 {
        if let Err(error) = send_before_deadline(&mut ws, Message::Binary(packet.clone())) {
            blocked = Some(error);
            break;
        }
    }
    assert!(blocked.unwrap().to_string().contains("200 ms deadline"));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[derive(Clone, Copy)]
enum RecoveryPeer {
    Admit {
        generation: u64,
        reset_after_media: bool,
    },
    Deny,
    ResetBeforeReady,
    Policy {
        generation: u64,
    },
}

#[derive(Debug)]
enum RecoveryObservation {
    Ready(usize, u64),
    Media(usize, u64, u64),
    Closed(usize, u64),
    Denied,
}

#[allow(clippy::result_large_err, clippy::too_many_lines)] // Bounded fault peer; tungstenite callback type.
fn recovery_fixture(
    plan: Vec<(usize, &'static str, RecoveryPeer)>,
) -> (
    String,
    std::thread::JoinHandle<()>,
    mpsc::Receiver<RecoveryObservation>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (observed_tx, observed_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for (index, token, peer) in plan {
            let socket = accept_tunnel(&listener);
            let observed = observed_tx.clone();
            workers.push(std::thread::spawn(move || {
                let bus = ["application", "microphone"][index];
                let admitted =
                    tungstenite::accept_hdr(socket, |request: &Request, mut response: Response| {
                        assert_eq!(
                            request.headers()["Authorization"],
                            format!("Bearer {token}")
                        );
                        assert_eq!(request.uri().query(), Some(format!("bus={bus}").as_str()));
                        if matches!(peer, RecoveryPeer::Deny) {
                            let denied = tungstenite::http::Response::builder()
                                .status(403)
                                .body(None)
                                .unwrap();
                            return Err(denied);
                        }
                        response
                            .headers_mut()
                            .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
                        Ok(response)
                    });
                if matches!(peer, RecoveryPeer::Deny) {
                    assert!(admitted.is_err());
                    observed.send(RecoveryObservation::Denied).unwrap();
                    return;
                }
                let mut ws = admitted.unwrap();
                let (generation, reset_after_media, policy) = match peer {
                    RecoveryPeer::Admit {
                        generation,
                        reset_after_media,
                    } => (generation, reset_after_media, false),
                    RecoveryPeer::Policy { generation } => (generation, false, true),
                    RecoveryPeer::ResetBeforeReady => {
                        ws.get_mut().shutdown(std::net::Shutdown::Both).unwrap();
                        return;
                    }
                    RecoveryPeer::Deny => unreachable!(),
                };
                ws.send(Message::Text(
                    serde_json::json!({"type":"READY","bus_id":bus,"source_generation":generation})
                        .to_string()
                        .into(),
                ))
                .unwrap();
                observed
                    .send(RecoveryObservation::Ready(index, generation))
                    .unwrap();
                if policy {
                    ws.send(Message::Close(Some(tungstenite::protocol::CloseFrame {
                        code: tungstenite::protocol::frame::coding::CloseCode::Policy,
                        reason: "capability revoked".into(),
                    })))
                    .unwrap();
                }
                loop {
                    match ws.read() {
                        Ok(Message::Binary(packet)) => {
                            let timestamp = u64::from_be_bytes(packet[24..32].try_into().unwrap());
                            observed
                                .send(RecoveryObservation::Media(index, generation, timestamp))
                                .unwrap();
                            if reset_after_media {
                                ws.get_mut().shutdown(std::net::Shutdown::Both).unwrap();
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => {
                            let _ = ws.flush();
                            break;
                        }
                        Err(tungstenite::Error::Io(error))
                            if error.kind() != std::io::ErrorKind::TimedOut =>
                        {
                            break
                        }
                        Err(
                            tungstenite::Error::Protocol(_) | tungstenite::Error::ConnectionClosed,
                        ) => break,
                        other => panic!("unexpected recovery fixture event: {other:?}"),
                    }
                }
                observed
                    .send(RecoveryObservation::Closed(index, generation))
                    .unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    (origin, server, observed_rx)
}

fn controlled(
    origin: &str,
    buses: usize,
    expiry: Instant,
) -> (
    Vec<IngressStream>,
    PreparedIngressRecovery,
    Vec<mpsc::SyncSender<EncodedAudioFrame>>,
) {
    let configurations = (0..buses)
        .map(|index| configuration(origin, ["application", "microphone"][index]))
        .collect::<Vec<_>>();
    let control = crate::RelayPublisherControl::new(&configurations[0], expiry).unwrap();
    let lease = control.claim().unwrap();
    let mut senders = Vec::new();
    let streams = configurations
        .iter()
        .map(|configuration| {
            let connection =
                prepare(configuration, 1, Instant::now() + Duration::from_secs(1)).unwrap();
            let (sender, encoded_rx) = mpsc::sync_channel(8);
            senders.push(sender);
            IngressStream {
                connection,
                encoded_rx,
                counters: Arc::new(EncoderCounters::default()),
            }
        })
        .collect();
    let recovery = PreparedIngressRecovery {
        credential: control.snapshot().unwrap(),
        control,
        configurations,
        delivery: IngressDeliveryState::new(buses),
        _lease: lease,
    };
    (streams, recovery, senders)
}

fn wait_media(observed: &mpsc::Receiver<RecoveryObservation>, generation: u64, buses: usize) {
    let mut seen = [false; 2];
    let deadline = Instant::now() + Duration::from_secs(3);
    while seen[..buses].iter().any(|seen| !seen) {
        match observed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            RecoveryObservation::Media(index, actual, timestamp) if actual == generation => {
                assert!(timestamp > 0);
                seen[index] = true;
            }
            RecoveryObservation::Ready(index, actual)
            | RecoveryObservation::Closed(index, actual) => {
                assert!(index < buses && actual > 0);
            }
            _ => {}
        }
    }
}

#[test]
fn given_fresh_revision_when_reconnecting_then_both_buses_advance_and_queued_old_audio_is_discarded(
) {
    let admit = |generation| RecoveryPeer::Admit {
        generation,
        reset_after_media: false,
    };
    let (origin, server, observed) = recovery_fixture(vec![
        (0, CAPABILITY, admit(1)),
        (1, CAPABILITY, admit(1)),
        (0, "replacement-capability", admit(2)),
        (1, "replacement-capability", admit(2)),
    ]);
    let expiry = Instant::now() + Duration::from_secs(5);
    let (streams, recovery, senders) = controlled(&origin, 2, expiry);
    let control = recovery.control.clone();
    let delivery = recovery.delivery.clone();
    let publisher = std::thread::spawn(move || run_test_recovery(streams, recovery));
    for sender in &senders {
        sender.send(frame(0, 11)).unwrap();
    }
    wait_media(&observed, 1, 2);
    for timestamp in &delivery.capture_end_ns {
        timestamp.store(100_000_000, Ordering::Release);
    }
    control
        .replace(
            2,
            ConnectorSecret::new("replacement-capability").unwrap(),
            expiry + Duration::from_secs(2),
        )
        .unwrap();
    for sender in &senders {
        sender.send(frame(1, 12)).unwrap();
    }
    while delivery.paused.load(Ordering::Acquire) || control.revision() == 1 {
        std::thread::sleep(Duration::from_millis(1));
    }
    // First observe both actual new READY messages, then send a later source range.
    let mut admitted = [false; 2];
    let deadline = Instant::now() + Duration::from_secs(3);
    while admitted.iter().any(|value| !value) {
        if let RecoveryObservation::Ready(index, 2) = observed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            admitted[index] = true;
        }
    }
    for sender in &senders {
        sender.send(frame(6, 13)).unwrap();
    }
    wait_media(&observed, 2, 2);
    drop(senders);
    let outcome = publisher.join().unwrap();
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    for receipt in outcome.statistics.streams {
        assert_eq!(receipt.ingress_source_generation, 2);
        assert_eq!(receipt.ingress_attachments, 2);
        assert_eq!(receipt.ingress_renewals, 1);
        assert_eq!(receipt.ingress_discontinuities, 1);
        assert_eq!(receipt.ingress_credential_revision, 2);
        assert_eq!(receipt.ingress_recovery_opus_drops, 1);
        assert_eq!(receipt.ingress_frames_sent, 2);
    }
    server.join().unwrap();
}

#[test]
fn given_one_lost_tunnel_when_recovering_then_whole_group_readmits_with_new_generations() {
    let admit = |generation, reset_after_media| RecoveryPeer::Admit {
        generation,
        reset_after_media,
    };
    let (origin, server, observed) = recovery_fixture(vec![
        (0, CAPABILITY, admit(1, true)),
        (1, CAPABILITY, admit(1, false)),
        (0, CAPABILITY, admit(2, false)),
        (1, CAPABILITY, admit(2, false)),
    ]);
    let (streams, recovery, senders) =
        controlled(&origin, 2, Instant::now() + Duration::from_secs(5));
    let publisher = std::thread::spawn(move || run_test_recovery(streams, recovery));
    for sender in &senders {
        sender.send(frame(0, 11)).unwrap();
    }
    let mut admitted = [false; 2];
    let deadline = Instant::now() + Duration::from_secs(3);
    while admitted.iter().any(|value| !value) {
        if let RecoveryObservation::Ready(index, 2) = observed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            admitted[index] = true;
        }
    }
    for sender in &senders {
        sender.send(frame(1, 12)).unwrap();
    }
    wait_media(&observed, 2, 2);
    drop(senders);
    let outcome = publisher.join().unwrap();
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    for receipt in outcome.statistics.streams {
        assert_eq!(receipt.ingress_reconnects, 1);
        assert_eq!(receipt.ingress_reconnect_attempts, 1);
        assert_eq!(receipt.ingress_attachments, 2);
        assert_eq!(receipt.ingress_credential_revision, 1);
    }
    server.join().unwrap();
}

#[test]
fn given_partial_recovery_denial_when_admitting_then_first_bus_rolls_back_without_media_or_retry() {
    let admit = |generation| RecoveryPeer::Admit {
        generation,
        reset_after_media: false,
    };
    let (origin, server, observed) = recovery_fixture(vec![
        (0, CAPABILITY, admit(1)),
        (1, CAPABILITY, admit(1)),
        (0, "replacement-capability", admit(2)),
        (1, "replacement-capability", RecoveryPeer::Deny),
    ]);
    let expiry = Instant::now() + Duration::from_secs(5);
    let (streams, recovery, senders) = controlled(&origin, 2, expiry);
    let control = recovery.control.clone();
    let publisher = std::thread::spawn(move || run_test_recovery(streams, recovery));
    control
        .replace(
            2,
            ConnectorSecret::new("replacement-capability").unwrap(),
            expiry + Duration::from_secs(1),
        )
        .unwrap();
    for sender in &senders {
        sender.send(frame(1, 9)).unwrap();
    }
    let outcome = publisher.join().unwrap();
    assert!(outcome.error.unwrap().contains("denied"));
    for receipt in outcome.statistics.streams {
        assert_eq!(receipt.ingress_frames_sent, 0);
        assert_eq!(receipt.ingress_attachments, 1);
        assert_eq!(receipt.ingress_reconnect_attempts, 1);
    }
    drop(senders);
    server.join().unwrap();
    while let Ok(event) = observed.try_recv() {
        assert!(!matches!(event, RecoveryObservation::Media(_, _, _)));
    }
}

#[test]
fn given_expiry_or_terminal_revoke_when_idle_then_group_closes_without_reconnecting() {
    for revoke in [false, true] {
        let (origin, server, _observed) = recovery_fixture(vec![(
            0,
            CAPABILITY,
            RecoveryPeer::Admit {
                generation: 1,
                reset_after_media: false,
            },
        )]);
        let (streams, recovery, senders) =
            controlled(&origin, 1, Instant::now() + Duration::from_millis(150));
        if revoke {
            recovery.control.revoke();
        }
        let started = Instant::now();
        let outcome = run_with_recovery(streams, Some(recovery), None);
        assert!(outcome.error.unwrap().to_string().contains("expired"));
        assert_eq!(outcome.statistics.streams[0].ingress_reconnect_attempts, 0);
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(senders);
        server.join().unwrap();
    }
}

struct TestRecoveryResult {
    statistics: PublishStats,
    error: Option<String>,
}

fn run_test_recovery(
    streams: Vec<IngressStream>,
    recovery: PreparedIngressRecovery,
) -> TestRecoveryResult {
    let result = run_with_recovery(streams, Some(recovery), None);
    TestRecoveryResult {
        statistics: result.statistics,
        error: result.error.map(|error| error.to_string()),
    }
}

#[test]
fn given_partial_transport_rollback_when_ready_reuses_observed_generation_then_group_is_terminal() {
    let admit = |generation| RecoveryPeer::Admit {
        generation,
        reset_after_media: false,
    };
    let (origin, server, observed) = recovery_fixture(vec![
        (0, CAPABILITY, admit(1)),
        (1, CAPABILITY, admit(1)),
        (0, "replacement-capability", admit(2)),
        (1, "replacement-capability", RecoveryPeer::ResetBeforeReady),
        (0, "replacement-capability", admit(2)),
    ]);
    let expiry = Instant::now() + Duration::from_secs(5);
    let (streams, recovery, senders) = controlled(&origin, 2, expiry);
    recovery
        .control
        .replace(
            2,
            ConnectorSecret::new("replacement-capability").unwrap(),
            expiry + Duration::from_secs(1),
        )
        .unwrap();
    let outcome = run_with_recovery(streams, Some(recovery), None);
    let error = outcome.error.unwrap();
    assert!(error.to_string().contains("did not advance"));
    assert_eq!(
        retryability(error.as_ref()),
        pocketstation::connector::ConnectorRetryability::Never
    );
    for receipt in outcome.statistics.streams {
        assert_eq!(receipt.ingress_frames_sent, 0);
        assert_eq!(receipt.ingress_attachments, 1);
        assert_eq!(receipt.ingress_reconnect_attempts, 2);
    }
    drop(senders);
    server.join().unwrap();
    while let Ok(event) = observed.try_recv() {
        assert!(!matches!(event, RecoveryObservation::Media(_, _, _)));
    }
}

#[test]
fn given_repeated_transport_loss_when_admitting_then_recovery_has_three_attempts_and_finite_budget()
{
    let (origin, server, _observed) = recovery_fixture(vec![
        (
            0,
            CAPABILITY,
            RecoveryPeer::Admit {
                generation: 1,
                reset_after_media: false,
            },
        ),
        (0, "replacement-capability", RecoveryPeer::ResetBeforeReady),
        (0, "replacement-capability", RecoveryPeer::ResetBeforeReady),
        (0, "replacement-capability", RecoveryPeer::ResetBeforeReady),
    ]);
    let expiry = Instant::now() + Duration::from_secs(5);
    let (streams, recovery, senders) = controlled(&origin, 1, expiry);
    recovery
        .control
        .replace(
            2,
            ConnectorSecret::new("replacement-capability").unwrap(),
            expiry + Duration::from_secs(1),
        )
        .unwrap();
    let started = Instant::now();
    let outcome = run_with_recovery(streams, Some(recovery), None);
    let error = outcome.error.unwrap();
    assert!(error.to_string().contains("exhausted"));
    assert_eq!(
        retryability(error.as_ref()),
        pocketstation::connector::ConnectorRetryability::Never
    );
    assert_eq!(outcome.statistics.streams[0].ingress_reconnect_attempts, 3);
    assert_eq!(outcome.statistics.streams[0].ingress_attachments, 1);
    assert!(started.elapsed() < RECOVERY_BUDGET);
    drop(senders);
    server.join().unwrap();
}

#[test]
fn given_policy_close_when_reading_idle_stream_then_group_is_terminal_without_retry() {
    let (origin, server, _observed) = recovery_fixture(vec![(
        0,
        CAPABILITY,
        RecoveryPeer::Policy { generation: 1 },
    )]);
    let (streams, recovery, senders) =
        controlled(&origin, 1, Instant::now() + Duration::from_secs(5));
    let outcome = run_with_recovery(streams, Some(recovery), None);
    let error = outcome.error.unwrap();
    assert!(error.to_string().contains("revoked"));
    assert_eq!(
        retryability(error.as_ref()),
        pocketstation::connector::ConnectorRetryability::Never
    );
    assert_eq!(outcome.statistics.streams[0].ingress_reconnect_attempts, 0);
    drop(senders);
    server.join().unwrap();
}
