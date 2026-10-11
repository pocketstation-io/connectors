//! Authenticated bounded Opus ingress into the existing Relay `AudioBus` plane.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tungstenite::Message;
use url::Url;

use super::publisher::{PublishStats, PublishStreamStats};
use super::signaling::{ws_set_read_timeout, ws_set_write_timeout, Ws};
use crate::audio::opus_worker::{EncodedAudioFrame, EncoderCounters};
use crate::configuration::RelayRouteConfiguration;
use crate::publisher_control::{
    PublisherCredential, PublisherLease, ATTACHMENT_TIMEOUT, RECOVERY_ATTEMPTS, RECOVERY_BUDGET,
};

type IngressError = Box<dyn std::error::Error>;

#[derive(Debug, thiserror::Error)]
enum IngressFailure {
    #[error("Relay ingress connection failed; reconnect requires a fresh attachment; direct fallback is disabled")]
    Transport,
    #[error("Relay ingress denied or revoked the source capability")]
    Authority,
    #[error("Relay ingress sent invalid or unexpected protocol data")]
    Protocol,
    #[error("Relay ingress admission identity does not match the selected AudioBus")]
    Identity,
    #[error("Relay ingress READY source generation did not advance")]
    Generation,
    #[error("Relay publisher capability expired, changed or was revoked")]
    Credential,
    #[error("Relay publisher exhausted its bounded reconnect attempts or deadline")]
    RecoveryBudget,
}

pub(crate) struct IngressDeliveryState {
    pub(crate) paused: AtomicBool,
    pub(crate) capture_end_ns: Vec<AtomicU64>,
    pub(crate) input_drops: Vec<AtomicU64>,
}

impl IngressDeliveryState {
    pub(crate) fn new(buses: usize) -> Arc<Self> {
        Arc::new(Self {
            paused: AtomicBool::new(false),
            capture_end_ns: (0..buses).map(|_| AtomicU64::new(0)).collect(),
            input_drops: (0..buses).map(|_| AtomicU64::new(0)).collect(),
        })
    }
}

pub(crate) struct PreparedIngressRecovery {
    pub(crate) control: crate::RelayPublisherControl,
    pub(crate) credential: PublisherCredential,
    pub(crate) configurations: Vec<RelayRouteConfiguration>,
    pub(crate) delivery: Arc<IngressDeliveryState>,
    pub(crate) _lease: PublisherLease,
}

const SUBPROTOCOL: &str = "pks-opus-v1";
const HEADER_BYTES: usize = 40;
const MAX_OPUS_PAYLOAD_BYTES: usize = 1_275;
const DURATION_SAMPLES: u32 = 960;
const MAX_FRAME_AGE: Duration = Duration::from_millis(200);

pub(crate) struct IngressConnection {
    ws: Ws,
    channels: u8,
    pub(crate) source_generation: u64,
}

#[derive(Deserialize)]
struct Ready {
    #[serde(rename = "type")]
    kind: String,
    bus_id: String,
    source_generation: u64,
}

pub(crate) fn prepare(
    configuration: &RelayRouteConfiguration,
    channels: u8,
    deadline: Instant,
) -> Result<IngressConnection, Box<dyn std::error::Error>> {
    let mut url = Url::parse(&configuration.relay_url)?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|()| "Relay ingress scheme is invalid")?;
    url.path_segments_mut()
        .map_err(|()| "Relay ingress URL cannot contain paths")?
        .pop_if_empty()
        .extend(["v1", "sessions", &configuration.relay_session_id, "audio"]);
    url.query_pairs_mut()
        .append_pair("bus", &configuration.bus_id);
    let mut ws = super::proxy::connect(
        url.as_str(),
        configuration.http_proxy.as_ref(),
        Some(configuration.source_token.expose_secret()),
        Some(SUBPROTOCOL),
        deadline,
    )?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or("Relay ingress readiness exceeded its startup deadline")?;
    ws_set_read_timeout(&ws, Some(remaining))?;
    ws_set_write_timeout(&ws, Some(remaining))?;
    let ready =
        super::proxy::before_deadline(super::proxy::clone_socket(&ws)?, deadline, || loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("Relay ingress readiness exceeded its startup deadline")?;
            ws_set_read_timeout(&ws, Some(remaining))?;
            match ws.read().map_err(|error| classify_socket_error(&error))? {
                Message::Text(text) => {
                    let ready: Ready =
                        serde_json::from_str(&text).map_err(|_| IngressFailure::Protocol)?;
                    if ready.kind != "READY"
                        || ready.bus_id != configuration.bus_id
                        || ready.source_generation == 0
                    {
                        return Err(IngressFailure::Identity.into());
                    }
                    return Ok(ready);
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => return Err(IngressFailure::Protocol.into()),
            }
        })?;
    super::proxy::clone_socket(&ws)?.set_nonblocking(true)?;
    Ok(IngressConnection {
        ws,
        channels,
        source_generation: ready.source_generation,
    })
}

pub(crate) fn cancel(
    connections: Vec<IngressConnection>,
) -> Result<(), Box<dyn std::error::Error>> {
    cancel_until(connections, Instant::now() + MAX_FRAME_AGE)
}

fn cancel_until(
    connections: Vec<IngressConnection>,
    deadline: Instant,
) -> Result<(), IngressError> {
    let mut failed = false;
    for mut connection in connections {
        failed |= send_until(&mut connection.ws, Message::Close(None), deadline, None).is_err();
        if let Ok(socket) = super::proxy::clone_socket(&connection.ws) {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
    if failed {
        Err("Relay ingress close failed".into())
    } else {
        Ok(())
    }
}

pub(crate) struct IngressStream {
    pub(crate) connection: IngressConnection,
    pub(crate) encoded_rx: mpsc::Receiver<EncodedAudioFrame>,
    pub(crate) counters: Arc<EncoderCounters>,
}

pub(crate) struct IngressRunResult {
    pub(crate) statistics: PublishStats,
    pub(crate) error: Option<Box<dyn std::error::Error>>,
}

struct ActiveIngress {
    connection: Option<IngressConnection>,
    encoded_rx: mpsc::Receiver<EncodedAudioFrame>,
    counters: Arc<EncoderCounters>,
    statistics: PublishStreamStats,
    disconnected: bool,
    channels: u8,
    highest_generation: u64,
    not_before_capture_ns: Option<u64>,
}

pub(crate) fn run(streams: Vec<IngressStream>) -> IngressRunResult {
    run_with_recovery(streams, None, None)
}

pub(crate) fn run_with_recovery(
    streams: Vec<IngressStream>,
    mut recovery: Option<PreparedIngressRecovery>,
    context: Option<&pocketstation::connector::ConnectorContext>,
) -> IngressRunResult {
    let started = Instant::now();
    let mut active = streams
        .into_iter()
        .map(|stream| ActiveIngress {
            statistics: PublishStreamStats {
                ingress_source_generation: stream.connection.source_generation,
                ingress_attachments: 1,
                ingress_credential_revision: recovery
                    .as_ref()
                    .map_or(0, |value| value.credential.revision),
                ..PublishStreamStats::default()
            },
            channels: stream.connection.channels,
            highest_generation: stream.connection.source_generation,
            connection: Some(stream.connection),
            encoded_rx: stream.encoded_rx,
            counters: stream.counters,
            disconnected: false,
            not_before_capture_ns: None,
        })
        .collect::<Vec<_>>();
    let result = publish_group(&mut active, &mut recovery, context);
    if let Some(recovery) = &recovery {
        recovery.delivery.paused.store(true, Ordering::Release);
        for (index, stream) in active.iter_mut().enumerate() {
            stream.statistics.ingress_recovery_input_drops =
                recovery.delivery.input_drops[index].load(Ordering::Relaxed);
        }
    }
    close_active(&mut active);
    IngressRunResult {
        statistics: PublishStats {
            streams: active.into_iter().map(|stream| stream.statistics).collect(),
            elapsed: started.elapsed(),
            drains: 0,
            writes: 0,
        },
        error: result.err(),
    }
}

fn publish_group(
    active: &mut [ActiveIngress],
    recovery: &mut Option<PreparedIngressRecovery>,
    context: Option<&pocketstation::connector::ConnectorContext>,
) -> Result<(), IngressError> {
    'publish: loop {
        if context.is_some_and(pocketstation::connector::ConnectorContext::is_abort_requested) {
            break;
        }
        if let Some(recovery) = recovery {
            if recovery.control.is_revoked() {
                return Err(IngressFailure::Credential.into());
            }
            if recovery.control.revision() != recovery.credential.revision
                && !reattach(active, recovery, context, Reattachment::Renewal)?
            {
                break;
            }
            if Instant::now() >= recovery.credential.valid_until {
                return Err(IngressFailure::Credential.into());
            }
        }
        let mut progress = false;
        for index in 0..active.len() {
            match poll_stream(&mut active[index], recovery.as_ref()) {
                Ok(polled) => progress |= polled,
                Err(error) => {
                    if let Some(recovery) = recovery {
                        let revision_changed =
                            recovery.control.revision() != recovery.credential.revision;
                        if recoverable(error.as_ref())
                            || (revision_changed && credential_changed(error.as_ref()))
                        {
                            let reason = if revision_changed {
                                Reattachment::Renewal
                            } else {
                                Reattachment::Tunnel
                            };
                            if !reattach(active, recovery, context, reason)? {
                                break 'publish;
                            }
                            continue 'publish;
                        }
                    }
                    return Err(error);
                }
            }
        }
        if active.iter().all(|stream| stream.disconnected) {
            break;
        }
        if !progress {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    Ok(())
}

fn poll_stream(
    stream: &mut ActiveIngress,
    recovery: Option<&PreparedIngressRecovery>,
) -> Result<bool, IngressError> {
    if stream.disconnected {
        return Ok(false);
    }
    let connection = stream.connection.as_mut().ok_or(IngressFailure::Protocol)?;
    match connection.ws.read() {
        Ok(Message::Close(frame)) => {
            return Err(match frame.map(|frame| frame.code) {
                Some(tungstenite::protocol::frame::coding::CloseCode::Policy) => {
                    IngressFailure::Authority.into()
                }
                None
                | Some(
                    tungstenite::protocol::frame::coding::CloseCode::Normal
                    | tungstenite::protocol::frame::coding::CloseCode::Away
                    | tungstenite::protocol::frame::coding::CloseCode::Restart
                    | tungstenite::protocol::frame::coding::CloseCode::Again,
                ) => IngressFailure::Transport.into(),
                _ => IngressFailure::Protocol.into(),
            })
        }
        Ok(Message::Ping(_) | Message::Pong(_)) => {}
        Ok(_) => return Err(IngressFailure::Protocol.into()),
        Err(tungstenite::Error::Io(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        Err(error) => return Err(classify_socket_error(&error)),
    }
    match stream.encoded_rx.try_recv() {
        Ok(frame) => send_frame(stream, &frame, recovery).map(|()| true),
        Err(mpsc::TryRecvError::Disconnected) => {
            stream.disconnected = true;
            Ok(true)
        }
        Err(mpsc::TryRecvError::Empty) => Ok(false),
    }
}

fn send_frame(
    stream: &mut ActiveIngress,
    frame: &EncodedAudioFrame,
    recovery: Option<&PreparedIngressRecovery>,
) -> Result<(), IngressError> {
    if frame
        .output_generation
        .as_ref()
        .is_some_and(|generation| !generation.is_active())
    {
        stream
            .counters
            .cancelled_output_frames
            .fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if stream
        .not_before_capture_ns
        .is_some_and(|cutoff| frame.capture_timestamp_ns < cutoff)
    {
        stream.statistics.ingress_recovery_opus_drops = stream
            .statistics
            .ingress_recovery_opus_drops
            .saturating_add(1);
        return Ok(());
    }
    if frame.wallclock.elapsed() > MAX_FRAME_AGE {
        stream
            .counters
            .publisher_stale_drops
            .fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let connection = stream.connection.as_mut().ok_or(IngressFailure::Protocol)?;
    let packet = encode_packet(frame, connection.channels).map_err(|_| IngressFailure::Protocol)?;
    let deadline = (Instant::now() + MAX_FRAME_AGE).min(
        recovery.map_or(Instant::now() + MAX_FRAME_AGE, |value| {
            value.credential.valid_until
        }),
    );
    if let Err(error) = send_until(
        &mut connection.ws,
        Message::Binary(packet.into()),
        deadline,
        recovery,
    ) {
        stream.statistics.ingress_recovery_opus_drops = stream
            .statistics
            .ingress_recovery_opus_drops
            .saturating_add(1);
        return Err(error);
    }
    stream.statistics.ingress_frames_sent = stream.statistics.ingress_frames_sent.saturating_add(1);
    stream.statistics.ingress_payload_bytes_sent = stream
        .statistics
        .ingress_payload_bytes_sent
        .saturating_add(u64::try_from(frame.payload.len()).unwrap_or(u64::MAX));
    Ok(())
}

#[derive(Clone, Copy)]
enum Reattachment {
    Renewal,
    Tunnel,
}

fn close_active(active: &mut [ActiveIngress]) {
    let connections = active
        .iter_mut()
        .filter_map(|stream| stream.connection.take())
        .collect();
    let _ = cancel(connections);
}

fn reattach(
    active: &mut [ActiveIngress],
    recovery: &mut PreparedIngressRecovery,
    context: Option<&pocketstation::connector::ConnectorContext>,
    reason: Reattachment,
) -> Result<bool, IngressError> {
    let recovery_deadline = Instant::now() + RECOVERY_BUDGET;
    recovery.delivery.paused.store(true, Ordering::Release);
    close_active(active);
    for attempt in 0..RECOVERY_ATTEMPTS {
        let wait_until =
            (Instant::now() + Duration::from_millis(100 << attempt)).min(recovery_deadline);
        while Instant::now() < wait_until {
            if context.is_some_and(pocketstation::connector::ConnectorContext::is_stop_requested) {
                return Ok(false);
            }
            if recovery.control.is_revoked() {
                return Err(IngressFailure::Credential.into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if context.is_some_and(pocketstation::connector::ConnectorContext::is_stop_requested) {
            return Ok(false);
        }
        let credential = recovery.control.snapshot()?;
        let deadline = (Instant::now() + ATTACHMENT_TIMEOUT)
            .min(recovery_deadline)
            .min(credential.valid_until)
            .min(Instant::now() + recovery.configurations[0].startup_timeout);
        if Instant::now() >= deadline {
            return Err(IngressFailure::RecoveryBudget.into());
        }
        for stream in active.iter_mut() {
            stream.statistics.ingress_reconnect_attempts = stream
                .statistics
                .ingress_reconnect_attempts
                .saturating_add(1);
        }
        let mut connections = Vec::with_capacity(active.len());
        let admission = admit_group(
            active,
            recovery,
            context,
            &credential,
            deadline,
            &mut connections,
        );
        if let Err(error) = admission {
            let _ = cancel_until(
                connections,
                (Instant::now() + MAX_FRAME_AGE).min(recovery_deadline),
            );
            if context.is_some_and(pocketstation::connector::ConnectorContext::is_stop_requested) {
                return Ok(false);
            }
            if recovery.control.is_revoked() {
                return Err(IngressFailure::Credential.into());
            }
            if !recoverable(error.as_ref()) && !credential_changed(error.as_ref()) {
                return Err(error);
            }
            if recovery.control.revision() != credential.revision {
                continue;
            }
            if !recoverable(error.as_ref()) {
                return Err(error);
            }
            continue;
        }
        let renewed = credential.revision != recovery.credential.revision
            || matches!(reason, Reattachment::Renewal);
        for (index, (stream, connection)) in active.iter_mut().zip(connections).enumerate() {
            stream.statistics.ingress_source_generation = connection.source_generation;
            stream.statistics.ingress_attachments =
                stream.statistics.ingress_attachments.saturating_add(1);
            stream.statistics.ingress_discontinuities =
                stream.statistics.ingress_discontinuities.saturating_add(1);
            if renewed {
                stream.statistics.ingress_renewals =
                    stream.statistics.ingress_renewals.saturating_add(1);
            } else {
                stream.statistics.ingress_reconnects =
                    stream.statistics.ingress_reconnects.saturating_add(1);
            }
            stream.statistics.ingress_credential_revision = credential.revision;
            stream.not_before_capture_ns =
                Some(recovery.delivery.capture_end_ns[index].load(Ordering::Acquire));
            stream.connection = Some(connection);
        }
        recovery.credential = credential;
        recovery.delivery.paused.store(false, Ordering::Release);
        return Ok(true);
    }
    Err(IngressFailure::RecoveryBudget.into())
}

fn admit_group(
    active: &mut [ActiveIngress],
    recovery: &PreparedIngressRecovery,
    context: Option<&pocketstation::connector::ConnectorContext>,
    credential: &PublisherCredential,
    deadline: Instant,
    connections: &mut Vec<IngressConnection>,
) -> Result<(), IngressError> {
    for (index, configuration) in recovery.configurations.iter().enumerate() {
        if recovery.control.is_revoked() || recovery.control.revision() != credential.revision {
            return Err(IngressFailure::Credential.into());
        }
        if context.is_some_and(pocketstation::connector::ConnectorContext::is_stop_requested) {
            return Err(IngressFailure::Credential.into());
        }
        let mut configuration = configuration.clone();
        configuration.source_token = credential.token.clone();
        let channels = active[index].channels;
        let connection = prepare(&configuration, channels, deadline)?;
        let generation = connection.source_generation;
        connections.push(connection);
        if generation <= active[index].highest_generation {
            return Err(IngressFailure::Generation.into());
        }
        active[index].highest_generation = generation;
        if recovery.control.is_revoked()
            || recovery.control.revision() != credential.revision
            || Instant::now() >= deadline
        {
            return Err(IngressFailure::Credential.into());
        }
    }
    Ok(())
}

fn classify_socket_error(error: &tungstenite::Error) -> IngressError {
    match error {
        tungstenite::Error::Io(_)
        | tungstenite::Error::ConnectionClosed
        | tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => IngressFailure::Transport.into(),
        _ => IngressFailure::Protocol.into(),
    }
}

fn recoverable(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(failure) = error.downcast_ref::<IngressFailure>() {
        return matches!(failure, IngressFailure::Transport);
    }
    if let Some(failure) = error.downcast_ref::<super::proxy::ProxyConnectError>() {
        return matches!(failure, super::proxy::ProxyConnectError::Transport);
    }
    // The existing DNS/CONNECT/absolute-deadline helpers use sanitized text.
    // Protocol and authority refusals above are typed and always terminal.
    error
        .downcast_ref::<crate::RelayPublisherControlError>()
        .is_none()
}

fn credential_changed(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<IngressFailure>()
        .is_some_and(|failure| matches!(failure, IngressFailure::Credential))
}

pub(crate) fn retryability(
    error: &(dyn std::error::Error + 'static),
) -> pocketstation::connector::ConnectorRetryability {
    if recoverable(error) {
        pocketstation::connector::ConnectorRetryability::Retryable
    } else {
        pocketstation::connector::ConnectorRetryability::Never
    }
}

#[cfg(test)]
fn send_before_deadline(ws: &mut Ws, message: Message) -> Result<(), IngressError> {
    send_until(ws, message, Instant::now() + MAX_FRAME_AGE, None)
}

fn send_until(
    ws: &mut Ws,
    message: Message,
    deadline: Instant,
    recovery: Option<&PreparedIngressRecovery>,
) -> Result<(), IngressError> {
    let valid = || {
        recovery.is_none_or(|value| {
            !value.control.is_revoked()
                && value.control.revision() == value.credential.revision
                && Instant::now() < value.credential.valid_until
        })
    };
    if !valid() {
        return Err(IngressFailure::Credential.into());
    }
    if Instant::now() >= deadline {
        return Err("Relay ingress write exceeded its 200 ms deadline".into());
    }
    // Queue exactly once; retry only flush. A failed socket is destroyed before
    // reattachment, so its buffered media can never be replayed on the new one.
    match ws.write(message) {
        Ok(()) => {}
        Err(tungstenite::Error::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(classify_socket_error(&error)),
    }
    loop {
        if !valid() {
            return Err(IngressFailure::Credential.into());
        }
        match ws.flush() {
            Ok(()) => return Ok(()),
            Err(tungstenite::Error::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock =>
            {
                if Instant::now() >= deadline {
                    return Err("Relay ingress write exceeded its 200 ms deadline".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(classify_socket_error(&error)),
        }
    }
}

fn encode_packet(
    frame: &EncodedAudioFrame,
    channels: u8,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !matches!(channels, 1 | 2)
        || frame.duration_samples != DURATION_SAMPLES
        || frame.payload.is_empty()
        || frame.payload.len() > MAX_OPUS_PAYLOAD_BYTES
    {
        return Err(
            "Relay ingress requires mono/stereo 20 ms Opus packets bounded to 1275 bytes".into(),
        );
    }
    let sample_timestamp = frame.rtp_time.numer();
    if !sample_timestamp.is_multiple_of(u64::from(DURATION_SAMPLES)) {
        return Err("Relay ingress Opus sample timestamp is not frame-aligned".into());
    }
    let mut packet = Vec::with_capacity(HEADER_BYTES + frame.payload.len());
    packet.extend_from_slice(b"PKSO");
    packet.extend_from_slice(&[1, channels, 0, 0]);
    packet.extend_from_slice(&(sample_timestamp / u64::from(DURATION_SAMPLES)).to_be_bytes());
    let timestamp_bytes = sample_timestamp.to_be_bytes();
    packet.extend_from_slice(&timestamp_bytes[4..]);
    packet.extend_from_slice(&u16::try_from(DURATION_SAMPLES)?.to_be_bytes());
    packet.extend_from_slice(&u16::try_from(frame.payload.len())?.to_be_bytes());
    packet.extend_from_slice(&frame.capture_timestamp_ns.to_be_bytes());
    packet.extend_from_slice(
        &frame
            .output_generation
            .as_ref()
            .map_or(0, |generation| generation.id().get())
            .to_be_bytes(),
    );
    packet.extend_from_slice(&frame.payload);
    Ok(packet)
}

#[cfg(test)]
#[path = "gateway_test.rs"]
mod socket_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use str0m::media::{Frequency, MediaTime};

    fn frame(timestamp_samples: u64) -> EncodedAudioFrame {
        EncodedAudioFrame {
            payload: vec![0xf8, 0xff, 0xfe],
            wallclock: Instant::now(),
            rtp_time: MediaTime::new(timestamp_samples, Frequency::FORTY_EIGHT_KHZ),
            duration_samples: DURATION_SAMPLES,
            audio_level: None,
            capture_timestamp_ns: 99,
            capture_age_ns: 0,
            output_generation: None,
        }
    }

    #[test]
    fn given_opus_frame_after_gap_when_encoded_then_packet_sequence_sample_time_and_clock_are_preserved(
    ) {
        let packet = encode_packet(&frame(2 * 960), 2).unwrap();
        assert_eq!(&packet[0..8], b"PKSO\x01\x02\0\0");
        assert_eq!(u64::from_be_bytes(packet[8..16].try_into().unwrap()), 2);
        assert_eq!(u32::from_be_bytes(packet[16..20].try_into().unwrap()), 1920);
        assert_eq!(u16::from_be_bytes(packet[20..22].try_into().unwrap()), 960);
        assert_eq!(u16::from_be_bytes(packet[22..24].try_into().unwrap()), 3);
        assert_eq!(u64::from_be_bytes(packet[24..32].try_into().unwrap()), 99);
        assert_eq!(u64::from_be_bytes(packet[32..40].try_into().unwrap()), 0);
        assert_eq!(&packet[40..], &[0xf8, 0xff, 0xfe]);
    }

    #[test]
    fn given_rtp_wrap_when_encoded_then_packet_sequence_remains_monotonic_and_rtp_wraps() {
        let samples = (u64::from(u32::MAX) / 960 + 1) * 960;
        let packet = encode_packet(&frame(samples), 1).unwrap();
        assert_eq!(
            u64::from_be_bytes(packet[8..16].try_into().unwrap()),
            samples / 960
        );
        assert_eq!(&packet[16..20], &samples.to_be_bytes()[4..]);
    }

    #[test]
    fn given_invalid_payload_or_duration_when_encoded_then_ingress_rejects_before_writing() {
        let mut audio = frame(0);
        audio.payload.resize(1276, 0);
        assert!(encode_packet(&audio, 1).is_err());
        audio.payload.clear();
        assert!(encode_packet(&audio, 1).is_err());
        assert!(encode_packet(&frame(1), 1).is_err());
        assert!(encode_packet(&frame(0), 3).is_err());
    }
}
