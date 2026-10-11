use std::error::Error;
use std::sync::{mpsc, Arc};

use pocketstation::codec::StreamProfile;

use crate::audio::latency_profile::LatencyProfile;
use crate::audio::opus_worker::{
    spawn_opus_encoder, EncodableFrame, EncodedAudioFrame, EncodedDeliveryPolicy, EncoderCounters,
};
use crate::rtc::handshake::HandshakeResult;
use crate::rtc::publisher::{
    run_publish_loop, PublishStats, PublishStream, PublisherBacklogPolicy,
};

pub(crate) const BUS_MAX_LEN: usize = 64;

pub(crate) struct PreparedFramePublisher {
    pub(crate) transport: PreparedMediaTransport,
    pub(crate) session_id: String,
    pub(crate) streams: Vec<PreparedFrameStream>,
    pub(crate) ingress_recovery: Option<crate::rtc::gateway::PreparedIngressRecovery>,
}

pub(crate) enum PreparedMediaTransport {
    WebRtc(Box<HandshakeResult>),
    WebSocket(Vec<crate::rtc::gateway::IngressConnection>),
}

impl PreparedMediaTransport {
    pub(crate) fn cancel(self, session_id: &str) -> Result<(), Box<dyn Error>> {
        match self {
            Self::WebRtc(handshake) => {
                crate::rtc::handshake::cancel_handshake(*handshake, session_id)
            }
            Self::WebSocket(connections) => crate::rtc::gateway::cancel(connections),
        }
    }
}

pub(crate) struct PreparedFrameStream {
    pub(crate) profile: StreamProfile,
    pub(crate) latency_profile: LatencyProfile,
    pub(crate) counters: Arc<EncoderCounters>,
}

pub(crate) struct FramePublisherExecution {
    pub(crate) result: Result<PublishStats, Box<dyn Error>>,
    pub(crate) ingress_error: Option<Box<dyn Error>>,
    pub(crate) encoder_threads: Vec<std::thread::JoinHandle<()>>,
}

pub(crate) fn run_prepared_frame_publisher<F>(
    prepared: PreparedFramePublisher,
    frame_receivers: Vec<mpsc::Receiver<F>>,
    context: &pocketstation::connector::ConnectorContext,
) -> Result<FramePublisherExecution, Box<dyn Error>>
where
    F: EncodableFrame,
{
    let stream_count = prepared.streams.len();
    if stream_count != frame_receivers.len() {
        return Err("relay publisher media, stream and receiver counts do not match".into());
    }
    let mut encoder_threads = Vec::with_capacity(stream_count);
    let mut encoded_streams = Vec::with_capacity(stream_count);
    for (stream, frame_rx) in prepared.streams.into_iter().zip(frame_receivers) {
        let (encoded_tx, encoded_rx) =
            mpsc::sync_channel::<EncodedAudioFrame>(stream.latency_profile.encoded_channel_depth());
        let encoder = spawn_opus_encoder(
            frame_rx,
            encoded_tx,
            Arc::clone(&stream.counters),
            stream.profile.opus_config(),
            EncodedDeliveryPolicy::for_latency_profile(stream.latency_profile),
        )
        .map_err(|error| format!("encoder thread spawn: {error}"))?;
        encoder_threads.push(encoder);
        encoded_streams.push((stream, encoded_rx));
    }
    let mut ingress_error = None;
    let result = match prepared.transport {
        PreparedMediaTransport::WebRtc(handshake) => {
            let HandshakeResult {
                rtc,
                ws,
                mids,
                bound_addr,
                udp,
            } = *handshake;
            if mids.len() != stream_count {
                return Err("relay media and encoded stream counts do not match".into());
            }
            let streams = mids
                .into_iter()
                .zip(encoded_streams)
                .map(|(mid, (stream, encoded_rx))| PublishStream {
                    mid,
                    encoded_rx,
                    backlog_policy: match stream.latency_profile {
                        LatencyProfile::Low => PublisherBacklogPolicy::DropStale,
                        LatencyProfile::Standard => PublisherBacklogPolicy::Preserve,
                    },
                    counters: stream.counters,
                })
                .collect();
            run_publish_loop(rtc, udp, ws, bound_addr, streams, &prepared.session_id)
        }
        PreparedMediaTransport::WebSocket(connections) => {
            if connections.len() != stream_count {
                return Err("relay ingress and encoded stream counts do not match".into());
            }
            let streams = connections
                .into_iter()
                .zip(encoded_streams)
                .map(
                    |(connection, (stream, encoded_rx))| crate::rtc::gateway::IngressStream {
                        connection,
                        encoded_rx,
                        counters: stream.counters,
                    },
                )
                .collect();
            let outcome = if prepared.ingress_recovery.is_some() {
                crate::rtc::gateway::run_with_recovery(
                    streams,
                    prepared.ingress_recovery,
                    Some(context),
                )
            } else {
                crate::rtc::gateway::run(streams)
            };
            ingress_error = outcome.error;
            Ok(outcome.statistics)
        }
    };
    Ok(FramePublisherExecution {
        result,
        ingress_error,
        encoder_threads,
    })
}

pub(crate) fn validate_bus(bus: &str) -> Result<(), Box<dyn Error>> {
    if bus.is_empty() || bus.len() > BUS_MAX_LEN {
        return Err(format!("bus label must be 1–{BUS_MAX_LEN} characters").into());
    }
    if !bus
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
    {
        return Err(format!(
            "bus label '{bus}' must contain only letters, digits, '.', '_', or '-'"
        )
        .into());
    }
    Ok(())
}
