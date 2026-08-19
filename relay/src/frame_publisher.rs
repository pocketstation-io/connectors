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
    pub(crate) handshake: HandshakeResult,
    pub(crate) session_id: String,
    pub(crate) streams: Vec<PreparedFrameStream>,
}

pub(crate) struct PreparedFrameStream {
    pub(crate) profile: StreamProfile,
    pub(crate) latency_profile: LatencyProfile,
    pub(crate) counters: Arc<EncoderCounters>,
}

pub(crate) struct FramePublisherExecution {
    pub(crate) result: Result<PublishStats, Box<dyn Error>>,
    pub(crate) encoder_threads: Vec<std::thread::JoinHandle<()>>,
}

pub(crate) fn run_prepared_frame_publisher<F>(
    prepared: PreparedFramePublisher,
    frame_receivers: Vec<mpsc::Receiver<F>>,
) -> Result<FramePublisherExecution, Box<dyn Error>>
where
    F: EncodableFrame,
{
    let HandshakeResult {
        rtc,
        ws,
        mids,
        bound_addr,
        udp,
    } = prepared.handshake;
    if mids.len() != prepared.streams.len() || mids.len() != frame_receivers.len() {
        return Err("relay publisher media, stream and receiver counts do not match".into());
    }
    let mut encoder_threads = Vec::with_capacity(mids.len());
    let mut publish_streams = Vec::with_capacity(mids.len());
    for ((mid, stream), frame_rx) in mids.into_iter().zip(prepared.streams).zip(frame_receivers) {
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
        publish_streams.push(PublishStream {
            mid,
            encoded_rx,
            backlog_policy: match stream.latency_profile {
                LatencyProfile::Low => PublisherBacklogPolicy::DropStale,
                LatencyProfile::Standard => PublisherBacklogPolicy::Preserve,
            },
            counters: stream.counters,
        });
    }
    let result = run_publish_loop(
        rtc,
        udp,
        ws,
        bound_addr,
        publish_streams,
        &prepared.session_id,
    );
    Ok(FramePublisherExecution {
        result,
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
