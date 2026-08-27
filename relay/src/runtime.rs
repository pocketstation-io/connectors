use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use pocketstation::codec::StreamProfile;
use pocketstation::connector::{
    ConnectorContext, ConnectorError, ConnectorErrorCode, ConnectorErrorStage, ConnectorFactory,
    ConnectorRetryability, ConnectorRunOutcome, ConnectorWorker,
};
use pocketstation::graph::NodeConfig;
use pocketstation::{
    EdgeObservations, EndpointAudioFrame, EndpointAudioReceiver, EndpointId, EndpointPortInput,
    EndpointPreparationGroup, EndpointReceiver, RouteId, SampleFormat, SampleSpec,
};

use crate::audio::latency_profile::LatencyProfile;
use crate::audio::opus_worker::EncoderCounters;
use crate::configuration::{RelayLatencyProfile, RelayRouteConfiguration};
use crate::frame_publisher::{
    run_prepared_frame_publisher, PreparedFramePublisher, PreparedFrameStream,
};
use crate::rtc::handshake::{cancel_handshake, run_handshake, PublishMedia};

const FEEDER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const MAX_GROUPED_BUSES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelayPublishReceiptKey {
    pub endpoint_id: EndpointId,
    pub route_id: RouteId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayPublishStatistics {
    pub rtp_packets_sent_total: u64,
    pub rtp_payload_bytes_sent_total: u64,
    pub rtc_poll_drains_total: u64,
    pub rtc_writer_writes_total: u64,
    pub elapsed_ms: u64,
    pub opus_packets_encoded_total: u64,
    pub opus_encode_errors_total: u64,
    pub encoder_channel_drops_total: u64,
    pub publisher_stale_drops_total: u64,
    pub ingress_queue_drops_total: u64,
    pub cancelled_output_frames_total: u64,
    pub cancelled_output_samples_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayPublishResult {
    pub key: RelayPublishReceiptKey,
    pub statistics: RelayPublishStatistics,
    pub edge_observations: EdgeObservations,
    pub error: Option<ConnectorError>,
}

#[derive(Clone)]
pub struct RelayPublishReceipt {
    state: Arc<RelayPublishReceiptState>,
}

impl RelayPublishReceipt {
    #[must_use]
    pub fn result(&self) -> Option<&RelayPublishResult> {
        self.state.result.get()
    }
}

struct RelayPublishReceiptState {
    ingress_queue_drops_total: AtomicU64,
    result: OnceLock<RelayPublishResult>,
}

impl RelayPublishReceiptState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ingress_queue_drops_total: AtomicU64::new(0),
            result: OnceLock::new(),
        })
    }
}

pub(crate) struct RelayConnectorFactory {
    transport: Arc<dyn RelayPublishTransport>,
    receipts: Arc<Mutex<HashMap<RelayPublishReceiptKey, RelayPublishReceipt>>>,
    receipt_capacity: usize,
}

impl RelayConnectorFactory {
    pub(crate) fn new(receipt_capacity: usize) -> Result<Self, ConnectorError> {
        Self::with_transport(Arc::new(NativeRelayPublishTransport), receipt_capacity)
    }

    fn with_transport(
        transport: Arc<dyn RelayPublishTransport>,
        receipt_capacity: usize,
    ) -> Result<Self, ConnectorError> {
        if receipt_capacity == 0 {
            return Err(relay_error(
                "relay.invalid_receipt_capacity",
                ConnectorErrorStage::Configuration,
                ConnectorRetryability::RetryAfterReconfiguration,
                "relay receipt capacity must be finite and non-zero",
            ));
        }
        Ok(Self {
            transport,
            receipts: Arc::new(Mutex::new(HashMap::with_capacity(receipt_capacity))),
            receipt_capacity,
        })
    }

    pub(crate) fn take_result(&self, key: RelayPublishReceiptKey) -> Option<RelayPublishResult> {
        let mut receipts = self.receipts.lock().ok()?;
        let result = receipts.get(&key)?.result()?.clone();
        receipts.remove(&key);
        Some(result)
    }

    pub(crate) fn receipt(&self, key: RelayPublishReceiptKey) -> Option<RelayPublishReceipt> {
        self.receipts
            .lock()
            .ok()
            .and_then(|receipts| receipts.get(&key).cloned())
    }
}

impl ConnectorFactory for RelayConnectorFactory {
    fn preparation_group(
        &self,
        _route_id: RouteId,
        configuration: &NodeConfig,
    ) -> Result<EndpointPreparationGroup, ConnectorError> {
        let configuration =
            RelayRouteConfiguration::from_node_config(configuration).map_err(|error| {
                relay_error(
                    "relay.invalid_configuration",
                    ConnectorErrorStage::Configuration,
                    ConnectorRetryability::RetryAfterReconfiguration,
                    error.to_string(),
                )
            })?;
        Ok(EndpointPreparationGroup::Shared(
            pocketstation::EndpointGroupId::new(configuration.publisher_group_id()),
        ))
    }

    fn prepare(
        &self,
        inputs: Vec<EndpointPortInput>,
    ) -> Result<Box<dyn ConnectorWorker>, ConnectorError> {
        if inputs.is_empty() || inputs.len() > MAX_GROUPED_BUSES {
            return Err(relay_error(
                "relay.invalid_bus_count",
                ConnectorErrorStage::Prepare,
                ConnectorRetryability::RetryAfterReconfiguration,
                "relay publisher requires between 1 and 16 grouped AudioBus inputs",
            ));
        }

        let mut routes = Vec::with_capacity(inputs.len());
        let mut transport_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let context = input.context();
            let key = RelayPublishReceiptKey {
                endpoint_id: context.endpoint_id(),
                route_id: context.route_context().route_id(),
            };
            let configuration = RelayRouteConfiguration::from_node_config(
                context.node_configuration(),
            )
            .map_err(|error| {
                relay_error(
                    "relay.invalid_configuration",
                    ConnectorErrorStage::Configuration,
                    ConnectorRetryability::RetryAfterReconfiguration,
                    error.to_string(),
                )
            })?;
            let (receiver, _) = input.into_parts();
            let EndpointReceiver::Audio {
                receiver,
                sample_spec,
            } = receiver
            else {
                return Err(relay_error(
                    "relay.audio_input_required",
                    ConnectorErrorStage::Prepare,
                    ConnectorRetryability::RetryAfterReconfiguration,
                    "relay publisher requires audio inputs",
                ));
            };
            let state = RelayPublishReceiptState::new();
            transport_inputs.push(RelayTransportPreparation {
                configuration,
                sample_spec,
            });
            routes.push(PreparedRelayRoute {
                key,
                receiver,
                state,
            });
        }

        validate_grouped_routes(&transport_inputs)?;
        let route_keys = routes.iter().map(|route| route.key).collect::<Vec<_>>();
        {
            let mut receipts = self.receipts.lock().map_err(|_| {
                relay_error(
                    "relay.receipt_registry_unavailable",
                    ConnectorErrorStage::Prepare,
                    ConnectorRetryability::Never,
                    "relay receipt registry is unavailable",
                )
            })?;
            if receipts.len().saturating_add(routes.len()) > self.receipt_capacity {
                return Err(relay_error(
                    "relay.receipt_capacity_exhausted",
                    ConnectorErrorStage::Prepare,
                    ConnectorRetryability::Retryable,
                    "relay receipt capacity is exhausted",
                ));
            }
            if route_keys.iter().any(|key| receipts.contains_key(key)) {
                return Err(relay_error(
                    "relay.duplicate_receipt",
                    ConnectorErrorStage::Prepare,
                    ConnectorRetryability::Never,
                    "relay receipt already exists for a grouped endpoint route",
                ));
            }
            for route in &routes {
                receipts.insert(
                    route.key,
                    RelayPublishReceipt {
                        state: Arc::clone(&route.state),
                    },
                );
            }
        }

        let prepared = match self.transport.prepare(transport_inputs) {
            Ok(prepared) => prepared,
            Err(error) => {
                remove_receipts(&self.receipts, &route_keys);
                return Err(error);
            }
        };
        Ok(Box::new(RelayPublishWorker {
            routes: Some(routes),
            prepared: Some(prepared),
            receipts: Arc::clone(&self.receipts),
        }))
    }
}

fn validate_grouped_routes(routes: &[RelayTransportPreparation]) -> Result<(), ConnectorError> {
    let first = &routes[0].configuration;
    let mut buses = HashSet::with_capacity(routes.len());
    for route in routes {
        if !first.same_publisher(&route.configuration) {
            return Err(relay_error(
                "relay.publisher_group_mismatch",
                ConnectorErrorStage::Prepare,
                ConnectorRetryability::RetryAfterReconfiguration,
                "grouped relay routes must share one Relay origin, Session, credential, publisher group, ICE configuration and startup deadline",
            ));
        }
        if !buses.insert(route.configuration.bus_id.as_str()) {
            return Err(relay_error(
                "relay.duplicate_bus",
                ConnectorErrorStage::Prepare,
                ConnectorRetryability::RetryAfterReconfiguration,
                "grouped relay routes contain a duplicate AudioBus identifier",
            ));
        }
    }
    Ok(())
}

fn remove_receipts(
    receipts: &Mutex<HashMap<RelayPublishReceiptKey, RelayPublishReceipt>>,
    keys: &[RelayPublishReceiptKey],
) {
    if let Ok(mut receipts) = receipts.lock() {
        for key in keys {
            receipts.remove(key);
        }
    }
}

struct RelayTransportPreparation {
    configuration: RelayRouteConfiguration,
    sample_spec: SampleSpec,
}

trait RelayPublishTransport: Send + Sync {
    fn prepare(
        &self,
        routes: Vec<RelayTransportPreparation>,
    ) -> Result<Box<dyn PreparedRelayPublishTransport>, ConnectorError>;
}

trait PreparedRelayPublishTransport: Send {
    fn run(
        self: Box<Self>,
        routes: Vec<PreparedRelayRoute>,
        context: ConnectorContext,
    ) -> Vec<RelayTransportOutcome>;

    fn cancel(self: Box<Self>) -> Result<(), ConnectorError>;
}

struct NativeRelayPublishTransport;

struct NativePreparedRelayPublishTransport {
    publisher: PreparedFramePublisher,
}

impl RelayPublishTransport for NativeRelayPublishTransport {
    fn prepare(
        &self,
        routes: Vec<RelayTransportPreparation>,
    ) -> Result<Box<dyn PreparedRelayPublishTransport>, ConnectorError> {
        let configuration = routes
            .first()
            .ok_or_else(|| {
                relay_error(
                    "relay.empty_group",
                    ConnectorErrorStage::Prepare,
                    ConnectorRetryability::Never,
                    "relay route group is empty",
                )
            })?
            .configuration
            .clone();
        let mut streams = Vec::with_capacity(routes.len());
        let mut media = Vec::with_capacity(routes.len());
        for route in routes {
            let stream_profile = relay_stream_profile(route.sample_spec)?;
            media.push(PublishMedia {
                stereo: stream_profile.is_stereo(),
                max_avg_bitrate_kbps: stream_profile.bitrate_kbps(),
                stream_id: route.configuration.bus_id,
            });
            streams.push(PreparedFrameStream {
                profile: stream_profile,
                latency_profile: match route.configuration.latency_profile {
                    RelayLatencyProfile::Standard => LatencyProfile::Standard,
                    RelayLatencyProfile::Low => LatencyProfile::Low,
                },
                counters: Arc::new(EncoderCounters::default()),
            });
        }
        let handshake = run_handshake(
            &configuration.relay_url,
            &configuration.relay_session_id,
            configuration.source_token.expose_secret(),
            &media,
            &configuration.ice_servers,
            configuration.startup_timeout,
        )
        .map_err(|error| {
            relay_error(
                "relay.handshake_failed",
                ConnectorErrorStage::Startup,
                ConnectorRetryability::Retryable,
                format!("relay handshake failed: {error}"),
            )
        })?;
        Ok(Box::new(NativePreparedRelayPublishTransport {
            publisher: PreparedFramePublisher {
                handshake,
                session_id: configuration.relay_session_id,
                streams,
            },
        }))
    }
}

fn relay_stream_profile(sample_spec: SampleSpec) -> Result<StreamProfile, ConnectorError> {
    if sample_spec.sample_rate_hz != 48_000 {
        return Err(relay_error(
            "relay.unsupported_sample_rate",
            ConnectorErrorStage::Prepare,
            ConnectorRetryability::RetryAfterReconfiguration,
            format!(
                "relay input sample rate is {} Hz; expected 48000 Hz",
                sample_spec.sample_rate_hz
            ),
        ));
    }
    if sample_spec.format != SampleFormat::F32Interleaved {
        return Err(relay_error(
            "relay.unsupported_sample_format",
            ConnectorErrorStage::Prepare,
            ConnectorRetryability::RetryAfterReconfiguration,
            "relay input must use interleaved f32 PCM",
        ));
    }
    match sample_spec.channels {
        1 => Ok(StreamProfile::VoiceMono20ms),
        2 => Ok(StreamProfile::MusicStereo20ms),
        channels => Err(relay_error(
            "relay.unsupported_channel_count",
            ConnectorErrorStage::Prepare,
            ConnectorRetryability::RetryAfterReconfiguration,
            format!("relay input has unsupported channel count {channels}"),
        )),
    }
}

impl PreparedRelayPublishTransport for NativePreparedRelayPublishTransport {
    #[allow(clippy::too_many_lines)]
    fn run(
        self: Box<Self>,
        routes: Vec<PreparedRelayRoute>,
        context: ConnectorContext,
    ) -> Vec<RelayTransportOutcome> {
        let publisher = self.publisher;
        if publisher.streams.len() != routes.len() {
            let error = relay_error(
                "relay.prepared_stream_mismatch",
                ConnectorErrorStage::Delivery,
                ConnectorRetryability::Never,
                "relay prepared stream count does not match grouped routes",
            );
            return failed_outcomes(&routes, &error);
        }

        let counters = publisher
            .streams
            .iter()
            .map(|stream| Arc::clone(&stream.counters))
            .collect::<Vec<_>>();
        let route_states = routes
            .iter()
            .map(|route| Arc::clone(&route.state))
            .collect::<Vec<_>>();
        let transport_stop = Arc::new(AtomicBool::new(false));
        let mut frame_receivers = Vec::with_capacity(routes.len());
        let mut feeders = Vec::with_capacity(routes.len());

        for (index, (route, stream)) in routes.into_iter().zip(publisher.streams.iter()).enumerate()
        {
            let (frame_tx, frame_rx) = mpsc::sync_channel::<EndpointAudioFrame>(
                stream.latency_profile.capture_channel_depth(),
            );
            let feeder_stop = Arc::clone(&transport_stop);
            let feeder_context = context.clone();
            let feeder_state = Arc::clone(&route.state);
            let mut receiver = route.receiver;
            let feeder = thread::Builder::new()
                .name(format!("pks-relay-bus-feed-{index}"))
                .spawn(move || {
                    loop {
                        if feeder_stop.load(Ordering::Acquire)
                            || feeder_context.is_abort_requested()
                        {
                            break;
                        }
                        if let Some(frame) = receiver.try_recv() {
                            feeder_context.record_frame_received(1);
                            match frame_tx.try_send(frame) {
                                Ok(()) => {}
                                Err(mpsc::TrySendError::Full(_)) => {
                                    feeder_state
                                        .ingress_queue_drops_total
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                Err(mpsc::TrySendError::Disconnected(_)) => break,
                            }
                        } else {
                            if feeder_context.shutdown_mode().is_some() {
                                break;
                            }
                            let _ = feeder_context.wait_for_stop(FEEDER_POLL_INTERVAL);
                        }
                    }
                    receiver
                });
            match feeder {
                Ok(feeder) => {
                    feeders.push(feeder);
                    frame_receivers.push(frame_rx);
                }
                Err(error) => {
                    transport_stop.store(true, Ordering::Release);
                    for feeder in feeders {
                        let _ = feeder.join();
                    }
                    return route_states
                        .iter()
                        .enumerate()
                        .map(|(route_index, state)| RelayTransportOutcome {
                            statistics: statistics(&counters[route_index], state, None, None),
                            edge_observations: EdgeObservations::default(),
                            error: Some(relay_error(
                                "relay.feeder_spawn_failed",
                                ConnectorErrorStage::Startup,
                                ConnectorRetryability::Retryable,
                                format!("relay feeder spawn failed: {error}"),
                            )),
                        })
                        .collect();
                }
            }
        }

        let execution = run_prepared_frame_publisher(publisher, frame_receivers);
        transport_stop.store(true, Ordering::Release);
        let mut receivers = Vec::with_capacity(feeders.len());
        let mut feeder_failed = false;
        for feeder in feeders {
            if let Ok(receiver) = feeder.join() {
                receivers.push(Some(receiver));
            } else {
                receivers.push(None);
                feeder_failed = true;
            }
        }

        let (publish_stats, mut shared_error) = match execution {
            Ok(execution) => {
                let publish_result = execution.result.map_err(|error| error.to_string());
                let encoder_failed = execution
                    .encoder_threads
                    .into_iter()
                    .any(|encoder| encoder.join().is_err());
                match (publish_result, encoder_failed) {
                    (Ok(stats), false) => (Some(stats), None),
                    (Ok(stats), true) => (
                        Some(stats),
                        Some(relay_error(
                            "relay.encoder_panicked",
                            ConnectorErrorStage::Delivery,
                            ConnectorRetryability::Retryable,
                            "relay encoder thread panicked",
                        )),
                    ),
                    (Err(error), _) => (
                        None,
                        Some(relay_error(
                            "relay.publisher_failed",
                            ConnectorErrorStage::Delivery,
                            ConnectorRetryability::Retryable,
                            error,
                        )),
                    ),
                }
            }
            Err(error) => (
                None,
                Some(relay_error(
                    "relay.publisher_start_failed",
                    ConnectorErrorStage::Startup,
                    ConnectorRetryability::Retryable,
                    error.to_string(),
                )),
            ),
        };
        if feeder_failed && shared_error.is_none() {
            shared_error = Some(relay_error(
                "relay.feeder_panicked",
                ConnectorErrorStage::Delivery,
                ConnectorRetryability::Retryable,
                "relay feeder thread panicked",
            ));
        }

        route_states
            .iter()
            .enumerate()
            .map(|(index, state)| {
                let encode_errors = counters[index].opus_encode_errors.load(Ordering::Relaxed);
                let partial_samples_discarded = counters[index]
                    .normalization_partial_samples_discarded
                    .load(Ordering::Relaxed);
                let mut error = shared_error.clone();
                if error.is_none() && (encode_errors > 0 || partial_samples_discarded > 0) {
                    error = Some(relay_error(
                        "relay.audio_processing_failed",
                        ConnectorErrorStage::Delivery,
                        ConnectorRetryability::Retryable,
                        format!(
                            "relay audio processing failed: {encode_errors} Opus encode errors, {partial_samples_discarded} partial PCM samples discarded"
                        ),
                    ));
                }
                let edge_observations = receivers
                    .get_mut(index)
                    .and_then(Option::take)
                    .map(|receiver| {
                        if error.is_some() {
                            receiver.mark_worker_failure();
                        }
                        receiver.observations()
                    })
                    .unwrap_or_default();
                RelayTransportOutcome {
                    statistics: statistics(
                        &counters[index],
                        state,
                        publish_stats.as_ref(),
                        publish_stats
                            .as_ref()
                            .and_then(|stats| stats.streams.get(index)),
                    ),
                    edge_observations,
                    error,
                }
            })
            .collect()
    }

    fn cancel(self: Box<Self>) -> Result<(), ConnectorError> {
        cancel_handshake(self.publisher.handshake, &self.publisher.session_id).map_err(|error| {
            relay_error(
                "relay.preparation_cancel_failed",
                ConnectorErrorStage::Shutdown,
                ConnectorRetryability::Retryable,
                error.to_string(),
            )
        })
    }
}

struct PreparedRelayRoute {
    key: RelayPublishReceiptKey,
    receiver: EndpointAudioReceiver,
    state: Arc<RelayPublishReceiptState>,
}

struct RelayPublishWorker {
    routes: Option<Vec<PreparedRelayRoute>>,
    prepared: Option<Box<dyn PreparedRelayPublishTransport>>,
    receipts: Arc<Mutex<HashMap<RelayPublishReceiptKey, RelayPublishReceipt>>>,
}

impl ConnectorWorker for RelayPublishWorker {
    fn run(mut self: Box<Self>, context: ConnectorContext) -> ConnectorRunOutcome {
        let Some(routes) = self.routes.take() else {
            return ConnectorRunOutcome::failure(relay_error(
                "relay.routes_unavailable",
                ConnectorErrorStage::Startup,
                ConnectorRetryability::Never,
                "relay grouped routes were already consumed",
            ));
        };
        let Some(prepared) = self.prepared.take() else {
            return ConnectorRunOutcome::failure(relay_error(
                "relay.preparation_unavailable",
                ConnectorErrorStage::Startup,
                ConnectorRetryability::Never,
                "relay transport preparation was already consumed",
            ));
        };
        let identities = routes
            .iter()
            .map(|route| (route.key, Arc::clone(&route.state)))
            .collect::<Vec<_>>();

        let _ = context.report_readiness_success();
        let outcomes = prepared.run(routes, context.clone());
        if outcomes.len() != identities.len() {
            return ConnectorRunOutcome::failure(relay_error(
                "relay.invalid_outcome_count",
                ConnectorErrorStage::Join,
                ConnectorRetryability::Never,
                "relay transport returned an invalid grouped outcome count",
            ));
        }

        let mut first_error = None;
        for ((key, state), outcome) in identities.into_iter().zip(outcomes) {
            record_outcome(&context, &outcome);
            if first_error.is_none() {
                first_error.clone_from(&outcome.error);
            }
            let result = RelayPublishResult {
                key,
                statistics: outcome.statistics,
                edge_observations: outcome.edge_observations,
                error: outcome.error,
            };
            let _ = state.result.set(result);
        }

        if let Some(error) = first_error {
            return ConnectorRunOutcome::failure(error);
        }
        if context.is_stop_requested() {
            ConnectorRunOutcome::success()
        } else {
            ConnectorRunOutcome::failure(relay_error(
                "relay.publisher_exited",
                ConnectorErrorStage::Delivery,
                ConnectorRetryability::Retryable,
                "relay publisher exited before stop was requested",
            ))
        }
    }

    fn cancel_preparation(mut self: Box<Self>) -> Result<(), ConnectorError> {
        let keys = self
            .routes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|route| route.key)
            .collect::<Vec<_>>();
        let result = self
            .prepared
            .take()
            .map_or(Ok(()), PreparedRelayPublishTransport::cancel);
        remove_receipts(&self.receipts, &keys);
        result
    }
}

fn record_outcome(context: &ConnectorContext, outcome: &RelayTransportOutcome) {
    context.record_frame_delivered(outcome.statistics.rtp_packets_sent_total);
    context.record_frame_dropped(
        outcome
            .edge_observations
            .frames_dropped_total
            .saturating_add(outcome.statistics.ingress_queue_drops_total)
            .saturating_add(outcome.statistics.encoder_channel_drops_total)
            .saturating_add(outcome.statistics.publisher_stale_drops_total),
    );
    context.record_discontinuity(outcome.edge_observations.discontinuities_total);
}

struct RelayTransportOutcome {
    statistics: RelayPublishStatistics,
    edge_observations: EdgeObservations,
    error: Option<ConnectorError>,
}

fn failed_outcomes(
    routes: &[PreparedRelayRoute],
    error: &ConnectorError,
) -> Vec<RelayTransportOutcome> {
    routes
        .iter()
        .map(|route| RelayTransportOutcome {
            statistics: RelayPublishStatistics::default(),
            edge_observations: route.receiver.observations(),
            error: Some(error.clone()),
        })
        .collect()
}

fn statistics(
    counters: &EncoderCounters,
    state: &RelayPublishReceiptState,
    publish: Option<&crate::rtc::publisher::PublishStats>,
    stream: Option<&crate::rtc::publisher::PublishStreamStats>,
) -> RelayPublishStatistics {
    RelayPublishStatistics {
        rtp_packets_sent_total: stream.map_or(0, |stats| stats.rtp_sent),
        rtp_payload_bytes_sent_total: stream.map_or(0, |stats| stats.bytes_sent),
        rtc_poll_drains_total: publish.map_or(0, |stats| stats.drains),
        rtc_writer_writes_total: publish.map_or(0, |stats| stats.writes),
        elapsed_ms: publish.map_or(0, |stats| {
            u64::try_from(stats.elapsed.as_millis()).unwrap_or(u64::MAX)
        }),
        opus_packets_encoded_total: counters.opus_packets_out.load(Ordering::Relaxed),
        opus_encode_errors_total: counters.opus_encode_errors.load(Ordering::Relaxed),
        encoder_channel_drops_total: counters.encoded_channel_drops.load(Ordering::Relaxed),
        publisher_stale_drops_total: counters.publisher_stale_drops.load(Ordering::Relaxed),
        ingress_queue_drops_total: state.ingress_queue_drops_total.load(Ordering::Relaxed),
        cancelled_output_frames_total: counters.cancelled_output_frames.load(Ordering::Relaxed),
        cancelled_output_samples_total: counters.cancelled_output_samples.load(Ordering::Relaxed),
    }
}

fn relay_error(
    code: &'static str,
    stage: ConnectorErrorStage,
    retryability: ConnectorRetryability,
    message: impl Into<String>,
) -> ConnectorError {
    let mut message = message.into();
    if message.len() > pocketstation::connector::MAX_CONNECTOR_ERROR_MESSAGE_BYTES {
        let mut boundary = pocketstation::connector::MAX_CONNECTOR_ERROR_MESSAGE_BYTES;
        while boundary > 0 && !message.is_char_boundary(boundary) {
            boundary -= 1;
        }
        message.truncate(boundary);
    }
    ConnectorError::new(
        ConnectorErrorCode::new(code).expect("relay error codes are compile-time constants"),
        stage,
        retryability,
        message,
    )
    .expect("relay error messages are bounded and non-empty")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use pocketstation::connector::{Connector, ConnectorSecret};
    use pocketstation::{ApplicationSelector, Session, Source};

    use crate::connector::relay_connector_manifest;

    #[derive(Default)]
    struct FakeTransportState {
        prepares: AtomicUsize,
        runs: AtomicUsize,
        inputs: AtomicUsize,
        cancels: AtomicUsize,
        fail_run: AtomicBool,
        shutdown_mode: AtomicU64,
    }

    struct FakeTransport {
        state: Arc<FakeTransportState>,
    }

    impl RelayPublishTransport for FakeTransport {
        fn prepare(
            &self,
            routes: Vec<RelayTransportPreparation>,
        ) -> Result<Box<dyn PreparedRelayPublishTransport>, ConnectorError> {
            self.state.prepares.fetch_add(1, Ordering::Relaxed);
            self.state.inputs.store(routes.len(), Ordering::Relaxed);
            Ok(Box::new(FakePreparedTransport {
                state: Arc::clone(&self.state),
            }))
        }
    }

    struct FakePreparedTransport {
        state: Arc<FakeTransportState>,
    }

    impl PreparedRelayPublishTransport for FakePreparedTransport {
        fn run(
            self: Box<Self>,
            mut routes: Vec<PreparedRelayRoute>,
            context: ConnectorContext,
        ) -> Vec<RelayTransportOutcome> {
            self.state.runs.fetch_add(1, Ordering::Relaxed);
            if self.state.fail_run.load(Ordering::Acquire) {
                let error = relay_error(
                    "relay.test_delivery_failure",
                    ConnectorErrorStage::Delivery,
                    ConnectorRetryability::Retryable,
                    "injected relay delivery failure",
                );
                return failed_outcomes(&routes, &error);
            }
            let mut delivered = vec![0_u64; routes.len()];
            loop {
                if context.is_abort_requested() {
                    break;
                }
                let mut progressed = false;
                for (index, route) in routes.iter_mut().enumerate() {
                    if route.receiver.try_recv().is_some() {
                        context.record_frame_received(1);
                        delivered[index] = delivered[index].saturating_add(1);
                        progressed = true;
                    }
                }
                if !progressed {
                    if context.shutdown_mode().is_some() {
                        break;
                    }
                    let _ = context.wait_for_stop(Duration::from_millis(1));
                }
            }
            self.state.shutdown_mode.store(
                match context.shutdown_mode() {
                    Some(pocketstation::EndpointShutdownMode::Drain) => 1,
                    Some(pocketstation::EndpointShutdownMode::Abort) => 2,
                    None => 0,
                },
                Ordering::Release,
            );
            routes
                .into_iter()
                .zip(delivered)
                .map(|(route, delivered)| RelayTransportOutcome {
                    statistics: RelayPublishStatistics {
                        rtp_packets_sent_total: delivered,
                        ..RelayPublishStatistics::default()
                    },
                    edge_observations: route.receiver.observations(),
                    error: None,
                })
                .collect()
        }

        fn cancel(self: Box<Self>) -> Result<(), ConnectorError> {
            self.state.cancels.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn fixture(
        receipt_capacity: usize,
    ) -> (Session, Arc<RelayConnectorFactory>, Arc<FakeTransportState>) {
        let session = pocketstation::conformance::session().expect("fixture Session");
        let state = Arc::new(FakeTransportState::default());
        let factory = Arc::new(
            RelayConnectorFactory::with_transport(
                Arc::new(FakeTransport {
                    state: Arc::clone(&state),
                }),
                receipt_capacity,
            )
            .expect("valid fake factory"),
        );
        (session, factory, state)
    }

    fn declare_bus(
        session: &Session,
        registered: &pocketstation::connector::RegisteredConnector,
        bus_id: &str,
        publisher_group_id: &str,
        relay_session_id: &str,
    ) -> pocketstation::EndpointHandle {
        let configuration = RelayRouteConfiguration::new(
            "https://relay.invalid",
            relay_session_id,
            ConnectorSecret::new("source-secret").expect("secret"),
            bus_id,
        )
        .expect("route configuration")
        .with_publisher_group(publisher_group_id)
        .expect("publisher group")
        .connector_configuration()
        .expect("connector configuration");
        registered
            .declare(
                session,
                configuration,
                pocketstation::EdgeContract::realtime_audio(),
            )
            .expect("relay endpoint declaration")
    }

    #[test]
    fn grouped_audio_buses_share_one_core_managed_worker_and_publish_results() {
        let (session, factory, state) = fixture(8);
        let registered = session
            .register_connector(
                Connector::new(
                    relay_connector_manifest().expect("manifest"),
                    factory.clone(),
                )
                .expect("connector"),
            )
            .expect("registration");
        let application_endpoint = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let microphone_endpoint = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-a",
            "session-a",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        let application_route = application
            .send(application_endpoint)
            .expect("application route");
        let microphone_route = microphone
            .send(microphone_endpoint)
            .expect("microphone route");

        let mut running = session.start().expect("start");
        let deadline = Instant::now() + Duration::from_secs(2);
        while state.runs.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(60));
        assert!(running.stop().is_success());

        assert_eq!(state.prepares.load(Ordering::Relaxed), 1);
        assert_eq!(state.runs.load(Ordering::Relaxed), 1);
        assert_eq!(state.inputs.load(Ordering::Relaxed), 2);
        assert_eq!(state.cancels.load(Ordering::Relaxed), 0);
        assert_eq!(state.shutdown_mode.load(Ordering::Acquire), 1);
        for key in [
            RelayPublishReceiptKey {
                endpoint_id: application_endpoint.id(),
                route_id: application_route,
            },
            RelayPublishReceiptKey {
                endpoint_id: microphone_endpoint.id(),
                route_id: microphone_route,
            },
        ] {
            let result = factory.take_result(key).expect("relay result");
            assert!(result.error.is_none());
            assert!(result.statistics.rtp_packets_sent_total > 0);
        }
        let observations = registered.observations().expect("observations");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].endpoint_ids.len(), 2);
        assert!(observations[0].endpoint.frames_received_total > 0);
        assert!(observations[0].endpoint.frames_delivered_total > 0);
    }

    #[test]
    fn given_running_relay_when_session_is_cancelled_then_transport_receives_abort_intent() {
        let (session, factory, state) = fixture(2);
        let registered = session
            .register_connector(
                Connector::new(relay_connector_manifest().expect("manifest"), factory)
                    .expect("connector"),
            )
            .expect("registration");
        let endpoint = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let microphone_endpoint = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-a",
            "session-a",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        application.send(endpoint).expect("application route");
        microphone
            .send(microphone_endpoint)
            .expect("microphone route");

        let mut running = session.start().expect("start");
        assert!(running.cancel().is_success());
        assert_eq!(state.shutdown_mode.load(Ordering::Acquire), 2);
    }

    #[test]
    fn grouped_routes_with_different_sessions_fail_before_transport_preparation() {
        let (session, factory, state) = fixture(8);
        let registered = session
            .register_connector(
                Connector::new(relay_connector_manifest().expect("manifest"), factory)
                    .expect("connector"),
            )
            .expect("registration");
        let first = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let second = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-a",
            "session-b",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        application.send(first).expect("first route");
        microphone.send(second).expect("second route");

        let Err(error) = session.start() else {
            panic!("publisher mismatch must fail");
        };
        assert!(error.to_string().contains("relay.publisher_group_mismatch"));
        assert_eq!(state.prepares.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn receipt_capacity_is_bounded_before_network_preparation() {
        let (session, factory, state) = fixture(1);
        let registered = session
            .register_connector(
                Connector::new(relay_connector_manifest().expect("manifest"), factory)
                    .expect("connector"),
            )
            .expect("registration");
        let first = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let second = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-a",
            "session-a",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        application.send(first).expect("first route");
        microphone.send(second).expect("second route");

        let Err(error) = session.start() else {
            panic!("receipt capacity must fail");
        };
        assert!(error
            .to_string()
            .contains("relay.receipt_capacity_exhausted"));
        assert_eq!(state.prepares.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn later_group_failure_cancels_an_already_prepared_relay_worker() {
        let (session, factory, state) = fixture(1);
        let registered = session
            .register_connector(
                Connector::new(relay_connector_manifest().expect("manifest"), factory)
                    .expect("connector"),
            )
            .expect("registration");
        let first = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let second = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-b",
            "session-a",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        application.send(first).expect("first route");
        microphone.send(second).expect("second route");

        let Err(error) = session.start() else {
            panic!("second publisher group must exhaust receipt capacity");
        };
        assert!(error
            .to_string()
            .contains("relay.receipt_capacity_exhausted"));
        assert_eq!(state.prepares.load(Ordering::Relaxed), 1);
        assert_eq!(state.cancels.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn provider_delivery_failure_reaches_receipt_observations_and_stop_outcome() {
        let (session, factory, state) = fixture(2);
        state.fail_run.store(true, Ordering::Release);
        let registered = session
            .register_connector(
                Connector::new(
                    relay_connector_manifest().expect("manifest"),
                    factory.clone(),
                )
                .expect("connector"),
            )
            .expect("registration");
        let endpoint = declare_bus(
            &session,
            &registered,
            "application",
            "publisher-a",
            "session-a",
        );
        let microphone_endpoint = declare_bus(
            &session,
            &registered,
            "microphone",
            "publisher-a",
            "session-a",
        );
        let application = session
            .capture(Source::application(ApplicationSelector::name("Demo")))
            .expect("application capture");
        let microphone = session
            .capture(Source::microphone_default())
            .expect("microphone capture");
        let route = application.send(endpoint).expect("route");
        microphone
            .send(microphone_endpoint)
            .expect("microphone route");
        let key = RelayPublishReceiptKey {
            endpoint_id: endpoint.id(),
            route_id: route,
        };

        let mut running = session.start().expect("start");
        let deadline = Instant::now() + Duration::from_secs(2);
        while factory
            .receipt(key)
            .and_then(|receipt| receipt.result().cloned())
            .is_none()
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        let stop = running.stop();
        assert!(!stop.is_success());
        let result = factory.take_result(key).expect("failure result");
        let error = result.error.expect("provider error");
        assert_eq!(error.code().as_str(), "relay.test_delivery_failure");
        assert_eq!(error.retryability(), ConnectorRetryability::Retryable);
        let observations = registered.observations().expect("observations");
        assert_eq!(observations[0].connector.failures_total, 1);
        assert_eq!(observations[0].endpoint.failures_total, 1);
    }

    #[test]
    fn manifest_and_configuration_keep_secrets_redacted() {
        let manifest = relay_connector_manifest().expect("manifest");
        assert_eq!(manifest.node().inputs().len(), 1);
        let configuration = RelayRouteConfiguration::new(
            "https://relay.invalid",
            "session-a",
            ConnectorSecret::new("never-print-this").expect("secret"),
            "application",
        )
        .expect("configuration");
        let debug = format!("{configuration:?}");
        assert!(!debug.contains("never-print-this"));
        let resolved = manifest
            .configuration()
            .resolve(
                &configuration
                    .connector_configuration()
                    .expect("connector configuration"),
            )
            .expect("resolved configuration");
        assert!(!format!("{resolved:?}").contains("never-print-this"));
    }
}
