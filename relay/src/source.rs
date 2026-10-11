//! Relay `AudioBus` subscriber projected as a Core external `Source`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use pocketstation::codec::OpusDecoder;
use pocketstation::timing::{monotonic_timestamp_ns, PROCESS_MONOTONIC_CLOCK_DOMAIN_ID};
use pocketstation::{
    AudioBufferPool, AudioCaps, AudioFrame, ChannelLayout, ConfigError, ExecutionPartition,
    ExecutionSafety, MediaCaps, Multiplicity, PortDirection, PortSpec, SampleFormat, SampleSpec,
    Session, SignalEnvelope, SignalLineage, SignalSpec, SignalTiming, SourceCancellation,
    SourceConfiguration, SourceDriver, SourceDriverError, SourceEmission, SourceFactory,
    SourceInstanceHandle, SourceManifest, SourceOutputHandle, SourcePrepareContext,
    SourceSessionContext, SourceTypeId,
};

use crate::configuration::RelayIceServer;
use crate::rtc::handshake::run_subscriber_handshake;
use crate::rtc::subscriber::{ReceivedOpusFrame, SubscriberConnection};

pub const RELAY_AUDIOBUS_SOURCE_TYPE_ID: &str = "io.pocketstation.source.relay-audio-bus.v1";
const OUTPUT_PORT: &str = "audio";
const SAMPLE_RATE_HZ: u32 = 48_000;
const POOL_SLOTS: usize = 64;
const MAX_PACKET_SAMPLES: usize = 960;
const POLL_IDLE: Duration = Duration::from_millis(1);

/// Subscriber capability kept out of `SourceConfiguration` and redacted from
/// all `Debug` output.
#[derive(Clone)]
pub struct RelaySubscriberToken(Arc<str>);

impl RelaySubscriberToken {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RelaySubscriberToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RelaySubscriberToken([REDACTED])")
    }
}

#[derive(Debug, Clone)]
pub struct RelayAudioBusSourceConfig {
    pub relay_url: String,
    pub relay_session_id: String,
    pub bus_id: String,
    pub subscriber_token: RelaySubscriberToken,
    /// Attachment generation obtained from the Relay control-plane state.
    /// A new publisher attachment must use a new explicit Core Session.
    pub source_generation: u32,
    pub startup_timeout: Duration,
    /// Delivery timeout, not a signal-energy or silence detector.
    pub no_frame_timeout: Duration,
    pub ice_servers: Vec<RelayIceServer>,
}

impl RelayAudioBusSourceConfig {
    /// Builds and validates an exact Relay session/bus subscription.
    ///
    /// # Errors
    ///
    /// Returns [`RelayAudioBusSourceError::InvalidConfiguration`] when an
    /// identity, generation, URL, or timeout violates the source contract.
    pub fn new(
        relay_url: impl Into<String>,
        relay_session_id: impl Into<String>,
        bus_id: impl Into<String>,
        subscriber_token: RelaySubscriberToken,
        source_generation: u32,
    ) -> Result<Self, RelayAudioBusSourceError> {
        let config = Self {
            relay_url: relay_url.into(),
            relay_session_id: relay_session_id.into(),
            bus_id: bus_id.into(),
            subscriber_token,
            source_generation,
            startup_timeout: Duration::from_secs(10),
            no_frame_timeout: Duration::from_secs(6),
            ice_servers: Vec::new(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates the public, non-secret source configuration.
    ///
    /// # Errors
    ///
    /// Returns [`RelayAudioBusSourceError::InvalidConfiguration`] when the
    /// source cannot be started without inventing identity or lifecycle state.
    pub fn validate(&self) -> Result<(), RelayAudioBusSourceError> {
        validate_bounded_identity("relay_url", &self.relay_url, 2048)?;
        if !(self.relay_url.starts_with("http://") || self.relay_url.starts_with("https://")) {
            return Err(RelayAudioBusSourceError::InvalidConfiguration(
                "relay_url must use http:// or https://".to_owned(),
            ));
        }
        validate_bounded_identity("relay_session_id", &self.relay_session_id, 256)?;
        validate_bounded_identity("bus_id", &self.bus_id, 128)?;
        if self.subscriber_token.expose().is_empty() {
            return Err(RelayAudioBusSourceError::InvalidConfiguration(
                "subscriber_token cannot be empty".to_owned(),
            ));
        }
        if self.source_generation == 0 {
            return Err(RelayAudioBusSourceError::InvalidConfiguration(
                "source_generation must be non-zero".to_owned(),
            ));
        }
        if self.startup_timeout.is_zero() || self.no_frame_timeout.is_zero() {
            return Err(RelayAudioBusSourceError::InvalidConfiguration(
                "startup_timeout and no_frame_timeout must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_bounded_identity(
    field: &str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), RelayAudioBusSourceError> {
    if value.is_empty() || value.trim() != value || value.len() > maximum_bytes {
        return Err(RelayAudioBusSourceError::InvalidConfiguration(format!(
            "{field} must be non-empty, trimmed, and at most {maximum_bytes} bytes"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelayAudioBusSourceSnapshot {
    pub prepared_total: u64,
    pub startup_failures_total: u64,
    pub connected_total: u64,
    pub first_frame_total: u64,
    pub received_frames_total: u64,
    pub emitted_frames_total: u64,
    pub noncontiguous_frames_total: u64,
    pub pool_dropped_frames_total: u64,
    pub decode_failures_total: u64,
    pub runtime_failures_total: u64,
    pub closed_total: u64,
    pub latest_frame_at_ns: u64,
    pub source_generation: u32,
    pub closed: bool,
}

#[derive(Clone)]
pub struct RelayAudioBusSourceObservations {
    inner: Arc<ObservationState>,
}

impl RelayAudioBusSourceObservations {
    #[must_use]
    pub fn snapshot(&self) -> RelayAudioBusSourceSnapshot {
        RelayAudioBusSourceSnapshot {
            prepared_total: self.inner.prepared_total.load(Ordering::Relaxed),
            startup_failures_total: self.inner.startup_failures_total.load(Ordering::Relaxed),
            connected_total: self.inner.connected_total.load(Ordering::Relaxed),
            first_frame_total: self.inner.first_frame_total.load(Ordering::Relaxed),
            received_frames_total: self.inner.received_frames_total.load(Ordering::Relaxed),
            emitted_frames_total: self.inner.emitted_frames_total.load(Ordering::Relaxed),
            noncontiguous_frames_total: self
                .inner
                .noncontiguous_frames_total
                .load(Ordering::Relaxed),
            pool_dropped_frames_total: self.inner.pool_dropped_frames_total.load(Ordering::Relaxed),
            decode_failures_total: self.inner.decode_failures_total.load(Ordering::Relaxed),
            runtime_failures_total: self.inner.runtime_failures_total.load(Ordering::Relaxed),
            closed_total: self.inner.closed_total.load(Ordering::Relaxed),
            latest_frame_at_ns: self.inner.latest_frame_at_ns.load(Ordering::Relaxed),
            source_generation: self.inner.source_generation,
            closed: self.inner.closed.load(Ordering::Acquire),
        }
    }
}

impl fmt::Debug for RelayAudioBusSourceObservations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.snapshot().fmt(formatter)
    }
}

#[derive(Default)]
struct ObservationCounters {
    prepared_total: AtomicU64,
    startup_failures_total: AtomicU64,
    connected_total: AtomicU64,
    first_frame_total: AtomicU64,
    received_frames_total: AtomicU64,
    emitted_frames_total: AtomicU64,
    noncontiguous_frames_total: AtomicU64,
    pool_dropped_frames_total: AtomicU64,
    decode_failures_total: AtomicU64,
    runtime_failures_total: AtomicU64,
    closed_total: AtomicU64,
    latest_frame_at_ns: AtomicU64,
    closed: AtomicBool,
}

struct ObservationState {
    source_generation: u32,
    prepared_total: AtomicU64,
    startup_failures_total: AtomicU64,
    connected_total: AtomicU64,
    first_frame_total: AtomicU64,
    received_frames_total: AtomicU64,
    emitted_frames_total: AtomicU64,
    noncontiguous_frames_total: AtomicU64,
    pool_dropped_frames_total: AtomicU64,
    decode_failures_total: AtomicU64,
    runtime_failures_total: AtomicU64,
    closed_total: AtomicU64,
    latest_frame_at_ns: AtomicU64,
    closed: AtomicBool,
}

impl ObservationState {
    fn new(source_generation: u32) -> Arc<Self> {
        let counters = ObservationCounters::default();
        Arc::new(Self {
            source_generation,
            prepared_total: counters.prepared_total,
            startup_failures_total: counters.startup_failures_total,
            connected_total: counters.connected_total,
            first_frame_total: counters.first_frame_total,
            received_frames_total: counters.received_frames_total,
            emitted_frames_total: counters.emitted_frames_total,
            noncontiguous_frames_total: counters.noncontiguous_frames_total,
            pool_dropped_frames_total: counters.pool_dropped_frames_total,
            decode_failures_total: counters.decode_failures_total,
            runtime_failures_total: counters.runtime_failures_total,
            closed_total: counters.closed_total,
            latest_frame_at_ns: counters.latest_frame_at_ns,
            closed: counters.closed,
        })
    }
}

struct PendingSource {
    config: RelayAudioBusSourceConfig,
    observations: Arc<ObservationState>,
}

pub struct RelayAudioBusSourceHandle {
    source: SourceInstanceHandle,
    output: SourceOutputHandle,
    observations: RelayAudioBusSourceObservations,
}

impl RelayAudioBusSourceHandle {
    #[must_use]
    pub const fn source(&self) -> &SourceInstanceHandle {
        &self.source
    }

    #[must_use]
    pub const fn output(&self) -> &SourceOutputHandle {
        &self.output
    }

    #[must_use]
    pub const fn observations(&self) -> &RelayAudioBusSourceObservations {
        &self.observations
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        SourceInstanceHandle,
        SourceOutputHandle,
        RelayAudioBusSourceObservations,
    ) {
        (self.source, self.output, self.observations)
    }
}

/// Session-scoped installer for one or more Relay-backed audio Sources.
///
/// Install once per Core `Session`, then declare each exact remote bus. Secrets
/// and socket ownership stay in the private source reservation keyed by Core's
/// allocated `SourceId`; public `SourceConfiguration` remains empty.
pub struct RelayAudioBusSources {
    factory: Arc<RelayAudioBusSourceFactory>,
    core_session_id: u64,
}

impl RelayAudioBusSources {
    /// Installs the Relay `AudioBus` source factory on one Core `Session`.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest cannot be constructed or the Core
    /// Session rejects source registration.
    pub fn install(session: &Session) -> Result<Self, RelayAudioBusSourceError> {
        let factory = Arc::new(RelayAudioBusSourceFactory::new()?);
        session
            .register_source(Arc::clone(&factory) as Arc<dyn SourceFactory>)
            .map_err(|error| RelayAudioBusSourceError::Session(error.to_string()))?;
        Ok(Self {
            factory,
            core_session_id: session.id().get(),
        })
    }

    /// Declares one exact remote bus while retaining credentials privately.
    ///
    /// # Errors
    ///
    /// Returns an error when the config is invalid, the Session differs from
    /// the installation Session, declaration fails, or the private reservation
    /// cannot be retained.
    pub fn declare(
        &self,
        session: &Session,
        config: RelayAudioBusSourceConfig,
    ) -> Result<RelayAudioBusSourceHandle, RelayAudioBusSourceError> {
        if session.id().get() != self.core_session_id {
            return Err(RelayAudioBusSourceError::WrongSession);
        }
        config.validate()?;
        let source = session
            .source(
                SourceTypeId::new(RELAY_AUDIOBUS_SOURCE_TYPE_ID)
                    .map_err(|error| RelayAudioBusSourceError::Manifest(error.to_string()))?,
                SourceConfiguration::default(),
            )
            .map_err(|error| RelayAudioBusSourceError::Session(error.to_string()))?;
        let observations = ObservationState::new(config.source_generation);
        self.factory.bind(
            source.source_id().get(),
            PendingSource {
                config,
                observations: Arc::clone(&observations),
            },
        )?;
        let output = source.output(OUTPUT_PORT).map_err(|error| {
            self.factory.cancel(source.source_id().get());
            RelayAudioBusSourceError::Session(error.to_string())
        })?;
        Ok(RelayAudioBusSourceHandle {
            source,
            output,
            observations: RelayAudioBusSourceObservations {
                inner: observations,
            },
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelayAudioBusSourceError {
    #[error("invalid Relay AudioBus source configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Relay AudioBus source manifest failed: {0}")]
    Manifest(String),
    #[error("Relay AudioBus source Session declaration failed: {0}")]
    Session(String),
    #[error("Relay AudioBus Sources must be declared on the Session where they were installed")]
    WrongSession,
    #[error("Relay AudioBus source reservation state is unavailable")]
    ReservationStateUnavailable,
    #[error("Relay AudioBus source reservation already exists")]
    ReservationConflict,
}

struct RelayAudioBusSourceFactory {
    manifest: SourceManifest,
    pending: Arc<Mutex<BTreeMap<u64, PendingSource>>>,
}

impl RelayAudioBusSourceFactory {
    fn new() -> Result<Self, RelayAudioBusSourceError> {
        let manifest = SourceManifest::new(
            SourceTypeId::new(RELAY_AUDIOBUS_SOURCE_TYPE_ID)
                .map_err(|error| RelayAudioBusSourceError::Manifest(error.to_string()))?,
            1,
            1,
            vec![PortSpec::new(
                OUTPUT_PORT,
                PortDirection::Output,
                SignalSpec::audio(),
                MediaCaps::Audio(AudioCaps {
                    sample_rate_hz: Some(SAMPLE_RATE_HZ),
                    frame_samples: Some(MAX_PACKET_SAMPLES),
                    channel_layout: ChannelLayout::Mono,
                    format: SampleFormat::F32Interleaved,
                }),
                Multiplicity::Many,
                true,
            )
            .map_err(|error| RelayAudioBusSourceError::Manifest(error.to_string()))?],
            ExecutionPartition::BlockingWorker,
            ExecutionSafety::BlockingAllowed,
        )
        .map_err(|error| RelayAudioBusSourceError::Manifest(error.to_string()))?;
        Ok(Self {
            manifest,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn bind(&self, source_id: u64, pending: PendingSource) -> Result<(), RelayAudioBusSourceError> {
        let mut reservations = self
            .pending
            .lock()
            .map_err(|_| RelayAudioBusSourceError::ReservationStateUnavailable)?;
        if reservations.insert(source_id, pending).is_some() {
            return Err(RelayAudioBusSourceError::ReservationConflict);
        }
        Ok(())
    }

    fn cancel(&self, source_id: u64) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&source_id);
        }
    }
}

impl SourceFactory for RelayAudioBusSourceFactory {
    fn manifest(&self) -> &SourceManifest {
        &self.manifest
    }

    fn validate_config(&self, configuration: &SourceConfiguration) -> Result<(), ConfigError> {
        if configuration.iter().next().is_some() {
            return Err(ConfigError::Invalid {
                key: "<configuration>".to_owned(),
                reason: "Relay AudioBus Sources use private Session reservations".to_owned(),
            });
        }
        Ok(())
    }

    fn create(
        &self,
        configuration: &SourceConfiguration,
    ) -> Result<Box<dyn SourceDriver>, SourceDriverError> {
        self.validate_config(configuration)
            .map_err(|error| SourceDriverError::Failed(error.to_string()))?;
        Ok(Box::new(RelayAudioBusSourceDriver {
            pending: Arc::clone(&self.pending),
            session: None,
            config: None,
            observations: None,
            connection: None,
            decoder: None,
            pool: AudioBufferPool::new(POOL_SLOTS, MAX_PACKET_SAMPLES),
            pending_frame: None,
            first_rtp_timestamp: None,
            first_local_timestamp_ns: None,
            previous_rtp_timestamp: None,
            next_sequence: 0,
            discontinuity_epoch: 0,
            last_delivery: None,
            seen_frame: false,
        }))
    }
}

struct DecodedFrame {
    samples: Vec<f32>,
    timestamp_ns: u64,
    duration_ns: u64,
    sequence_number: u64,
    discontinuity_epoch: u64,
}

struct RelayAudioBusSourceDriver {
    pending: Arc<Mutex<BTreeMap<u64, PendingSource>>>,
    session: Option<SourceSessionContext>,
    config: Option<RelayAudioBusSourceConfig>,
    observations: Option<Arc<ObservationState>>,
    connection: Option<SubscriberConnection>,
    decoder: Option<OpusDecoder>,
    pool: Arc<AudioBufferPool>,
    pending_frame: Option<DecodedFrame>,
    first_rtp_timestamp: Option<u64>,
    first_local_timestamp_ns: Option<u64>,
    previous_rtp_timestamp: Option<u64>,
    next_sequence: u64,
    discontinuity_epoch: u64,
    last_delivery: Option<Instant>,
    seen_frame: bool,
}

impl SourceDriver for RelayAudioBusSourceDriver {
    fn prepare(&mut self, context: &SourcePrepareContext) -> Result<(), SourceDriverError> {
        let session = context.session.clone().ok_or_else(|| {
            SourceDriverError::Failed("Relay AudioBus Source requires Session identity".to_owned())
        })?;
        let pending = self
            .pending
            .lock()
            .map_err(|_| {
                SourceDriverError::Failed(
                    "Relay AudioBus reservation state is unavailable".to_owned(),
                )
            })?
            .remove(&session.source_id.get())
            .ok_or_else(|| {
                SourceDriverError::Failed(
                    "Relay AudioBus Source reservation is absent or consumed".to_owned(),
                )
            })?;
        pending
            .observations
            .prepared_total
            .fetch_add(1, Ordering::Relaxed);
        self.session = Some(session);
        self.config = Some(pending.config);
        self.observations = Some(pending.observations);

        let handshake_result = {
            let config = self.config.as_ref().ok_or_else(|| {
                SourceDriverError::Failed("Relay AudioBus config is absent".to_owned())
            })?;
            run_subscriber_handshake(
                &config.relay_url,
                &config.relay_session_id,
                &config.bus_id,
                config.subscriber_token.expose(),
                &config.ice_servers,
                config.startup_timeout,
            )
        };
        let handshake = match handshake_result {
            Ok(handshake) => handshake,
            Err(error) => return Err(self.fail_startup(error.to_string())),
        };
        let relay_session_id = self
            .config
            .as_ref()
            .ok_or_else(|| SourceDriverError::Failed("Relay AudioBus config is absent".to_owned()))?
            .relay_session_id
            .clone();
        let connection = match SubscriberConnection::new(handshake, relay_session_id) {
            Ok(connection) => connection,
            Err(error) => return Err(self.fail_startup(error.to_string())),
        };
        self.connection = Some(connection);
        self.observation_state()?
            .connected_total
            .fetch_add(1, Ordering::Relaxed);
        self.decoder = match OpusDecoder::new() {
            Ok(decoder) => Some(decoder),
            Err(error) => return Err(self.fail_startup(error.to_string())),
        };

        let startup_timeout = self
            .config
            .as_ref()
            .ok_or_else(|| SourceDriverError::Failed("Relay AudioBus config is absent".to_owned()))?
            .startup_timeout;
        let Some(startup_deadline) = Instant::now().checked_add(startup_timeout) else {
            return Err(self.fail_startup("startup deadline overflow"));
        };
        while Instant::now() < startup_deadline {
            let encoded = match self.poll_encoded() {
                Ok(encoded) => encoded,
                Err(error) => return Err(self.fail_startup(error.to_string())),
            };
            if let Some(encoded) = encoded {
                let decoded = match self.decode(&encoded) {
                    Ok(decoded) => decoded,
                    Err(error) => return Err(self.fail_startup(error.to_string())),
                };
                self.pending_frame = Some(decoded);
                self.observation_state()?
                    .first_frame_total
                    .fetch_add(1, Ordering::Relaxed);
                self.last_delivery = Some(Instant::now());
                return Ok(());
            }
            thread::sleep(POLL_IDLE);
        }
        Err(self.fail_startup(
            "Relay AudioBus connected but delivered no Opus frame before startup timeout",
        ))
    }

    fn next(
        &mut self,
        cancellation: &SourceCancellation,
    ) -> Result<Option<SourceEmission>, SourceDriverError> {
        if let Some(decoded) = self.pending_frame.take() {
            return self.emission(&decoded).map(Some);
        }
        loop {
            if cancellation.is_cancelled() {
                return Ok(None);
            }
            let encoded = self.poll_encoded().inspect_err(|_| {
                self.observe_runtime_failure();
            })?;
            if let Some(encoded) = encoded {
                let decoded = self.decode(&encoded).inspect_err(|_| {
                    self.observe_runtime_failure();
                })?;
                if let Some(emission) = self.emission_or_drop(&decoded)? {
                    return Ok(Some(emission));
                }
                continue;
            }
            let no_frame_timeout = self
                .config
                .as_ref()
                .ok_or_else(|| SourceDriverError::Failed("source is not prepared".to_owned()))?
                .no_frame_timeout;
            if self
                .last_delivery
                .is_some_and(|last| last.elapsed() >= no_frame_timeout)
            {
                self.observation_state()?
                    .runtime_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(SourceDriverError::Failed(format!(
                    "Relay AudioBus delivered no media frame for {} ms",
                    no_frame_timeout.as_millis()
                )));
            }
            thread::sleep(POLL_IDLE);
        }
    }

    fn close(&mut self) -> Result<(), SourceDriverError> {
        let result = self
            .connection
            .as_mut()
            .map(SubscriberConnection::close)
            .transpose()
            .map_err(|error| SourceDriverError::Failed(error.to_string()));
        self.observe_closed();
        result.map(|_| ())
    }
}

impl RelayAudioBusSourceDriver {
    fn observation_state(&self) -> Result<&ObservationState, SourceDriverError> {
        self.observations
            .as_deref()
            .ok_or_else(|| SourceDriverError::Failed("source observations are absent".to_owned()))
    }

    fn observe_runtime_failure(&self) {
        if let Some(observations) = &self.observations {
            observations
                .runtime_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn observe_closed(&self) {
        if let Some(observations) = &self.observations {
            if !observations.closed.swap(true, Ordering::AcqRel) {
                observations.closed_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn fail_startup(&mut self, message: impl Into<String>) -> SourceDriverError {
        if let Some(observations) = &self.observations {
            observations
                .startup_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Some(connection) = &mut self.connection {
            let _ = connection.close();
        }
        self.observe_closed();
        SourceDriverError::Failed(message.into())
    }

    fn poll_encoded(&mut self) -> Result<Option<ReceivedOpusFrame>, SourceDriverError> {
        self.connection
            .as_mut()
            .ok_or_else(|| SourceDriverError::Failed("subscriber is not prepared".to_owned()))?
            .poll_frame()
            .map_err(|error| SourceDriverError::Failed(error.to_string()))
    }

    fn decode(&mut self, encoded: &ReceivedOpusFrame) -> Result<DecodedFrame, SourceDriverError> {
        let observations = Arc::clone(self.observations.as_ref().ok_or_else(|| {
            SourceDriverError::Failed("source observations are absent".to_owned())
        })?);
        observations
            .received_frames_total
            .fetch_add(1, Ordering::Relaxed);
        if self.seen_frame && !encoded.contiguous {
            self.discontinuity_epoch = self.discontinuity_epoch.saturating_add(1);
            observations
                .noncontiguous_frames_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if self
            .previous_rtp_timestamp
            .is_some_and(|previous| encoded.rtp_timestamp < previous)
        {
            self.discontinuity_epoch = self.discontinuity_epoch.saturating_add(1);
            self.first_rtp_timestamp = None;
            self.first_local_timestamp_ns = None;
        }
        self.previous_rtp_timestamp = Some(encoded.rtp_timestamp);
        self.seen_frame = true;

        let mut samples = Vec::with_capacity(MAX_PACKET_SAMPLES);
        let decoded_samples = self
            .decoder
            .as_mut()
            .ok_or_else(|| SourceDriverError::Failed("Opus decoder is not prepared".to_owned()))?
            .decode_into(&encoded.payload, &mut samples, false)
            .map_err(|error| {
                observations
                    .decode_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                SourceDriverError::Failed(error.to_string())
            })?;
        if decoded_samples != MAX_PACKET_SAMPLES {
            observations
                .decode_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(SourceDriverError::Failed(format!(
                "unsupported decoded Opus frame size: {decoded_samples} mono samples; expected the qualified 20 ms / 960-sample profile"
            )));
        }
        let now_ns = monotonic_timestamp_ns();
        let first_rtp = *self
            .first_rtp_timestamp
            .get_or_insert(encoded.rtp_timestamp);
        let first_local = *self.first_local_timestamp_ns.get_or_insert(now_ns);
        let delta_ticks = encoded.rtp_timestamp.saturating_sub(first_rtp);
        let delta_ns = u64::try_from(
            u128::from(delta_ticks).saturating_mul(1_000_000_000)
                / u128::from(encoded.rtp_clock_hz),
        )
        .unwrap_or(u64::MAX);
        let timestamp_ns = first_local.saturating_add(delta_ns);
        let duration_ns = u64::try_from(
            (decoded_samples as u128).saturating_mul(1_000_000_000) / u128::from(SAMPLE_RATE_HZ),
        )
        .unwrap_or(u64::MAX);
        let sequence_number = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.last_delivery = Some(Instant::now());
        observations
            .latest_frame_at_ns
            .store(now_ns, Ordering::Relaxed);
        Ok(DecodedFrame {
            samples,
            timestamp_ns,
            duration_ns,
            sequence_number,
            discontinuity_epoch: self.discontinuity_epoch,
        })
    }

    fn emission_or_drop(
        &mut self,
        decoded: &DecodedFrame,
    ) -> Result<Option<SourceEmission>, SourceDriverError> {
        if self.pool.available_slots() == 0 {
            self.discontinuity_epoch = self.discontinuity_epoch.saturating_add(1);
            self.observation_state()?
                .pool_dropped_frames_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.emission(decoded).map(Some)
    }

    fn emission(&mut self, decoded: &DecodedFrame) -> Result<SourceEmission, SourceDriverError> {
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| SourceDriverError::Failed("Session context is absent".to_owned()))?;
        let output = session.output(OUTPUT_PORT).ok_or_else(|| {
            SourceDriverError::Failed("Relay AudioBus output identity is absent".to_owned())
        })?;
        let mut buffer = self.pool.acquire().ok_or_else(|| {
            SourceDriverError::Failed("Relay AudioBus audio pool is exhausted".to_owned())
        })?;
        buffer
            .try_copy_from_slice(&decoded.samples)
            .map_err(|error| SourceDriverError::Failed(error.to_string()))?;
        let sample_spec = SampleSpec::new(SAMPLE_RATE_HZ, 1, SampleFormat::F32Interleaved);
        let frame = AudioFrame::try_new(
            output.stream_id,
            session.source_id,
            decoded.sequence_number,
            decoded.timestamp_ns,
            sample_spec,
            buffer,
        )
        .map_err(|error| SourceDriverError::Failed(error.to_string()))?;
        let source_generation = self
            .config
            .as_ref()
            .ok_or_else(|| SourceDriverError::Failed("source config is absent".to_owned()))?
            .source_generation;
        let lineage = SignalLineage::try_new(
            session.session_id,
            output.stream_id,
            session.source_id,
            PROCESS_MONOTONIC_CLOCK_DOMAIN_ID,
            decoded.sequence_number,
            source_generation,
            decoded.discontinuity_epoch,
            0,
        )
        .map_err(|error| SourceDriverError::Failed(error.to_string()))?;
        let timing = SignalTiming::try_new(
            Some(decoded.timestamp_ns),
            monotonic_timestamp_ns(),
            Some(decoded.timestamp_ns),
            Some(decoded.duration_ns),
        )
        .map_err(|error| SourceDriverError::Failed(error.to_string()))?;
        let envelope = SignalEnvelope::from_audio(frame, None).with_lineage(lineage, timing);
        self.observation_state()?
            .emitted_frames_total
            .fetch_add(1, Ordering::Relaxed);
        Ok(SourceEmission {
            output_port: OUTPUT_PORT.to_owned(),
            envelope,
            terminal: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pocketstation::codec::OpusEncoder;

    fn config() -> RelayAudioBusSourceConfig {
        RelayAudioBusSourceConfig::new(
            "https://relay.example.test",
            "session-7",
            "phone-microphone",
            RelaySubscriberToken::new("top-secret"),
            4,
        )
        .unwrap()
    }

    fn decoder_driver(observations: Arc<ObservationState>) -> RelayAudioBusSourceDriver {
        RelayAudioBusSourceDriver {
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            session: None,
            config: None,
            observations: Some(observations),
            connection: None,
            decoder: Some(OpusDecoder::new().unwrap()),
            pool: AudioBufferPool::new(POOL_SLOTS, MAX_PACKET_SAMPLES),
            pending_frame: None,
            first_rtp_timestamp: None,
            first_local_timestamp_ns: None,
            previous_rtp_timestamp: None,
            next_sequence: 0,
            discontinuity_epoch: 0,
            last_delivery: None,
            seen_frame: false,
        }
    }

    fn encoded_frame(rtp_timestamp: u64, contiguous: bool) -> ReceivedOpusFrame {
        let mut encoder = OpusEncoder::new().unwrap();
        let samples = [0.125_f32; MAX_PACKET_SAMPLES];
        let mut payload = Vec::new();
        encoder.encode_into(&samples, &mut payload).unwrap();
        ReceivedOpusFrame {
            payload: payload.into(),
            rtp_timestamp,
            rtp_clock_hz: SAMPLE_RATE_HZ,
            contiguous,
        }
    }

    #[test]
    fn subscriber_token_and_config_debug_are_redacted() {
        let rendered = format!("{:?}", config());
        assert!(!rendered.contains("top-secret"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn zero_generation_is_rejected_before_session_declaration() {
        let error = RelayAudioBusSourceConfig::new(
            "https://relay.example.test",
            "session-7",
            "phone-microphone",
            RelaySubscriberToken::new("secret"),
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("source_generation"));
    }

    #[test]
    fn public_source_configuration_rejects_secret_or_runtime_fields() {
        let factory = RelayAudioBusSourceFactory::new().unwrap();
        let mut configuration = SourceConfiguration::default();
        configuration.insert("token", "must-not-live-here");
        assert!(factory.validate_config(&configuration).is_err());
    }

    #[test]
    fn observations_report_declared_generation_without_inventing_continuity() {
        let state = ObservationState::new(9);
        state.received_frames_total.store(3, Ordering::Relaxed);
        state.noncontiguous_frames_total.store(1, Ordering::Relaxed);
        let snapshot = RelayAudioBusSourceObservations { inner: state }.snapshot();
        assert_eq!(snapshot.source_generation, 9);
        assert_eq!(snapshot.received_frames_total, 3);
        assert_eq!(snapshot.noncontiguous_frames_total, 1);
    }

    #[test]
    fn noncontiguous_remote_media_advances_the_core_discontinuity_epoch() {
        let observations = ObservationState::new(3);
        let mut driver = decoder_driver(Arc::clone(&observations));
        let first = driver.decode(&encoded_frame(0, true)).unwrap();
        let after_gap = driver
            .decode(&encoded_frame(MAX_PACKET_SAMPLES as u64 * 2, false))
            .unwrap();

        assert_eq!(first.sequence_number, 0);
        assert_eq!(first.discontinuity_epoch, 0);
        assert_eq!(after_gap.sequence_number, 1);
        assert_eq!(after_gap.discontinuity_epoch, 1);
        assert_eq!(
            observations
                .noncontiguous_frames_total
                .load(Ordering::Relaxed),
            1
        );
    }
}
