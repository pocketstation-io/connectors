use std::sync::Arc;

use pocketstation::connector::{
    Connector, ConnectorCapability, ConnectorConfigurationConstraint, ConnectorConfigurationError,
    ConnectorConfigurationField, ConnectorConfigurationRequirement, ConnectorConfigurationSchema,
    ConnectorConfigurationValue, ConnectorConfigurationValueKind, ConnectorManifest,
    ConnectorManifestError, ConnectorReadinessPolicy, ConnectorReadinessPolicyError,
    ConnectorRegistrationError, ConnectorRequirement, RegisteredConnector,
};
use pocketstation::graph::{
    AudioCaps, ChannelLayout, ExecutionPartition, MediaCaps, Multiplicity, NodeDescriptor,
    NodeTypeId, PortDirection, PortSpec, SafetyContract, SignalSpec,
};
use pocketstation::{OperatorId, SampleFormat, Session};

use crate::configuration::{
    BUS_ID_KEY, DEFAULT_STARTUP_TIMEOUT_MS, ICE_SERVERS_KEY, LATENCY_PROFILE_KEY,
    MAX_STARTUP_TIMEOUT_MS, PUBLISHER_GROUP_ID_KEY, RELAY_SESSION_ID_KEY, RELAY_URL_KEY,
    SOURCE_TOKEN_KEY, STARTUP_TIMEOUT_MS_KEY,
};
use crate::runtime::{
    RelayConnectorFactory, RelayPublishReceipt, RelayPublishReceiptKey, RelayPublishResult,
};

pub const RELAY_CONNECTOR_OPERATOR_ID: &str = "io.pocketstation.relay.publish.v1";
pub const RELAY_CONNECTOR_NODE_TYPE_ID: &str = "io.pocketstation.relay.publish.node.v1";
const DEFAULT_RECEIPT_CAPACITY: usize = 64;

pub struct RelayConnector {
    factory: Arc<RelayConnectorFactory>,
}

impl RelayConnector {
    /// Creates a Relay connector with the default bounded receipt capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if the built-in connector contract is invalid.
    pub fn new() -> Result<Self, RelayConnectorBuildError> {
        Self::with_receipt_capacity(DEFAULT_RECEIPT_CAPACITY)
    }

    /// Creates a Relay connector with an explicit bounded receipt capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the capacity is zero or the connector contract is invalid.
    pub fn with_receipt_capacity(
        receipt_capacity: usize,
    ) -> Result<Self, RelayConnectorBuildError> {
        Ok(Self {
            factory: Arc::new(RelayConnectorFactory::new(receipt_capacity)?),
        })
    }

    /// Builds the SDK-neutral Core connector value.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be validated.
    pub fn connector(&self) -> Result<Connector, RelayConnectorBuildError> {
        Ok(Connector::new(
            relay_connector_manifest()?,
            self.factory.clone(),
        )?)
    }

    /// Registers this connector with one `Session`.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest or Session registration is invalid.
    pub fn register(
        &self,
        session: &Session,
    ) -> Result<RegisteredConnector, RelayConnectorBuildError> {
        Ok(session.register_connector(self.connector()?)?)
    }

    #[must_use]
    pub fn receipt(&self, key: RelayPublishReceiptKey) -> Option<RelayPublishReceipt> {
        self.factory.receipt(key)
    }

    #[must_use]
    pub fn take_result(&self, key: RelayPublishReceiptKey) -> Option<RelayPublishResult> {
        self.factory.take_result(key)
    }
}

impl Default for RelayConnector {
    fn default() -> Self {
        Self::new().expect("default relay connector contract must remain valid")
    }
}

/// Builds the versioned Relay connector manifest.
///
/// # Errors
///
/// Returns an error if its graph, configuration, readiness, or metadata contract is invalid.
#[allow(clippy::too_many_lines)]
pub fn relay_connector_manifest() -> Result<ConnectorManifest, RelayConnectorBuildError> {
    let media = MediaCaps::Audio(AudioCaps {
        sample_rate_hz: Some(48_000),
        frame_samples: None,
        channel_layout: ChannelLayout::Any,
        format: SampleFormat::F32Interleaved,
    });
    let input = PortSpec::new(
        "audio",
        PortDirection::Input,
        SignalSpec::audio(),
        media,
        Multiplicity::Many,
        true,
    )
    .map_err(|error| RelayConnectorBuildError::GraphContract(error.to_string()))?;
    let node = NodeDescriptor::new(
        NodeTypeId::from(RELAY_CONNECTOR_NODE_TYPE_ID),
        "PocketStation Relay publisher",
        vec![input],
        Vec::new(),
        ExecutionPartition::AsyncWorker,
        SafetyContract::NetworkAllowed,
        true,
    )
    .map_err(|error| RelayConnectorBuildError::GraphContract(error.to_string()))?;
    let configuration = ConnectorConfigurationSchema::new(
        1,
        vec![
            required_text(
                RELAY_URL_KEY,
                "Absolute PocketStation Relay HTTP or HTTPS origin.",
            ),
            required_text(
                RELAY_SESSION_ID_KEY,
                "Authoritative RelaySession identifier returned by the control plane.",
            ),
            ConnectorConfigurationField::new(
                SOURCE_TOKEN_KEY,
                ConnectorConfigurationValueKind::Secret,
                ConnectorConfigurationRequirement::Required,
                "RelaySession source credential. This field is always redacted.",
            )
            .with_constraint(ConnectorConfigurationConstraint::NonEmpty),
            required_text(BUS_ID_KEY, "Named AudioBus published by this route."),
            required_text(
                PUBLISHER_GROUP_ID_KEY,
                "Session-scoped group whose AudioBuses share one lifecycle.",
            ),
            ConnectorConfigurationField::new(
                LATENCY_PROFILE_KEY,
                ConnectorConfigurationValueKind::Text,
                ConnectorConfigurationRequirement::Default(ConnectorConfigurationValue::Text(
                    "standard".to_owned(),
                )),
                "Bounded queue policy: standard preserves queued music; low favors freshness.",
            )
            .with_constraint(ConnectorConfigurationConstraint::OneOf(vec![
                "standard".to_owned(),
                "low".to_owned(),
            ])),
            ConnectorConfigurationField::new(
                ICE_SERVERS_KEY,
                ConnectorConfigurationValueKind::Secret,
                ConnectorConfigurationRequirement::Optional,
                "Bounded JSON projection of STUN servers. TURN credentials are rejected by connector version 0.1.",
            )
            .with_constraint(ConnectorConfigurationConstraint::NonEmpty),
            ConnectorConfigurationField::new(
                STARTUP_TIMEOUT_MS_KEY,
                ConnectorConfigurationValueKind::DurationMilliseconds,
                ConnectorConfigurationRequirement::Default(
                    ConnectorConfigurationValue::DurationMilliseconds(DEFAULT_STARTUP_TIMEOUT_MS),
                ),
                "One finite deadline covering signaling, ICE and DTLS startup.",
            )
            .with_constraint(ConnectorConfigurationConstraint::UnsignedRange {
                minimum: 1,
                maximum: MAX_STARTUP_TIMEOUT_MS,
            }),
        ],
    )?;
    let manifest = ConnectorManifest::new(
        1,
        OperatorId::new(RELAY_CONNECTOR_OPERATOR_ID),
        env!("CARGO_PKG_VERSION"),
        node,
        configuration,
        ConnectorReadinessPolicy::new(
            std::time::Duration::from_millis(DEFAULT_STARTUP_TIMEOUT_MS),
            std::time::Duration::from_millis(100),
            1,
            3,
        )?,
    )?
    .with_capability(ConnectorCapability::new(
        "audio.pcm-f32-to-opus-rtp",
        "Consumes bounded 48 kHz interleaved f32 PCM and publishes Opus over RTP.",
    )?)
    .with_capability(ConnectorCapability::new(
        "relay.grouped-audio-buses",
        "Publishes multiple named AudioBuses under one transactional connector lifecycle.",
    )?)
    .with_requirement(ConnectorRequirement::new(
        "network.udp",
        true,
        "Requires UDP reachability to the selected Relay media plane.",
    )?)
    .with_requirement(ConnectorRequirement::new(
        "network.turn-not-supported",
        false,
        "Connector version 0.1 does not allocate TURN relays; networks requiring TURN are unsupported.",
    )?);
    manifest.validate()?;
    Ok(manifest)
}

fn required_text(name: &'static str, documentation: &'static str) -> ConnectorConfigurationField {
    ConnectorConfigurationField::new(
        name,
        ConnectorConfigurationValueKind::Text,
        ConnectorConfigurationRequirement::Required,
        documentation,
    )
    .with_constraint(ConnectorConfigurationConstraint::NonEmpty)
}

#[derive(Debug, thiserror::Error)]
pub enum RelayConnectorBuildError {
    #[error("invalid relay graph contract: {0}")]
    GraphContract(String),
    #[error(transparent)]
    Connector(#[from] pocketstation::connector::ConnectorError),
    #[error(transparent)]
    Configuration(#[from] ConnectorConfigurationError),
    #[error(transparent)]
    Readiness(#[from] ConnectorReadinessPolicyError),
    #[error(transparent)]
    Manifest(#[from] ConnectorManifestError),
    #[error(transparent)]
    Registration(#[from] ConnectorRegistrationError),
}
