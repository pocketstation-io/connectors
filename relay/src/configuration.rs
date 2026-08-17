use std::time::Duration;

use pocketstation::connector::{
    ConnectorConfiguration, ConnectorConfigurationError, ConnectorConfigurationValue,
    ConnectorSecret,
};
use pocketstation::graph::NodeConfig;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::frame_publisher::validate_bus;

pub const MAX_ICE_SERVERS: usize = 16;
pub const MAX_ICE_SERVER_URLS: usize = 8;
pub(crate) const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 30_000;
pub(crate) const MAX_STARTUP_TIMEOUT_MS: u64 = 120_000;

pub(crate) const RELAY_URL_KEY: &str = "relay_url";
pub(crate) const RELAY_SESSION_ID_KEY: &str = "relay_session_id";
pub(crate) const SOURCE_TOKEN_KEY: &str = "source_token";
pub(crate) const BUS_ID_KEY: &str = "bus_id";
pub(crate) const PUBLISHER_GROUP_ID_KEY: &str = "publisher_group_id";
pub(crate) const LATENCY_PROFILE_KEY: &str = "latency_profile";
pub(crate) const ICE_SERVERS_KEY: &str = "ice_servers";
pub(crate) const STARTUP_TIMEOUT_MS_KEY: &str = "startup_timeout_ms";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayLatencyProfile {
    Standard,
    Low,
}

impl RelayLatencyProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Low => "low",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, RelayConfigurationError> {
        match value {
            "standard" => Ok(Self::Standard),
            "low" => Ok(Self::Low),
            _ => Err(RelayConfigurationError::InvalidLatencyProfile),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayIceServer {
    urls: Vec<String>,
}

impl std::fmt::Debug for RelayIceServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayIceServer")
            .field("urls", &self.urls)
            .finish()
    }
}

impl RelayIceServer {
    /// Creates a bounded list of STUN server URLs used for server-reflexive discovery.
    ///
    /// # Errors
    ///
    /// Returns an error when the list is empty, exceeds the package limit, or
    /// contains anything other than a valid `stun:` authority.
    pub fn new(
        urls: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, RelayConfigurationError> {
        let server = Self {
            urls: urls.into_iter().map(Into::into).collect(),
        };
        server.validate()?;
        Ok(server)
    }

    #[must_use]
    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    fn validate(&self) -> Result<(), RelayConfigurationError> {
        if self.urls.is_empty() || self.urls.len() > MAX_ICE_SERVER_URLS {
            return Err(RelayConfigurationError::InvalidIceServerCount);
        }
        for raw in &self.urls {
            if stun_authority(raw).is_none() {
                return Err(RelayConfigurationError::InvalidIceServerUrl);
            }
        }
        Ok(())
    }

    pub(crate) fn stun_authorities(&self) -> impl Iterator<Item = &str> {
        self.urls.iter().filter_map(|url| stun_authority(url))
    }
}

fn stun_authority(raw: &str) -> Option<&str> {
    let authority = raw
        .strip_prefix("stun:")?
        .strip_prefix("//")
        .unwrap_or_else(|| {
            raw.strip_prefix("stun:")
                .expect("prefix was checked immediately above")
        });
    if authority.is_empty()
        || authority
            .chars()
            .any(|character| matches!(character, '/' | '?' | '#' | '@'))
        || authority.trim() != authority
    {
        return None;
    }
    Some(authority)
}

#[derive(Clone)]
pub struct RelayRouteConfiguration {
    pub(crate) relay_url: String,
    pub(crate) relay_session_id: String,
    pub(crate) source_token: ConnectorSecret,
    pub(crate) bus_id: String,
    pub(crate) publisher_group_id: String,
    pub(crate) latency_profile: RelayLatencyProfile,
    pub(crate) ice_servers: Vec<RelayIceServer>,
    pub(crate) startup_timeout: Duration,
}

impl std::fmt::Debug for RelayRouteConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayRouteConfiguration")
            .field("relay_url", &self.relay_url)
            .field("relay_session_id", &self.relay_session_id)
            .field("source_token", &"<redacted>")
            .field("bus_id", &self.bus_id)
            .field("publisher_group_id", &self.publisher_group_id)
            .field("latency_profile", &self.latency_profile)
            .field("ice_servers", &self.ice_servers)
            .field("startup_timeout", &self.startup_timeout)
            .finish()
    }
}

impl RelayRouteConfiguration {
    /// Creates the configuration for one named Relay `AudioBus` route.
    ///
    /// # Errors
    ///
    /// Returns an error when the origin, Session identity, token, bus, or
    /// default startup contract is invalid.
    pub fn new(
        relay_url: impl Into<String>,
        relay_session_id: impl Into<String>,
        source_token: ConnectorSecret,
        bus_id: impl Into<String>,
    ) -> Result<Self, RelayConfigurationError> {
        let relay_session_id = relay_session_id.into();
        let configuration = Self {
            relay_url: relay_url.into(),
            publisher_group_id: relay_session_id.clone(),
            relay_session_id,
            source_token,
            bus_id: bus_id.into(),
            latency_profile: RelayLatencyProfile::Standard,
            ice_servers: Vec::new(),
            startup_timeout: Duration::from_millis(DEFAULT_STARTUP_TIMEOUT_MS),
        };
        configuration.validate()?;
        Ok(configuration)
    }

    /// Selects the Session-scoped group that shares one publisher lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error when the group identifier is empty or too large.
    pub fn with_publisher_group(
        mut self,
        publisher_group_id: impl Into<String>,
    ) -> Result<Self, RelayConfigurationError> {
        self.publisher_group_id = publisher_group_id.into();
        self.validate()?;
        Ok(self)
    }

    /// Adds the finite STUN configuration used by the current publisher transport.
    ///
    /// # Errors
    ///
    /// Returns an error when a server or the aggregate server list is invalid.
    pub fn with_ice_servers(
        mut self,
        ice_servers: impl IntoIterator<Item = RelayIceServer>,
    ) -> Result<Self, RelayConfigurationError> {
        self.ice_servers = ice_servers.into_iter().collect();
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub fn with_low_latency(mut self) -> Self {
        self.latency_profile = RelayLatencyProfile::Low;
        self
    }

    /// Sets the complete signaling, ICE, and DTLS startup deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the deadline is zero or exceeds 120 seconds.
    pub fn with_startup_timeout(
        mut self,
        startup_timeout: Duration,
    ) -> Result<Self, RelayConfigurationError> {
        self.startup_timeout = startup_timeout;
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub fn bus_id(&self) -> &str {
        &self.bus_id
    }

    #[must_use]
    pub fn publisher_group_id(&self) -> &str {
        &self.publisher_group_id
    }

    pub(crate) fn same_publisher(&self, other: &Self) -> bool {
        self.relay_url == other.relay_url
            && self.relay_session_id == other.relay_session_id
            && self.source_token == other.source_token
            && self.publisher_group_id == other.publisher_group_id
            && self.ice_servers == other.ice_servers
            && self.startup_timeout == other.startup_timeout
    }

    /// Converts this typed value into Core's secret-aware connector configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when validation or the bounded secret encoding fails.
    pub fn connector_configuration(
        &self,
    ) -> Result<ConnectorConfiguration, RelayConfigurationError> {
        self.validate()?;
        let mut configuration = ConnectorConfiguration::new()
            .with(
                RELAY_URL_KEY,
                ConnectorConfigurationValue::Text(self.relay_url.clone()),
            )
            .with(
                RELAY_SESSION_ID_KEY,
                ConnectorConfigurationValue::Text(self.relay_session_id.clone()),
            )
            .with(
                SOURCE_TOKEN_KEY,
                ConnectorConfigurationValue::Secret(self.source_token.clone()),
            )
            .with(
                BUS_ID_KEY,
                ConnectorConfigurationValue::Text(self.bus_id.clone()),
            )
            .with(
                PUBLISHER_GROUP_ID_KEY,
                ConnectorConfigurationValue::Text(self.publisher_group_id.clone()),
            )
            .with(
                LATENCY_PROFILE_KEY,
                ConnectorConfigurationValue::Text(self.latency_profile.as_str().to_owned()),
            )
            .with(
                STARTUP_TIMEOUT_MS_KEY,
                ConnectorConfigurationValue::DurationMilliseconds(
                    u64::try_from(self.startup_timeout.as_millis())
                        .map_err(|_| RelayConfigurationError::InvalidStartupTimeout)?,
                ),
            );
        if !self.ice_servers.is_empty() {
            configuration.insert(
                ICE_SERVERS_KEY,
                ConnectorConfigurationValue::Secret(ConnectorSecret::new(encode_ice_servers(
                    &self.ice_servers,
                )?)?),
            );
        }
        Ok(configuration)
    }

    pub(crate) fn from_node_config(
        configuration: &NodeConfig,
    ) -> Result<Self, RelayConfigurationError> {
        let source_token = required_sensitive(configuration, SOURCE_TOKEN_KEY)?;
        let startup_timeout_ms = configuration
            .get(STARTUP_TIMEOUT_MS_KEY)
            .unwrap_or("30000")
            .parse::<u64>()
            .map_err(|_| RelayConfigurationError::InvalidStartupTimeout)?;
        let ice_servers = configuration
            .get(ICE_SERVERS_KEY)
            .map_or(Ok(Vec::new()), decode_ice_servers)?;
        let result = Self {
            relay_url: required(configuration, RELAY_URL_KEY)?.to_owned(),
            relay_session_id: required(configuration, RELAY_SESSION_ID_KEY)?.to_owned(),
            source_token: ConnectorSecret::new(source_token)?,
            bus_id: required(configuration, BUS_ID_KEY)?.to_owned(),
            publisher_group_id: required(configuration, PUBLISHER_GROUP_ID_KEY)?.to_owned(),
            latency_profile: RelayLatencyProfile::parse(
                configuration.get(LATENCY_PROFILE_KEY).unwrap_or("standard"),
            )?,
            ice_servers,
            startup_timeout: Duration::from_millis(startup_timeout_ms),
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), RelayConfigurationError> {
        let relay =
            Url::parse(&self.relay_url).map_err(|_| RelayConfigurationError::InvalidRelayUrl)?;
        if !matches!(relay.scheme(), "http" | "https")
            || relay.host_str().is_none()
            || relay.cannot_be_a_base()
            || !matches!(relay.path(), "" | "/")
            || relay.query().is_some()
            || relay.fragment().is_some()
        {
            return Err(RelayConfigurationError::InvalidRelayUrl);
        }
        if self.relay_session_id.trim().is_empty()
            || self.publisher_group_id.trim().is_empty()
            || self.publisher_group_id.len() > 256
        {
            return Err(RelayConfigurationError::InvalidIdentity);
        }
        validate_bus(&self.bus_id).map_err(|_| RelayConfigurationError::InvalidBusId)?;
        if self.ice_servers.len() > MAX_ICE_SERVERS {
            return Err(RelayConfigurationError::InvalidIceServerCount);
        }
        for server in &self.ice_servers {
            server.validate()?;
        }
        let startup_timeout_ms = self.startup_timeout.as_millis();
        if startup_timeout_ms == 0 || startup_timeout_ms > u128::from(MAX_STARTUP_TIMEOUT_MS) {
            return Err(RelayConfigurationError::InvalidStartupTimeout);
        }
        Ok(())
    }
}

fn required<'a>(
    configuration: &'a NodeConfig,
    key: &'static str,
) -> Result<&'a str, RelayConfigurationError> {
    configuration
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or(RelayConfigurationError::MissingField(key))
}

fn required_sensitive<'a>(
    configuration: &'a NodeConfig,
    key: &'static str,
) -> Result<&'a str, RelayConfigurationError> {
    let value = required(configuration, key)?;
    if !configuration.is_sensitive(key) {
        return Err(RelayConfigurationError::SecretNotSensitive(key));
    }
    Ok(value)
}

#[derive(Serialize, Deserialize)]
struct WireIceServer {
    urls: Vec<String>,
    username: Option<String>,
    credential: Option<String>,
}

fn encode_ice_servers(servers: &[RelayIceServer]) -> Result<String, RelayConfigurationError> {
    let wire = servers
        .iter()
        .map(|server| WireIceServer {
            urls: server.urls.clone(),
            username: None,
            credential: None,
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&wire).map_err(|_| RelayConfigurationError::InvalidIceServerEncoding)
}

fn decode_ice_servers(encoded: &str) -> Result<Vec<RelayIceServer>, RelayConfigurationError> {
    let wire: Vec<WireIceServer> = serde_json::from_str(encoded)
        .map_err(|_| RelayConfigurationError::InvalidIceServerEncoding)?;
    if wire.len() > MAX_ICE_SERVERS {
        return Err(RelayConfigurationError::InvalidIceServerCount);
    }
    wire.into_iter()
        .map(|server| {
            if server.username.is_some() || server.credential.is_some() {
                return Err(RelayConfigurationError::InvalidIceCredential);
            }
            RelayIceServer::new(server.urls)
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum RelayConfigurationError {
    #[error("relay URL must be an absolute HTTP or HTTPS origin")]
    InvalidRelayUrl,
    #[error("relay Session and publisher group identifiers must be non-empty and bounded")]
    InvalidIdentity,
    #[error("AudioBus identifier is invalid")]
    InvalidBusId,
    #[error("relay latency profile must be 'standard' or 'low'")]
    InvalidLatencyProfile,
    #[error("relay startup timeout must be finite and between 1 ms and 120 s")]
    InvalidStartupTimeout,
    #[error("ICE server count is outside the finite package limit")]
    InvalidIceServerCount,
    #[error("this connector version supports only a valid stun: UDP server authority")]
    InvalidIceServerUrl,
    #[error("STUN server configuration must not contain TURN credentials")]
    InvalidIceCredential,
    #[error("ICE server configuration could not be encoded or decoded")]
    InvalidIceServerEncoding,
    #[error("relay connector configuration is missing required field '{0}'")]
    MissingField(&'static str),
    #[error("relay connector secret field '{0}' was not marked sensitive")]
    SecretNotSensitive(&'static str),
    #[error(transparent)]
    ConnectorConfiguration(#[from] ConnectorConfigurationError),
}
