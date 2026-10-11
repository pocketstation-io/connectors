//! Caller-issued capabilities for one grouped asynchronous publisher.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pocketstation::connector::ConnectorSecret;

use crate::{RelayPublicationTransport, RelayRouteConfiguration};

const MAX_CAPABILITY_LIFETIME: Duration = Duration::from_hours(24);
pub(crate) const RECOVERY_ATTEMPTS: u32 = 3;
pub(crate) const RECOVERY_BUDGET: Duration = Duration::from_secs(5);
pub(crate) const ATTACHMENT_TIMEOUT: Duration = Duration::from_secs(1);

/// Fresh credentials supplied by the caller's existing management worker.
///
/// This handle does not issue, renew, parse, or broaden a capability. It is
/// restricted to one Relay origin, Session and publisher group using WebSocket
/// ingress. All access occurs on management or ordinary connector workers.
#[derive(Clone)]
pub struct RelayPublisherControl {
    shared: Arc<SharedControl>,
}

struct SharedControl {
    configuration: RelayRouteConfiguration,
    credential: Mutex<PublisherCredential>,
    revision: AtomicU64,
    revoked: AtomicBool,
    claimed: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct PublisherCredential {
    pub(crate) token: ConnectorSecret,
    pub(crate) valid_until: Instant,
    pub(crate) revision: u64,
}

impl std::fmt::Debug for RelayPublisherControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayPublisherControl")
            .field(
                "publisher_group_id",
                &self.shared.configuration.publisher_group_id(),
            )
            .field("revision", &self.revision())
            .field("revoked", &self.is_revoked())
            .field("credential", &"<redacted>")
            .finish()
    }
}

impl RelayPublisherControl {
    /// Binds the initial capability and its caller-validated monotonic expiry.
    ///
    /// # Errors
    /// Rejects non-WebSocket publication and expiry outside the next 24 hours.
    pub fn new(
        configuration: &RelayRouteConfiguration,
        valid_until: Instant,
    ) -> Result<Self, RelayPublisherControlError> {
        Self::new_with_revision(configuration, valid_until, 1)
    }

    /// Binds a capability already renewed by the caller's management worker.
    ///
    /// # Errors
    /// Rejects zero revision, non-WebSocket publication and invalid expiry.
    pub fn new_with_revision(
        configuration: &RelayRouteConfiguration,
        valid_until: Instant,
        revision: u64,
    ) -> Result<Self, RelayPublisherControlError> {
        if revision == 0 {
            return Err(RelayPublisherControlError::StaleReplacement);
        }
        if configuration.transport != RelayPublicationTransport::WebSocket {
            return Err(RelayPublisherControlError::UnsupportedTransport);
        }
        validate_expiry(valid_until)?;
        Ok(Self {
            shared: Arc::new(SharedControl {
                configuration: configuration.clone(),
                credential: Mutex::new(PublisherCredential {
                    token: configuration.source_token.clone(),
                    valid_until,
                    revision,
                }),
                revision: AtomicU64::new(revision),
                revoked: AtomicBool::new(false),
                claimed: AtomicBool::new(false),
            }),
        })
    }

    /// Replaces credentials after an actual successful management renewal.
    ///
    /// # Errors
    /// Rejects expired deadlines, stale revisions, non-extending expiry, and
    /// replacement after terminal revocation. Secret values are never reported.
    pub fn replace(
        &self,
        revision: u64,
        source_token: ConnectorSecret,
        valid_until: Instant,
    ) -> Result<(), RelayPublisherControlError> {
        validate_expiry(valid_until)?;
        let mut credential = self
            .shared
            .credential
            .lock()
            .map_err(|_| RelayPublisherControlError::Unavailable)?;
        if self.is_revoked() {
            return Err(RelayPublisherControlError::Revoked);
        }
        if revision <= credential.revision || valid_until <= credential.valid_until {
            return Err(RelayPublisherControlError::StaleReplacement);
        }
        *credential = PublisherCredential {
            token: source_token,
            valid_until,
            revision,
        };
        self.shared.revision.store(revision, Ordering::Release);
        Ok(())
    }

    /// Permanently fences this publisher; a new owner requires a new handle.
    pub fn revoke(&self) {
        self.shared.revoked.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.shared.revision.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_revoked(&self) -> bool {
        self.shared.revoked.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot(&self) -> Result<PublisherCredential, RelayPublisherControlError> {
        let credential = self
            .shared
            .credential
            .lock()
            .map_err(|_| RelayPublisherControlError::Unavailable)?
            .clone();
        if self.is_revoked() {
            return Err(RelayPublisherControlError::Revoked);
        }
        validate_expiry(credential.valid_until)?;
        Ok(credential)
    }

    pub(crate) fn matches(&self, configuration: &RelayRouteConfiguration) -> bool {
        let bound = &self.shared.configuration;
        bound.relay_url == configuration.relay_url
            && bound.relay_session_id == configuration.relay_session_id
            && bound.publisher_group_id == configuration.publisher_group_id
            && configuration.transport == RelayPublicationTransport::WebSocket
    }

    pub(crate) fn claim(&self) -> Result<PublisherLease, RelayPublisherControlError> {
        self.shared
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RelayPublisherControlError::AlreadyPublishing)?;
        Ok(PublisherLease(self.clone()))
    }
}

pub(crate) struct PublisherLease(RelayPublisherControl);

impl Drop for PublisherLease {
    fn drop(&mut self) {
        self.0.shared.claimed.store(false, Ordering::Release);
    }
}

fn validate_expiry(valid_until: Instant) -> Result<(), RelayPublisherControlError> {
    valid_until
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero() && *duration <= MAX_CAPABILITY_LIFETIME)
        .ok_or(RelayPublisherControlError::InvalidExpiry)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum RelayPublisherControlError {
    #[error("renewable publication requires WebSocket ingress")]
    UnsupportedTransport,
    #[error("publisher capability expiry must be in the next 24 hours")]
    InvalidExpiry,
    #[error("publisher replacement revision and expiry must strictly advance")]
    StaleReplacement,
    #[error("publisher capability was terminally revoked")]
    Revoked,
    #[error("publisher credential state is unavailable")]
    Unavailable,
    #[error("publisher control already has an active grouped publication")]
    AlreadyPublishing,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> RelayRouteConfiguration {
        RelayRouteConfiguration::new(
            "http://127.0.0.1:1",
            "session",
            ConnectorSecret::new("initial-secret").unwrap(),
            "application",
        )
        .unwrap()
        .with_websocket_ingress()
        .unwrap()
    }

    #[test]
    fn given_fresh_capability_when_replaced_then_stale_expired_and_revoked_updates_fail_closed() {
        let expiry = Instant::now() + Duration::from_secs(5);
        let control = RelayPublisherControl::new(&configuration(), expiry).unwrap();
        let replacement = ConnectorSecret::new("replacement-secret").unwrap();
        control
            .replace(2, replacement.clone(), expiry + Duration::from_secs(1))
            .unwrap();
        assert_eq!(control.snapshot().unwrap().revision, 2);
        assert!(control
            .replace(2, replacement.clone(), expiry + Duration::from_secs(2))
            .is_err());
        assert!(control.replace(3, replacement.clone(), expiry).is_err());
        assert!(control
            .replace(3, replacement.clone(), Instant::now())
            .is_err());
        assert!(!format!("{control:?}").contains("secret"));
        control.revoke();
        assert!(control.snapshot().is_err());
        assert!(control
            .replace(3, replacement, expiry + Duration::from_secs(2))
            .is_err());
    }

    #[test]
    fn given_control_when_claimed_then_other_sessions_cannot_compete_and_cancel_releases_claim() {
        let control =
            RelayPublisherControl::new(&configuration(), Instant::now() + Duration::from_secs(5))
                .unwrap();
        let lease = control.claim().unwrap();
        assert!(control.claim().is_err());
        drop(lease);
        assert!(control.claim().is_ok());
        assert!(control.matches(&configuration()));
        assert!(!control.matches(&configuration().with_publisher_group("other").unwrap()));
        assert!(RelayPublisherControl::new(&configuration(), Instant::now()).is_err());
    }
}
