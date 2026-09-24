use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use parking_lot::Mutex;
use tonic::{Request, Status};
use tracing::warn;

/// Validates the optional static token used by all Geyser RPC methods.
#[derive(Clone)]
pub(crate) struct TokenAuth {
    expected: Option<String>,
}

impl TokenAuth {
    pub(crate) fn new(expected: Option<String>) -> Self {
        Self { expected }
    }

    pub(crate) fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected) = &self.expected else {
            return Ok(());
        };
        let authorized = request
            .metadata()
            .get("x-token")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|actual| actual == expected);
        if authorized {
            metrics::counter!("plugin.grpc.auth.authorized.total").increment(1);
            Ok(())
        } else {
            metrics::counter!("plugin.grpc.auth.unauthorized.total").increment(1);
            Err(Status::unauthenticated("missing or invalid x-token"))
        }
    }
}

/// Enforces a bounded number of live streams for each subscriber identity.
#[derive(Clone)]
pub(crate) struct SubscriptionTracker {
    counts: Arc<Mutex<HashMap<String, usize>>>,
    limit: NonZeroUsize,
    enforce: bool,
}

impl SubscriptionTracker {
    pub(crate) fn new(limit: NonZeroUsize, enforce: bool) -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            limit,
            enforce,
        }
    }

    pub(crate) fn acquire<T>(&self, request: &Request<T>) -> Result<SubscriptionGuard, Status> {
        let subscriber_id = request
            .metadata()
            .get("x-subscription-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                request
                    .remote_addr()
                    .map(|address| address.ip().to_string())
            })
            .unwrap_or_else(|| "anonymous".to_owned());

        let mut counts = self.counts.lock();
        let count = counts.entry(subscriber_id.clone()).or_default();
        if *count >= self.limit.get() {
            metrics::counter!("plugin.grpc.subscription_limit_exceeded.total").increment(1);
            if self.enforce {
                return Err(Status::resource_exhausted(format!(
                    "subscriber {subscriber_id:?} reached the concurrent subscription limit ({})",
                    self.limit
                )));
            }
            warn!(
                %subscriber_id,
                limit = self.limit.get(),
                "gRPC subscriber exceeded the configured stream limit"
            );
        }
        *count = count.saturating_add(1);
        metrics::gauge!("plugin.grpc.connections.active").increment(1.0);

        Ok(SubscriptionGuard {
            counts: Arc::clone(&self.counts),
            subscriber_id,
        })
    }
}

#[derive(Debug)]
pub(crate) struct SubscriptionGuard {
    counts: Arc<Mutex<HashMap<String, usize>>>,
    subscriber_id: String,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let mut counts = self.counts.lock();
        if let Some(count) = counts.get_mut(&self.subscriber_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.subscriber_id);
            }
        }
        metrics::gauge!("plugin.grpc.connections.active").decrement(1.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforced_limit_is_released_with_guard() {
        let tracker = SubscriptionTracker::new(NonZeroUsize::new(1).unwrap(), true);
        let request = Request::new(());
        let guard = tracker.acquire(&request).unwrap();
        assert_eq!(
            tracker.acquire(&request).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        drop(guard);
        assert!(tracker.acquire(&request).is_ok());
    }

    #[test]
    fn token_is_required_when_configured() {
        let auth = TokenAuth::new(Some("secret".to_owned()));
        assert_eq!(
            auth.authorize(&Request::new(())).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-token", "secret".parse().unwrap());
        assert!(auth.authorize(&request).is_ok());
    }
}
