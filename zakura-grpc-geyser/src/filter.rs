use std::collections::HashSet;

use tonic::Status;
use zakura_grpc_proto::geyser::{
    EventType, SubscribeRequest, SubscribeRequestFilter, SubscribeUpdate,
};

use crate::{config::FilterLimits, event::block_height};

/// A validated subscription filter compiled for the live hot path.
#[derive(Debug)]
pub(crate) struct EventFilter {
    default_all: bool,
    legacy: Option<CompiledFilter>,
    named: Vec<(String, CompiledFilter)>,
}

impl EventFilter {
    pub(crate) fn new(request: &SubscribeRequest, limits: &FilterLimits) -> Result<Self, Status> {
        if request.filters.len() > limits.max_named_filters {
            return Err(Status::invalid_argument(format!(
                "at most {} named filters are allowed",
                limits.max_named_filters
            )));
        }

        let default_all = request.event_types.is_empty() && request.filters.is_empty();
        if default_all && !limits.allow_all {
            return Err(Status::invalid_argument(
                "an empty filter is disabled; select at least one event type",
            ));
        }

        let legacy = (!request.event_types.is_empty())
            .then(|| CompiledFilter::new(&request.event_types, None, limits))
            .transpose()?;

        let mut named = Vec::with_capacity(request.filters.len());
        for (name, filter) in &request.filters {
            validate_name(name, limits)?;
            if filter.event_types.is_empty() && !limits.allow_all {
                return Err(Status::invalid_argument(format!(
                    "named filter {name:?} must select at least one event type"
                )));
            }
            named.push((name.clone(), CompiledFilter::from_proto(filter, limits)?));
        }
        named.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        Ok(Self {
            default_all,
            legacy,
            named,
        })
    }

    /// Returns filter names to attach to an update, or `None` when it must be skipped.
    pub(crate) fn matched_names(&self, update: &SubscribeUpdate) -> Option<Vec<String>> {
        let legacy_matches = self
            .legacy
            .as_ref()
            .is_some_and(|filter| filter.matches(update));
        let names: Vec<_> = self
            .named
            .iter()
            .filter(|(_, filter)| filter.matches(update))
            .map(|(name, _)| name.clone())
            .collect();

        (self.default_all || legacy_matches || !names.is_empty()).then_some(names)
    }
}

#[derive(Debug)]
struct CompiledFilter {
    kinds: HashSet<i32>,
    min_height: Option<u32>,
}

impl CompiledFilter {
    fn from_proto(filter: &SubscribeRequestFilter, limits: &FilterLimits) -> Result<Self, Status> {
        Self::new(&filter.event_types, filter.min_height, limits)
    }

    fn new(
        event_types: &[i32],
        min_height: Option<u32>,
        limits: &FilterLimits,
    ) -> Result<Self, Status> {
        if event_types.len() > limits.max_event_types {
            return Err(Status::invalid_argument(format!(
                "at most {} event types are allowed per filter",
                limits.max_event_types
            )));
        }

        let mut kinds = HashSet::with_capacity(event_types.len());
        for kind in event_types {
            let parsed = EventType::try_from(*kind)
                .map_err(|_| Status::invalid_argument(format!("unknown event type {kind}")))?;
            if parsed == EventType::Unspecified {
                return Err(Status::invalid_argument(
                    "EVENT_TYPE_UNSPECIFIED cannot be used as a filter",
                ));
            }
            kinds.insert(*kind);
        }

        Ok(Self { kinds, min_height })
    }

    fn matches(&self, update: &SubscribeUpdate) -> bool {
        let kind_matches = self.kinds.is_empty() || self.kinds.contains(&update.event_type);
        let height_matches = self
            .min_height
            .is_none_or(|minimum| block_height(update).is_some_and(|height| height >= minimum));
        kind_matches && height_matches
    }
}

fn validate_name(name: &str, limits: &FilterLimits) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("filter names cannot be empty"));
    }
    if name.len() > limits.max_name_bytes {
        return Err(Status::invalid_argument(format!(
            "filter name is longer than {} bytes",
            limits.max_name_bytes
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use zakura_grpc_proto::geyser::{subscribe_update, BlockUpdate};

    use super::*;

    fn update(height: u32, kind: EventType) -> SubscribeUpdate {
        SubscribeUpdate {
            event_type: kind.into(),
            update: Some(subscribe_update::Update::Block(BlockUpdate {
                height,
                ..BlockUpdate::default()
            })),
            ..SubscribeUpdate::default()
        }
    }

    #[test]
    fn empty_request_matches_everything() {
        let filter =
            EventFilter::new(&SubscribeRequest::default(), &FilterLimits::default()).unwrap();
        assert_eq!(
            filter.matched_names(&update(1, EventType::BlockFinalized)),
            Some(Vec::new())
        );
    }

    #[test]
    fn named_filters_return_every_match_in_stable_order() {
        let filters = HashMap::from([
            (
                "tip".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::BlockFinalized.into()],
                    min_height: Some(10),
                },
            ),
            (
                "blocks".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::BlockFinalized.into()],
                    min_height: None,
                },
            ),
        ]);
        let filter = EventFilter::new(
            &SubscribeRequest {
                filters,
                ..SubscribeRequest::default()
            },
            &FilterLimits::default(),
        )
        .unwrap();

        assert_eq!(
            filter.matched_names(&update(10, EventType::BlockFinalized)),
            Some(vec!["blocks".to_owned(), "tip".to_owned()])
        );
        assert_eq!(
            filter.matched_names(&update(9, EventType::BlockFinalized)),
            Some(vec!["blocks".to_owned()])
        );
    }

    #[test]
    fn limits_reject_excess_named_filters() {
        let request = SubscribeRequest {
            filters: HashMap::from([
                ("one".to_owned(), SubscribeRequestFilter::default()),
                ("two".to_owned(), SubscribeRequestFilter::default()),
            ]),
            ..SubscribeRequest::default()
        };
        let limits = FilterLimits {
            max_named_filters: 1,
            ..FilterLimits::default()
        };
        assert_eq!(
            EventFilter::new(&request, &limits).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
