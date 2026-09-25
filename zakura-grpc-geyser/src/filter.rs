use std::collections::HashSet;

use tonic::Status;
use zakura_chain::{transaction, transparent::Address};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, transparent_input, utxo_change, EventType,
    SubscribeRequest, SubscribeRequestFilter, SubscribeUpdate,
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
            .then(|| CompiledFilter::new(&request.event_types, None, &[], &[], limits))
            .transpose()?;

        let mut named = Vec::with_capacity(request.filters.len());
        for (name, filter) in &request.filters {
            validate_name(name, limits)?;
            if filter.event_types.is_empty()
                && filter.transaction_ids.is_empty()
                && filter.transparent_addresses.is_empty()
                && !limits.allow_all
            {
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
    kinds: u64,
    min_height: Option<u32>,
    transaction_ids: HashSet<String>,
    transparent_addresses: HashSet<String>,
}

impl CompiledFilter {
    fn from_proto(filter: &SubscribeRequestFilter, limits: &FilterLimits) -> Result<Self, Status> {
        Self::new(
            &filter.event_types,
            filter.min_height,
            &filter.transaction_ids,
            &filter.transparent_addresses,
            limits,
        )
    }

    fn new(
        event_types: &[i32],
        min_height: Option<u32>,
        transaction_ids: &[String],
        transparent_addresses: &[String],
        limits: &FilterLimits,
    ) -> Result<Self, Status> {
        if event_types.len() > limits.max_event_types {
            return Err(Status::invalid_argument(format!(
                "at most {} event types are allowed per filter",
                limits.max_event_types
            )));
        }
        if transaction_ids.len() > limits.max_transaction_ids {
            return Err(Status::invalid_argument(format!(
                "at most {} transaction IDs are allowed per filter",
                limits.max_transaction_ids
            )));
        }
        if transparent_addresses.len() > limits.max_transparent_addresses {
            return Err(Status::invalid_argument(format!(
                "at most {} transparent addresses are allowed per filter",
                limits.max_transparent_addresses
            )));
        }

        let mut kinds = 0u64;
        for kind in event_types {
            let parsed = EventType::try_from(*kind)
                .map_err(|_| Status::invalid_argument(format!("unknown event type {kind}")))?;
            if parsed == EventType::Unspecified {
                return Err(Status::invalid_argument(
                    "EVENT_TYPE_UNSPECIFIED cannot be used as a filter",
                ));
            }
            kinds |= event_type_bit(*kind)
                .expect("known protobuf event types fit in the u64 filter bitmask");
        }

        let transaction_ids = transaction_ids
            .iter()
            .map(|transaction_id| {
                transaction_id
                    .parse::<transaction::Hash>()
                    .map(|transaction_id| transaction_id.to_string())
                    .map_err(|_| {
                        Status::invalid_argument(format!(
                            "invalid transaction ID {transaction_id:?}"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        let transparent_addresses = transparent_addresses
            .iter()
            .map(|address| {
                address
                    .parse::<Address>()
                    .map(|address| canonical_transparent_address(address).to_string())
                    .map_err(|_| {
                        Status::invalid_argument(format!("invalid transparent address {address:?}"))
                    })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            kinds,
            min_height,
            transaction_ids,
            transparent_addresses,
        })
    }

    fn matches(&self, update: &SubscribeUpdate) -> bool {
        let kind_matches = self.kinds == 0
            || event_type_bit(update.event_type)
                .is_some_and(|event_type| self.kinds & event_type != 0);
        let height_matches = self
            .min_height
            .is_none_or(|minimum| block_height(update).is_some_and(|height| height >= minimum));
        let transaction_matches = self.transaction_ids.is_empty()
            || update_matches_transaction_id(update, &self.transaction_ids);
        let address_matches = self.transparent_addresses.is_empty()
            || update_matches_transparent_address(update, &self.transparent_addresses);
        kind_matches && height_matches && transaction_matches && address_matches
    }
}

fn canonical_transparent_address(address: Address) -> Address {
    match address {
        Address::Tex {
            network_kind,
            validating_key_hash,
        } => Address::PayToPublicKeyHash {
            network_kind,
            pub_key_hash: validating_key_hash,
        },
        address => address,
    }
}

fn update_matches_transaction_id(
    update: &SubscribeUpdate,
    transaction_ids: &HashSet<String>,
) -> bool {
    match update.update.as_ref() {
        Some(subscribe_update::Update::Transaction(transaction)) => {
            transaction_ids.contains(&transaction.transaction_id)
        }
        Some(subscribe_update::Update::Utxo(utxo)) => {
            transaction_ids.contains(&utxo.transaction_id)
        }
        Some(subscribe_update::Update::Mempool(mempool)) => mempool
            .transaction_ids
            .iter()
            .any(|transaction_id| transaction_ids.contains(transaction_id)),
        Some(subscribe_update::Update::BestChain(best_chain)) => best_chain
            .change
            .as_ref()
            .and_then(|change| match change {
                best_chain_update::Change::Grow(grow) => Some(grow),
                best_chain_update::Change::Reset(_) => None,
            })
            .is_some_and(|grow| {
                grow.transaction_ids
                    .iter()
                    .any(|transaction_id| transaction_ids.contains(transaction_id))
            }),
        Some(subscribe_update::Update::Block(_) | subscribe_update::Update::Pong(_)) | None => {
            false
        }
    }
}

fn update_matches_transparent_address(
    update: &SubscribeUpdate,
    transparent_addresses: &HashSet<String>,
) -> bool {
    match update.update.as_ref() {
        Some(subscribe_update::Update::Transaction(transaction)) => transaction
            .transparent_inputs
            .iter()
            .filter_map(|input| match input.input.as_ref() {
                Some(transparent_input::Input::Prevout(previous)) => {
                    previous.previous_address.as_ref()
                }
                Some(transparent_input::Input::Coinbase(_)) | None => None,
            })
            .chain(
                transaction
                    .transparent_outputs
                    .iter()
                    .filter_map(|output| output.address.as_ref()),
            )
            .any(|address| transparent_addresses.contains(address)),
        Some(subscribe_update::Update::Utxo(utxo)) => utxo.changes.iter().any(|change| {
            let address = match change.change.as_ref() {
                Some(utxo_change::Change::Created(created)) => created.address.as_ref(),
                Some(utxo_change::Change::Spent(spent)) => spent.previous_address.as_ref(),
                None => None,
            };
            address.is_some_and(|address| transparent_addresses.contains(address))
        }),
        Some(
            subscribe_update::Update::Block(_)
            | subscribe_update::Update::BestChain(_)
            | subscribe_update::Update::Mempool(_)
            | subscribe_update::Update::Pong(_),
        )
        | None => false,
    }
}

fn event_type_bit(event_type: i32) -> Option<u64> {
    u32::try_from(event_type)
        .ok()
        .and_then(|event_type| 1u64.checked_shl(event_type))
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

    use zakura_chain::parameters::Network;
    use zakura_grpc_proto::geyser::{
        subscribe_update, BlockUpdate, TransactionUpdate, TransparentOutput,
    };

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

    fn transaction_update(transaction_id: String, address: String) -> SubscribeUpdate {
        SubscribeUpdate {
            event_type: EventType::Transaction.into(),
            update: Some(subscribe_update::Update::Transaction(TransactionUpdate {
                transaction_id,
                transparent_outputs: vec![TransparentOutput {
                    address: Some(address),
                    ..TransparentOutput::default()
                }],
                ..TransactionUpdate::default()
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
                    ..SubscribeRequestFilter::default()
                },
            ),
            (
                "blocks".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::BlockFinalized.into()],
                    min_height: None,
                    ..SubscribeRequestFilter::default()
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

    #[test]
    fn named_filter_matches_transaction_id_and_address() {
        let transaction_id = transaction::Hash([7; 32]).to_string();
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [9; 20]);
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "wallet".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::Transaction.into()],
                    transaction_ids: vec![transaction_id.clone()],
                    transparent_addresses: vec![address.to_string()],
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let filter = EventFilter::new(&request, &FilterLimits::default()).unwrap();

        assert_eq!(
            filter.matched_names(&transaction_update(transaction_id, address.to_string())),
            Some(vec!["wallet".to_owned()])
        );
    }
}
