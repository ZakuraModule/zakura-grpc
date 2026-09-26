use std::collections::HashSet;

use tonic::Status;
use zakura_chain::{transaction, transparent::Address};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, transparent_input, utxo_change, BlockPayload, EventType,
    SubscribeRequest, SubscribeRequestFilter, SubscribeUpdate,
};

use crate::{config::FilterLimits, event::block_height};

/// A validated subscription filter compiled for the live hot path.
#[derive(Debug)]
pub(crate) struct EventFilter {
    default_all: bool,
    legacy: Option<CompiledFilter>,
    named: Vec<(String, CompiledFilter)>,
    needs_addresses: bool,
}

/// Match attribution and the richest block representation requested by its filters.
pub(crate) struct FilterMatch {
    pub(crate) names: Vec<String>,
    pub(crate) block_payload: BlockPayload,
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
            .then(|| {
                CompiledFilter::from_proto(
                    &SubscribeRequestFilter {
                        event_types: request.event_types.clone(),
                        ..SubscribeRequestFilter::default()
                    },
                    limits,
                )
            })
            .transpose()?;

        let mut named = Vec::with_capacity(request.filters.len());
        for (name, filter) in &request.filters {
            validate_name(name, limits)?;
            if filter_is_empty(filter) && !limits.allow_all {
                return Err(Status::invalid_argument(format!(
                    "named filter {name:?} must select at least one event type"
                )));
            }
            named.push((name.clone(), CompiledFilter::from_proto(filter, limits)?));
        }
        named.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let needs_addresses = legacy
            .iter()
            .chain(named.iter().map(|(_, filter)| filter))
            .any(CompiledFilter::uses_addresses);

        Ok(Self {
            default_all,
            legacy,
            named,
            needs_addresses,
        })
    }

    /// Returns match attribution and payload projection, or `None` when the update is skipped.
    pub(crate) fn matched(&self, update: &SubscribeUpdate) -> Option<FilterMatch> {
        let context = UpdateContext::new(update, self.needs_addresses);
        let legacy_matches = self
            .legacy
            .as_ref()
            .is_some_and(|filter| filter.matches(&context));
        let matched_named: Vec<_> = self
            .named
            .iter()
            .filter(|(_, filter)| filter.matches(&context))
            .collect();
        if !self.default_all && !legacy_matches && matched_named.is_empty() {
            return None;
        }

        let block_payload = if self.default_all
            || legacy_matches
            || matched_named
                .iter()
                .any(|(_, filter)| filter.block_payload == BlockPayload::Full)
        {
            BlockPayload::Full
        } else {
            BlockPayload::MetaOnly
        };
        Some(FilterMatch {
            names: matched_named
                .into_iter()
                .map(|(name, _)| name.clone())
                .collect(),
            block_payload,
        })
    }
}

#[derive(Debug)]
struct CompiledFilter {
    kinds: u64,
    min_height: Option<u32>,
    transaction_ids: HashSet<String>,
    address_include: HashSet<String>,
    address_exclude: HashSet<String>,
    address_required: HashSet<String>,
    coinbase: Option<bool>,
    transaction_version: Option<u32>,
    has_transparent: Option<bool>,
    has_sapling: Option<bool>,
    has_orchard: Option<bool>,
    min_value_zat: Option<u64>,
    block_payload: BlockPayload,
}

impl CompiledFilter {
    fn from_proto(filter: &SubscribeRequestFilter, limits: &FilterLimits) -> Result<Self, Status> {
        if filter.event_types.len() > limits.max_event_types {
            return Err(Status::invalid_argument(format!(
                "at most {} event types are allowed per filter",
                limits.max_event_types
            )));
        }

        let mut kinds = 0u64;
        for kind in &filter.event_types {
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

        let transaction_ids = compile_transaction_ids(
            &filter.transaction_ids,
            limits.max_transaction_ids,
            &limits.transaction_id_reject,
        )?;
        let legacy_addresses = compile_addresses(
            "transparent_addresses",
            &filter.transparent_addresses,
            limits.max_transparent_addresses,
            &limits.transparent_address_reject,
        )?;
        let mut address_include = compile_addresses(
            "address_include",
            &filter.address_include,
            limits.max_address_include,
            &limits.address_include_reject,
        )?;
        address_include.extend(legacy_addresses);
        let address_exclude = compile_addresses(
            "address_exclude",
            &filter.address_exclude,
            limits.max_address_exclude,
            &limits.address_exclude_reject,
        )?;
        let address_required = compile_addresses(
            "address_required",
            &filter.address_required,
            limits.max_address_required,
            &limits.address_required_reject,
        )?;
        let block_payload = BlockPayload::try_from(filter.block_payload).map_err(|_| {
            Status::invalid_argument(format!(
                "unknown block payload mode {}",
                filter.block_payload
            ))
        })?;

        Ok(Self {
            kinds,
            min_height: filter.min_height,
            transaction_ids,
            address_include,
            address_exclude,
            address_required,
            coinbase: filter.coinbase,
            transaction_version: filter.transaction_version,
            has_transparent: filter.has_transparent,
            has_sapling: filter.has_sapling,
            has_orchard: filter.has_orchard,
            min_value_zat: filter.min_value_zat,
            block_payload: match block_payload {
                BlockPayload::Unspecified | BlockPayload::Full => BlockPayload::Full,
                BlockPayload::MetaOnly => BlockPayload::MetaOnly,
            },
        })
    }

    fn uses_addresses(&self) -> bool {
        !self.address_include.is_empty()
            || !self.address_exclude.is_empty()
            || !self.address_required.is_empty()
    }

    fn matches(&self, context: &UpdateContext<'_>) -> bool {
        let update = context.update;
        let kind_matches = self.kinds == 0
            || event_type_bit(update.event_type)
                .is_some_and(|event_type| self.kinds & event_type != 0);
        let height_matches = self
            .min_height
            .is_none_or(|minimum| block_height(update).is_some_and(|height| height >= minimum));
        let transaction_matches = self.transaction_ids.is_empty()
            || update_matches_transaction_id(update, &self.transaction_ids);
        kind_matches
            && height_matches
            && transaction_matches
            && self.addresses_match(context.addresses.as_deref())
            && self.transaction_metadata_matches(context.transaction)
    }

    fn addresses_match(&self, addresses: Option<&[&str]>) -> bool {
        if !self.uses_addresses() {
            return true;
        }
        let Some(addresses) = addresses else {
            return false;
        };
        (self.address_include.is_empty()
            || addresses
                .iter()
                .any(|address| self.address_include.contains(*address)))
            && addresses
                .iter()
                .all(|address| !self.address_exclude.contains(*address))
            && self
                .address_required
                .iter()
                .all(|required| addresses.contains(&required.as_str()))
    }

    fn transaction_metadata_matches(&self, facts: Option<TransactionFacts>) -> bool {
        let uses_metadata = self.coinbase.is_some()
            || self.transaction_version.is_some()
            || self.has_transparent.is_some()
            || self.has_sapling.is_some()
            || self.has_orchard.is_some()
            || self.min_value_zat.is_some();
        if !uses_metadata {
            return true;
        }
        let Some(facts) = facts else {
            return false;
        };
        self.coinbase
            .is_none_or(|expected| expected == facts.coinbase)
            && self
                .transaction_version
                .is_none_or(|expected| expected == facts.version)
            && self
                .has_transparent
                .is_none_or(|expected| expected == facts.has_feature(HAS_TRANSPARENT))
            && self
                .has_sapling
                .is_none_or(|expected| expected == facts.has_feature(HAS_SAPLING))
            && self
                .has_orchard
                .is_none_or(|expected| expected == facts.has_feature(HAS_ORCHARD))
            && self.min_value_zat.is_none_or(|minimum| {
                facts
                    .transparent_input_value_zat
                    .is_some_and(|value| value >= minimum)
                    || facts.transparent_output_value_zat >= minimum
            })
    }
}

fn filter_is_empty(filter: &SubscribeRequestFilter) -> bool {
    filter.event_types.is_empty()
        && filter.min_height.is_none()
        && filter.transaction_ids.is_empty()
        && filter.transparent_addresses.is_empty()
        && filter.address_include.is_empty()
        && filter.address_exclude.is_empty()
        && filter.address_required.is_empty()
        && filter.coinbase.is_none()
        && filter.transaction_version.is_none()
        && filter.has_transparent.is_none()
        && filter.has_sapling.is_none()
        && filter.has_orchard.is_none()
        && filter.min_value_zat.is_none()
}

fn compile_transaction_ids(
    values: &[String],
    maximum: usize,
    rejected: &[String],
) -> Result<HashSet<String>, Status> {
    validate_list_size("transaction_ids", values.len(), maximum)?;
    let rejected = rejected
        .iter()
        .map(|value| {
            value
                .parse::<transaction::Hash>()
                .map(|value| value.to_string())
                .map_err(|_| Status::internal("invalid configured transaction ID reject-list"))
        })
        .collect::<Result<HashSet<_>, _>>()?;
    values
        .iter()
        .map(|value| {
            let canonical = value
                .parse::<transaction::Hash>()
                .map(|value| value.to_string())
                .map_err(|_| {
                    Status::invalid_argument(format!("invalid transaction ID {value:?}"))
                })?;
            if rejected.contains(&canonical) {
                return Err(Status::invalid_argument(format!(
                    "transaction_ids contains rejected transaction ID {value:?}"
                )));
            }
            Ok(canonical)
        })
        .collect()
}

fn compile_addresses(
    field: &str,
    values: &[String],
    maximum: usize,
    rejected: &[String],
) -> Result<HashSet<String>, Status> {
    validate_list_size(field, values.len(), maximum)?;
    let rejected = rejected
        .iter()
        .map(|value| {
            canonical_transparent_address_string(value).map_err(|()| {
                Status::internal("invalid configured transparent address reject-list")
            })
        })
        .collect::<Result<HashSet<_>, _>>()?;
    values
        .iter()
        .map(|value| {
            let canonical = canonical_transparent_address_string(value).map_err(|()| {
                Status::invalid_argument(format!("invalid transparent address {value:?}"))
            })?;
            if rejected.contains(&canonical) {
                return Err(Status::invalid_argument(format!(
                    "{field} contains rejected transparent address {value:?}"
                )));
            }
            Ok(canonical)
        })
        .collect()
}

fn validate_list_size(field: &str, actual: usize, maximum: usize) -> Result<(), Status> {
    if actual > maximum {
        return Err(Status::invalid_argument(format!(
            "at most {maximum} values are allowed in {field} per filter"
        )));
    }
    Ok(())
}

fn canonical_transparent_address_string(value: &str) -> Result<String, ()> {
    value
        .parse::<Address>()
        .map(canonical_transparent_address)
        .map(|address| address.to_string())
        .map_err(|_| ())
}

#[derive(Clone, Copy)]
struct TransactionFacts {
    coinbase: bool,
    version: u32,
    features: u8,
    transparent_input_value_zat: Option<u64>,
    transparent_output_value_zat: u64,
}

const HAS_TRANSPARENT: u8 = 1 << 0;
const HAS_SAPLING: u8 = 1 << 1;
const HAS_ORCHARD: u8 = 1 << 2;

impl TransactionFacts {
    fn has_feature(self, feature: u8) -> bool {
        self.features & feature != 0
    }
}

struct UpdateContext<'a> {
    update: &'a SubscribeUpdate,
    addresses: Option<Vec<&'a str>>,
    transaction: Option<TransactionFacts>,
}

impl<'a> UpdateContext<'a> {
    fn new(update: &'a SubscribeUpdate, needs_addresses: bool) -> Self {
        Self {
            update,
            addresses: needs_addresses
                .then(|| update_transparent_addresses(update))
                .flatten(),
            transaction: update_transaction_facts(update),
        }
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
        Some(subscribe_update::Update::MempoolTransaction(transaction)) => {
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
        Some(
            subscribe_update::Update::Block(_)
            | subscribe_update::Update::Ping(_)
            | subscribe_update::Update::Pong(_),
        )
        | None => false,
    }
}

fn update_transparent_addresses(update: &SubscribeUpdate) -> Option<Vec<&str>> {
    match update.update.as_ref() {
        Some(subscribe_update::Update::Transaction(transaction)) => Some(
            transaction
                .transparent_inputs
                .iter()
                .filter_map(|input| match input.input.as_ref() {
                    Some(transparent_input::Input::Prevout(previous)) => {
                        previous.previous_address.as_deref()
                    }
                    Some(transparent_input::Input::Coinbase(_)) | None => None,
                })
                .chain(
                    transaction
                        .transparent_outputs
                        .iter()
                        .filter_map(|output| output.address.as_deref()),
                )
                .collect(),
        ),
        Some(subscribe_update::Update::MempoolTransaction(transaction)) => Some(
            transaction
                .transparent_inputs
                .iter()
                .filter_map(|input| match input.input.as_ref() {
                    Some(transparent_input::Input::Prevout(previous)) => {
                        previous.previous_address.as_deref()
                    }
                    Some(transparent_input::Input::Coinbase(_)) | None => None,
                })
                .chain(
                    transaction
                        .transparent_outputs
                        .iter()
                        .filter_map(|output| output.address.as_deref()),
                )
                .collect(),
        ),
        Some(subscribe_update::Update::Utxo(utxo)) => Some(
            utxo.changes
                .iter()
                .filter_map(|change| match change.change.as_ref() {
                    Some(utxo_change::Change::Created(created)) => created.address.as_deref(),
                    Some(utxo_change::Change::Spent(spent)) => spent.previous_address.as_deref(),
                    None => None,
                })
                .collect(),
        ),
        Some(
            subscribe_update::Update::Block(_)
            | subscribe_update::Update::BestChain(_)
            | subscribe_update::Update::Mempool(_)
            | subscribe_update::Update::Ping(_)
            | subscribe_update::Update::Pong(_),
        )
        | None => None,
    }
}

fn update_transaction_facts(update: &SubscribeUpdate) -> Option<TransactionFacts> {
    match update.update.as_ref()? {
        subscribe_update::Update::Transaction(transaction) => Some(TransactionFacts {
            coinbase: transaction.coinbase,
            version: transaction.version,
            features: transaction_feature_bits(
                transaction.has_transparent,
                transaction.has_sapling,
                transaction.has_orchard,
            ),
            transparent_input_value_zat: transaction.transparent_input_value_zat,
            transparent_output_value_zat: transaction.transparent_output_value_zat,
        }),
        subscribe_update::Update::MempoolTransaction(transaction) => Some(TransactionFacts {
            coinbase: transaction.coinbase,
            version: transaction.version,
            features: transaction_feature_bits(
                transaction.has_transparent,
                transaction.has_sapling,
                transaction.has_orchard,
            ),
            transparent_input_value_zat: transaction.transparent_input_value_zat,
            transparent_output_value_zat: transaction.transparent_output_value_zat,
        }),
        subscribe_update::Update::Utxo(utxo) => Some(TransactionFacts {
            coinbase: utxo.coinbase,
            version: utxo.version,
            features: transaction_feature_bits(
                utxo.has_transparent,
                utxo.has_sapling,
                utxo.has_orchard,
            ),
            transparent_input_value_zat: utxo.transparent_input_value_zat,
            transparent_output_value_zat: utxo.transparent_output_value_zat,
        }),
        subscribe_update::Update::Block(_)
        | subscribe_update::Update::BestChain(_)
        | subscribe_update::Update::Mempool(_)
        | subscribe_update::Update::Ping(_)
        | subscribe_update::Update::Pong(_) => None,
    }
}

fn transaction_feature_bits(has_transparent: bool, has_sapling: bool, has_orchard: bool) -> u8 {
    (u8::from(has_transparent) * HAS_TRANSPARENT)
        | (u8::from(has_sapling) * HAS_SAPLING)
        | (u8::from(has_orchard) * HAS_ORCHARD)
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
        subscribe_update, BlockUpdate, MempoolTransactionUpdate, TransactionUpdate,
        TransparentOutput, UtxoChange, UtxoCreated, UtxoUpdate,
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
                version: 5,
                has_transparent: true,
                ..TransactionUpdate::default()
            })),
            ..SubscribeUpdate::default()
        }
    }

    fn mempool_transaction_update(transaction_id: String, address: String) -> SubscribeUpdate {
        SubscribeUpdate {
            event_type: EventType::MempoolTransaction.into(),
            update: Some(subscribe_update::Update::MempoolTransaction(
                MempoolTransactionUpdate {
                    transaction_id,
                    transparent_outputs: vec![TransparentOutput {
                        address: Some(address),
                        ..TransparentOutput::default()
                    }],
                    version: 5,
                    has_transparent: true,
                    ..MempoolTransactionUpdate::default()
                },
            )),
            ..SubscribeUpdate::default()
        }
    }

    fn matched_names(filter: &EventFilter, update: &SubscribeUpdate) -> Option<Vec<String>> {
        filter.matched(update).map(|matched| matched.names)
    }

    #[test]
    fn empty_request_matches_everything() {
        let filter =
            EventFilter::new(&SubscribeRequest::default(), &FilterLimits::default()).unwrap();
        assert_eq!(
            matched_names(&filter, &update(1, EventType::BlockFinalized)),
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
            matched_names(&filter, &update(10, EventType::BlockFinalized)),
            Some(vec!["blocks".to_owned(), "tip".to_owned()])
        );
        assert_eq!(
            matched_names(&filter, &update(9, EventType::BlockFinalized)),
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
            matched_names(
                &filter,
                &transaction_update(transaction_id, address.to_string())
            ),
            Some(vec!["wallet".to_owned()])
        );
    }

    #[test]
    fn named_filter_matches_mempool_transaction_id_and_address() {
        let transaction_id = transaction::Hash([8; 32]).to_string();
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [10; 20]);
        let filter = EventFilter::new(
            &SubscribeRequest {
                filters: HashMap::from([(
                    "mempool-wallet".to_owned(),
                    SubscribeRequestFilter {
                        event_types: vec![EventType::MempoolTransaction.into()],
                        transaction_ids: vec![transaction_id.clone()],
                        transparent_addresses: vec![address.to_string()],
                        ..SubscribeRequestFilter::default()
                    },
                )]),
                ..SubscribeRequest::default()
            },
            &FilterLimits::default(),
        )
        .unwrap();

        assert_eq!(
            matched_names(
                &filter,
                &mempool_transaction_update(transaction_id, address.to_string())
            ),
            Some(vec!["mempool-wallet".to_owned()])
        );
    }

    #[test]
    fn address_include_exclude_and_required_use_zcash_semantics() {
        let required_a = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [1; 20]);
        let required_b = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [2; 20]);
        let excluded = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [3; 20]);
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "wallet".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::Transaction.into()],
                    address_include: vec![required_a.to_string()],
                    address_exclude: vec![excluded.to_string()],
                    address_required: vec![required_a.to_string(), required_b.to_string()],
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let filter = EventFilter::new(&request, &FilterLimits::default()).unwrap();
        let transaction_id = transaction::Hash([4; 32]).to_string();
        let mut matching = transaction_update(transaction_id.clone(), required_a.to_string());
        let Some(subscribe_update::Update::Transaction(transaction)) = matching.update.as_mut()
        else {
            panic!("test update is a transaction");
        };
        transaction.transparent_outputs.push(TransparentOutput {
            address: Some(required_b.to_string()),
            ..TransparentOutput::default()
        });
        assert_eq!(
            matched_names(&filter, &matching),
            Some(vec!["wallet".to_owned()])
        );

        let missing_required = transaction_update(transaction_id.clone(), required_a.to_string());
        assert_eq!(matched_names(&filter, &missing_required), None);

        let mut contains_excluded = matching;
        let Some(subscribe_update::Update::Transaction(transaction)) =
            contains_excluded.update.as_mut()
        else {
            panic!("test update is a transaction");
        };
        transaction.transparent_outputs.push(TransparentOutput {
            address: Some(excluded.to_string()),
            ..TransparentOutput::default()
        });
        assert_eq!(matched_names(&filter, &contains_excluded), None);
    }

    #[test]
    fn transaction_metadata_filters_are_combined_with_and() {
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [5; 20]);
        let transaction_id = transaction::Hash([5; 32]).to_string();
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "shielded".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::Transaction.into()],
                    coinbase: Some(false),
                    transaction_version: Some(5),
                    has_transparent: Some(true),
                    has_sapling: Some(true),
                    has_orchard: Some(false),
                    min_value_zat: Some(50),
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let filter = EventFilter::new(&request, &FilterLimits::default()).unwrap();
        let mut matching = transaction_update(transaction_id, address.to_string());
        if let Some(subscribe_update::Update::Transaction(transaction)) = matching.update.as_mut() {
            transaction.has_sapling = true;
            transaction.transparent_output_value_zat = 50;
        } else {
            panic!("test update is a transaction");
        }
        assert_eq!(
            matched_names(&filter, &matching),
            Some(vec!["shielded".to_owned()])
        );

        if let Some(subscribe_update::Update::Transaction(transaction)) = matching.update.as_mut() {
            transaction.has_orchard = true;
        } else {
            panic!("test update is a transaction");
        }
        assert_eq!(matched_names(&filter, &matching), None);
    }

    #[test]
    fn block_payload_uses_the_richest_matching_named_filter() {
        let request = SubscribeRequest {
            filters: HashMap::from([
                (
                    "meta".to_owned(),
                    SubscribeRequestFilter {
                        event_types: vec![EventType::BlockFinalized.into()],
                        block_payload: BlockPayload::MetaOnly.into(),
                        ..SubscribeRequestFilter::default()
                    },
                ),
                (
                    "full-from-100".to_owned(),
                    SubscribeRequestFilter {
                        event_types: vec![EventType::BlockFinalized.into()],
                        min_height: Some(100),
                        block_payload: BlockPayload::Full.into(),
                        ..SubscribeRequestFilter::default()
                    },
                ),
            ]),
            ..SubscribeRequest::default()
        };
        let filter = EventFilter::new(&request, &FilterLimits::default()).unwrap();

        let meta = filter
            .matched(&update(99, EventType::BlockFinalized))
            .unwrap();
        assert_eq!(meta.names, vec!["meta".to_owned()]);
        assert_eq!(meta.block_payload, BlockPayload::MetaOnly);

        let full = filter
            .matched(&update(100, EventType::BlockFinalized))
            .unwrap();
        assert_eq!(
            full.names,
            vec!["full-from-100".to_owned(), "meta".to_owned()]
        );
        assert_eq!(full.block_payload, BlockPayload::Full);
    }

    #[test]
    fn payload_selection_does_not_bypass_disabled_catch_all_filters() {
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "meta".to_owned(),
                SubscribeRequestFilter {
                    block_payload: BlockPayload::MetaOnly.into(),
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let limits = FilterLimits {
            allow_all: false,
            ..FilterLimits::default()
        };

        assert_eq!(
            EventFilter::new(&request, &limits).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn address_limits_and_reject_lists_are_field_specific() {
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [6; 20]);
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "wallet".to_owned(),
                SubscribeRequestFilter {
                    address_include: vec![address.to_string()],
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let rejected = FilterLimits {
            address_include_reject: vec![address.to_string()],
            ..FilterLimits::default()
        };
        assert_eq!(
            EventFilter::new(&request, &rejected).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let disabled = FilterLimits {
            max_address_include: 0,
            ..FilterLimits::default()
        };
        assert_eq!(
            EventFilter::new(&request, &disabled).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn utxo_updates_support_address_feature_and_value_filters() {
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [7; 20]);
        let request = SubscribeRequest {
            filters: HashMap::from([(
                "orchard-utxo".to_owned(),
                SubscribeRequestFilter {
                    event_types: vec![EventType::Utxo.into()],
                    address_required: vec![address.to_string()],
                    transaction_version: Some(5),
                    has_transparent: Some(true),
                    has_sapling: Some(false),
                    has_orchard: Some(true),
                    min_value_zat: Some(100),
                    ..SubscribeRequestFilter::default()
                },
            )]),
            ..SubscribeRequest::default()
        };
        let filter = EventFilter::new(&request, &FilterLimits::default()).unwrap();
        let update = SubscribeUpdate {
            event_type: EventType::Utxo.into(),
            update: Some(subscribe_update::Update::Utxo(UtxoUpdate {
                version: 5,
                has_transparent: true,
                has_orchard: true,
                transparent_output_value_zat: 100,
                changes: vec![UtxoChange {
                    change: Some(utxo_change::Change::Created(UtxoCreated {
                        address: Some(address.to_string()),
                        ..UtxoCreated::default()
                    })),
                }],
                ..UtxoUpdate::default()
            })),
            ..SubscribeUpdate::default()
        };

        assert_eq!(
            matched_names(&filter, &update),
            Some(vec!["orchard-utxo".to_owned()])
        );
    }
}
