use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use prost_types::Timestamp;
use rayon::{prelude::*, ThreadPool};
use zakura_chain::{
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::{self, Transaction},
    transparent,
};
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, EventEnvelope, MempoolEvent, MempoolEventKind, PluginError,
    PluginEvent,
};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, transparent_input, utxo_change, BestChainGrow,
    BestChainReset, BestChainUpdate, BlockCommitment, BlockPayload, BlockUpdate, CanonicalBlock,
    EventType, MempoolAction, MempoolTransactionUpdate, MempoolUpdate, Outpoint, SubscribeUpdate,
    TransactionUpdate, TransparentCoinbaseInput, TransparentInput, TransparentOutput,
    TransparentPrevoutInput, UtxoChange, UtxoCreated, UtxoSpent, UtxoUpdate,
};

pub(crate) fn encode_event(
    event: &EventEnvelope,
    transaction_updates: bool,
    utxo_updates: bool,
    mempool_transaction_updates: bool,
    encoding_pool: Option<&ThreadPool>,
    parallel_encoding_min_transactions: usize,
) -> Result<Vec<SubscribeUpdate>, PluginError> {
    let metadata = UpdateMetadata::new(event);
    let mut updates = match &event.payload {
        PluginEvent::BlockAccepted(block) => vec![new_update(
            &metadata,
            EventType::BlockAccepted,
            subscribe_update::Update::Block(encode_block(block, false)?),
        )],
        PluginEvent::BlockFinalized(block) => vec![new_update(
            &metadata,
            EventType::BlockFinalized,
            subscribe_update::Update::Block(encode_block(block, true)?),
        )],
        PluginEvent::BestChainChanged(change) => vec![new_update(
            &metadata,
            EventType::BestChainChanged,
            subscribe_update::Update::BestChain(encode_best_chain(change)),
        )],
        PluginEvent::MempoolChanged(change) => {
            let action = match change.kind {
                MempoolEventKind::Added => MempoolAction::Added,
                MempoolEventKind::Invalidated => MempoolAction::Invalidated,
                MempoolEventKind::Mined => MempoolAction::Mined,
            };
            vec![new_update(
                &metadata,
                EventType::MempoolChanged,
                subscribe_update::Update::Mempool(MempoolUpdate {
                    action: action.into(),
                    transaction_ids: change
                        .transaction_ids
                        .iter()
                        .map(|transaction_id| {
                            encode_unmined_transaction_id(
                                transaction_id.mined_id(),
                                transaction_id.auth_digest(),
                            )
                        })
                        .collect(),
                }),
            )]
        }
    };

    match &event.payload {
        PluginEvent::BlockAccepted(block) => append_block_updates(
            &mut updates,
            &metadata,
            block,
            BlockEncodingOptions {
                commitment: BlockCommitment::Accepted,
                transaction_updates,
                utxo_updates,
                encoding_pool,
                parallel_encoding_min_transactions,
            },
        )?,
        PluginEvent::BlockFinalized(block) => append_block_updates(
            &mut updates,
            &metadata,
            block,
            BlockEncodingOptions {
                commitment: BlockCommitment::Finalized,
                transaction_updates,
                utxo_updates,
                encoding_pool,
                parallel_encoding_min_transactions,
            },
        )?,
        PluginEvent::MempoolChanged(change) => append_mempool_transaction_updates(
            &mut updates,
            &metadata,
            change,
            mempool_transaction_updates,
            encoding_pool,
            parallel_encoding_min_transactions,
        )?,
        PluginEvent::BestChainChanged(_) => {}
    }

    Ok(updates)
}

fn append_mempool_transaction_updates(
    updates: &mut Vec<SubscribeUpdate>,
    metadata: &UpdateMetadata,
    change: &MempoolEvent,
    enabled: bool,
    encoding_pool: Option<&ThreadPool>,
    parallel_encoding_min_transactions: usize,
) -> Result<(), PluginError> {
    if !enabled || change.kind != MempoolEventKind::Added || change.transactions.is_empty() {
        return Ok(());
    }

    let encode = |transaction: &transaction::VerifiedUnminedTx| {
        encode_mempool_transaction_update(metadata, change, transaction)
    };
    let encoded = if let Some(pool) =
        encoding_pool.filter(|_| change.transactions.len() >= parallel_encoding_min_transactions)
    {
        metrics::counter!("plugin.grpc.encoding.mempool_batches.total", "mode" => "parallel")
            .increment(1);
        pool.install(|| {
            change
                .transactions
                .par_iter()
                .map(encode)
                .collect::<Result<Vec<_>, PluginError>>()
        })?
    } else {
        metrics::counter!("plugin.grpc.encoding.mempool_batches.total", "mode" => "sequential")
            .increment(1);
        change
            .transactions
            .iter()
            .map(encode)
            .collect::<Result<Vec<_>, PluginError>>()?
    };
    updates.extend(encoded);

    Ok(())
}

fn encode_mempool_transaction_update(
    metadata: &UpdateMetadata,
    change: &MempoolEvent,
    verified: &transaction::VerifiedUnminedTx,
) -> Result<SubscribeUpdate, PluginError> {
    let transaction = verified.transaction.transaction().as_ref();
    let (transaction_id, auth_digest) = transaction.txid_and_auth_digest();
    let transaction_id_string = transaction_id.to_string();
    let EncodedTransparentUpdates {
        transaction_inputs,
        transaction_outputs,
        transparent_input_value_zat,
        transparent_output_value_zat,
        ..
    } = encode_transparent_updates(
        transaction,
        &transaction_id_string,
        &change.network,
        PreviousOutputs::Mempool(&verified.spent_outputs),
        true,
        false,
    )?;
    let transaction_bytes = transaction
        .zcash_serialize_to_vec()
        .map_err(|error| PluginError::new(format!("failed to encode transaction: {error}")))?;
    let miner_fee_zat = u64::try_from(verified.miner_fee.zatoshis())
        .expect("verified mempool transaction fees are non-negative by type");
    let admitted_at = verified.time.as_ref().map(|time| Timestamp {
        seconds: time.timestamp(),
        nanos: i32::try_from(time.timestamp_subsec_nanos())
            .expect("nanoseconds fit in i32 because they are always below one billion"),
    });

    Ok(new_update(
        metadata,
        EventType::MempoolTransaction,
        subscribe_update::Update::MempoolTransaction(MempoolTransactionUpdate {
            transaction_id: transaction_id_string,
            unmined_transaction_id: encode_unmined_transaction_id(transaction_id, auth_digest),
            auth_digest: auth_digest.map(|digest| digest.to_string()),
            transaction: transaction_bytes.into(),
            network: change.network.to_string(),
            transparent_inputs: transaction_inputs,
            transparent_outputs: transaction_outputs,
            version: transaction.version(),
            lock_time: transaction.raw_lock_time(),
            lock_time_is_time: transaction.lock_time_is_time(),
            expiry_height: transaction.expiry_height().map(|height| height.0),
            transparent_input_value_zat,
            transparent_output_value_zat,
            miner_fee_zat,
            admitted_at,
            admitted_height: verified.height.map(|height| height.0),
            conventional_actions: verified.conventional_actions,
            unpaid_actions: verified.unpaid_actions,
            legacy_sigop_count: verified.legacy_sigop_count,
            p2sh_sigop_count: verified.p2sh_sigop_count,
            fee_weight_ratio: verified.fee_weight_ratio,
            coinbase: transaction.is_coinbase(),
            has_transparent: transaction.has_transparent_inputs_or_outputs(),
            has_sapling: transaction.has_sapling_shielded_data(),
            has_orchard: transaction.has_orchard_shielded_data(),
        }),
    ))
}

#[derive(Clone, Copy)]
struct BlockEncodingOptions<'a> {
    commitment: BlockCommitment,
    transaction_updates: bool,
    utxo_updates: bool,
    encoding_pool: Option<&'a ThreadPool>,
    parallel_encoding_min_transactions: usize,
}

fn append_block_updates(
    updates: &mut Vec<SubscribeUpdate>,
    metadata: &UpdateMetadata,
    block: &BlockEvent,
    options: BlockEncodingOptions<'_>,
) -> Result<(), PluginError> {
    if !options.transaction_updates && !options.utxo_updates {
        return Ok(());
    }

    let block_hash = block.hash.to_string();
    let network = block.network.to_string();
    let encode = |(transaction_index, transaction): (usize, &Arc<Transaction>)| {
        encode_transaction_updates(
            metadata,
            block,
            &block_hash,
            &network,
            options.commitment,
            options.transaction_updates,
            options.utxo_updates,
            transaction_index,
            transaction.as_ref(),
        )
    };
    let encoded = if let Some(pool) = options
        .encoding_pool
        .filter(|_| block.block.transactions.len() >= options.parallel_encoding_min_transactions)
    {
        metrics::counter!("plugin.grpc.encoding.blocks.total", "mode" => "parallel").increment(1);
        pool.install(|| {
            block
                .block
                .transactions
                .par_iter()
                .enumerate()
                .map(encode)
                .collect::<Result<Vec<_>, PluginError>>()
        })?
    } else {
        metrics::counter!("plugin.grpc.encoding.blocks.total", "mode" => "sequential").increment(1);
        block
            .block
            .transactions
            .iter()
            .enumerate()
            .map(encode)
            .collect::<Result<Vec<_>, PluginError>>()?
    };
    updates.extend(encoded.into_iter().flatten());

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn encode_transaction_updates(
    metadata: &UpdateMetadata,
    block: &BlockEvent,
    block_hash: &str,
    network: &str,
    commitment: BlockCommitment,
    transaction_updates: bool,
    utxo_updates: bool,
    transaction_index: usize,
    transaction: &Transaction,
) -> Result<Vec<SubscribeUpdate>, PluginError> {
    let transaction_index = u32::try_from(transaction_index)
        .map_err(|_| PluginError::new("transaction index exceeds the gRPC protocol's u32 range"))?;
    let (transaction_id, auth_digest) = transaction.txid_and_auth_digest();
    let transaction_id_string = transaction_id.to_string();
    let EncodedTransparentUpdates {
        transaction_inputs,
        transaction_outputs,
        utxo_changes,
        transparent_input_value_zat,
        transparent_output_value_zat,
    } = encode_transparent_updates(
        transaction,
        &transaction_id_string,
        &block.network,
        PreviousOutputs::Block(&block.spent_outputs),
        transaction_updates,
        utxo_updates,
    )?;
    let mut updates =
        Vec::with_capacity(usize::from(transaction_updates) + usize::from(utxo_updates));

    if transaction_updates {
        let unmined_transaction_id = encode_unmined_transaction_id(transaction_id, auth_digest);
        let transaction_bytes = transaction
            .zcash_serialize_to_vec()
            .map_err(|error| PluginError::new(format!("failed to encode transaction: {error}")))?;
        updates.push(new_update(
            metadata,
            EventType::Transaction,
            subscribe_update::Update::Transaction(TransactionUpdate {
                transaction_id: transaction_id_string.clone(),
                unmined_transaction_id,
                auth_digest: auth_digest.map(|digest| digest.to_string()),
                transaction: transaction_bytes.into(),
                height: block.height.0,
                block_hash: block_hash.to_owned(),
                transaction_index,
                commitment: commitment.into(),
                coinbase: transaction.is_coinbase(),
                transparent_inputs: transaction_inputs,
                transparent_outputs: transaction_outputs,
                version: transaction.version(),
                lock_time: transaction.raw_lock_time(),
                lock_time_is_time: transaction.lock_time_is_time(),
                expiry_height: transaction.expiry_height().map(|height| height.0),
                network: network.to_owned(),
                transparent_input_value_zat,
                transparent_output_value_zat,
                has_transparent: transaction.has_transparent_inputs_or_outputs(),
                has_sapling: transaction.has_sapling_shielded_data(),
                has_orchard: transaction.has_orchard_shielded_data(),
            }),
        ));
    }

    if utxo_updates && !utxo_changes.is_empty() {
        updates.push(new_update(
            metadata,
            EventType::Utxo,
            subscribe_update::Update::Utxo(UtxoUpdate {
                height: block.height.0,
                block_hash: block_hash.to_owned(),
                transaction_id: transaction_id_string,
                transaction_index,
                commitment: commitment.into(),
                changes: utxo_changes,
                coinbase: transaction.is_coinbase(),
                version: transaction.version(),
                has_transparent: transaction.has_transparent_inputs_or_outputs(),
                has_sapling: transaction.has_sapling_shielded_data(),
                has_orchard: transaction.has_orchard_shielded_data(),
                transparent_input_value_zat,
                transparent_output_value_zat,
            }),
        ));
    }

    Ok(updates)
}

struct EncodedTransparentUpdates {
    transaction_inputs: Vec<TransparentInput>,
    transaction_outputs: Vec<TransparentOutput>,
    utxo_changes: Vec<UtxoChange>,
    transparent_input_value_zat: Option<u64>,
    transparent_output_value_zat: u64,
}

struct TransparentEncodingContext<'a> {
    network: &'a Network,
    previous_outputs: PreviousOutputs<'a>,
    transaction_updates: bool,
    utxo_updates: bool,
}

#[derive(Clone, Copy)]
enum PreviousOutputs<'a> {
    Block(&'a HashMap<transparent::OutPoint, transparent::OrderedUtxo>),
    Mempool(&'a [transparent::Output]),
}

#[derive(Clone, Default)]
struct EncodedPreviousOutput {
    value_zat: Option<u64>,
    lock_script: Option<Bytes>,
    address: Option<String>,
    height: Option<u32>,
    from_coinbase: Option<bool>,
}

fn encode_transparent_updates(
    transaction: &Transaction,
    transaction_id_string: &str,
    network: &Network,
    previous_outputs: PreviousOutputs<'_>,
    transaction_updates: bool,
    utxo_updates: bool,
) -> Result<EncodedTransparentUpdates, PluginError> {
    let context = TransparentEncodingContext {
        network,
        previous_outputs,
        transaction_updates,
        utxo_updates,
    };
    let mut transaction_inputs = if transaction_updates {
        Vec::with_capacity(transaction.inputs().len())
    } else {
        Vec::new()
    };
    let mut transaction_outputs = if transaction_updates {
        Vec::with_capacity(transaction.outputs().len())
    } else {
        Vec::new()
    };
    let mut utxo_changes = if utxo_updates {
        Vec::with_capacity(
            transaction
                .inputs()
                .len()
                .saturating_add(transaction.outputs().len()),
        )
    } else {
        Vec::new()
    };
    let mut transparent_input_value_zat = (transaction_updates || utxo_updates).then_some(0u64);
    let mut transparent_output_value_zat = 0u64;

    for (input_index, input) in transaction.inputs().iter().enumerate() {
        let grpc_input_index = u32::try_from(input_index)
            .map_err(|_| PluginError::new("input index exceeds the gRPC protocol's u32 range"))?;
        let input_value_zat = encode_transparent_input(
            &context,
            input,
            input_index,
            grpc_input_index,
            &mut transaction_inputs,
            &mut utxo_changes,
        );
        transparent_input_value_zat = match (transparent_input_value_zat, input_value_zat) {
            (Some(total), Some(value)) => Some(total.checked_add(value).ok_or_else(|| {
                PluginError::new("transparent input value exceeds the gRPC protocol's u64 range")
            })?),
            _ => None,
        };
    }

    for (output_index, output) in transaction.outputs().iter().enumerate() {
        let output_index = u32::try_from(output_index)
            .map_err(|_| PluginError::new("output index exceeds the gRPC protocol's u32 range"))?;
        let output_value_zat = encode_transparent_output(
            &context,
            output,
            output_index,
            transaction_id_string,
            &mut transaction_outputs,
            &mut utxo_changes,
        );
        transparent_output_value_zat = transparent_output_value_zat
            .checked_add(output_value_zat)
            .ok_or_else(|| {
            PluginError::new("transparent output value exceeds the gRPC protocol's u64 range")
        })?;
    }

    Ok(EncodedTransparentUpdates {
        transaction_inputs,
        transaction_outputs,
        utxo_changes,
        transparent_input_value_zat,
        transparent_output_value_zat,
    })
}

fn encode_transparent_input(
    context: &TransparentEncodingContext<'_>,
    input: &transparent::Input,
    input_index: usize,
    grpc_input_index: u32,
    transaction_inputs: &mut Vec<TransparentInput>,
    utxo_changes: &mut Vec<UtxoChange>,
) -> Option<u64> {
    match input {
        transparent::Input::PrevOut {
            outpoint,
            unlock_script,
            sequence,
        } => {
            let previous = encode_previous_output(context, outpoint, input_index);
            let grpc_outpoint = Outpoint {
                transaction_id: outpoint.hash.to_string(),
                output_index: outpoint.index,
            };
            let unlock_script = Bytes::copy_from_slice(unlock_script.as_raw_bytes());

            if context.transaction_updates {
                transaction_inputs.push(TransparentInput {
                    input_index: grpc_input_index,
                    sequence: *sequence,
                    input: Some(transparent_input::Input::Prevout(TransparentPrevoutInput {
                        previous_output: Some(grpc_outpoint.clone()),
                        unlock_script: unlock_script.clone(),
                        previous_value_zat: previous.value_zat,
                        previous_lock_script: previous.lock_script.clone(),
                        previous_address: previous.address.clone(),
                        previous_height: previous.height,
                        previous_from_coinbase: previous.from_coinbase,
                    })),
                });
            }
            if context.utxo_updates {
                utxo_changes.push(UtxoChange {
                    change: Some(utxo_change::Change::Spent(UtxoSpent {
                        outpoint: Some(grpc_outpoint),
                        input_index: grpc_input_index,
                        unlock_script,
                        sequence: *sequence,
                        previous_value_zat: previous.value_zat,
                        previous_lock_script: previous.lock_script,
                        previous_address: previous.address,
                        previous_height: previous.height,
                        previous_from_coinbase: previous.from_coinbase,
                    })),
                });
            }
            previous.value_zat
        }
        transparent::Input::Coinbase {
            height,
            data,
            sequence,
        } => {
            if context.transaction_updates {
                transaction_inputs.push(TransparentInput {
                    input_index: grpc_input_index,
                    sequence: *sequence,
                    input: Some(transparent_input::Input::Coinbase(
                        TransparentCoinbaseInput {
                            height: height.0,
                            data: Bytes::copy_from_slice(data),
                        },
                    )),
                });
            }
            Some(0)
        }
    }
}

fn encode_transparent_output(
    context: &TransparentEncodingContext<'_>,
    output: &transparent::Output,
    output_index: u32,
    transaction_id: &str,
    transaction_outputs: &mut Vec<TransparentOutput>,
    utxo_changes: &mut Vec<UtxoChange>,
) -> u64 {
    let value_zat = u64::try_from(output.value.zatoshis())
        .expect("transparent output values are non-negative by type");
    let lock_script = Bytes::copy_from_slice(output.lock_script.as_raw_bytes());
    let address = output
        .address(context.network)
        .map(|address| address.to_string());

    if context.transaction_updates {
        transaction_outputs.push(TransparentOutput {
            output_index,
            value_zat,
            lock_script: lock_script.clone(),
            address: address.clone(),
        });
    }
    if context.utxo_updates {
        utxo_changes.push(UtxoChange {
            change: Some(utxo_change::Change::Created(UtxoCreated {
                outpoint: Some(Outpoint {
                    transaction_id: transaction_id.to_owned(),
                    output_index,
                }),
                value_zat,
                lock_script,
                address,
            })),
        });
    }
    value_zat
}

fn encode_previous_output(
    context: &TransparentEncodingContext<'_>,
    outpoint: &transparent::OutPoint,
    input_index: usize,
) -> EncodedPreviousOutput {
    let (output, height, from_coinbase) = match context.previous_outputs {
        PreviousOutputs::Block(previous_outputs) => {
            let Some(previous) = previous_outputs.get(outpoint) else {
                return EncodedPreviousOutput::default();
            };
            (
                &previous.utxo.output,
                Some(previous.utxo.height.0),
                Some(previous.utxo.from_coinbase),
            )
        }
        PreviousOutputs::Mempool(previous_outputs) => {
            let Some(previous) = previous_outputs.get(input_index) else {
                return EncodedPreviousOutput::default();
            };
            (previous, None, None)
        }
    };

    EncodedPreviousOutput {
        value_zat: Some(
            u64::try_from(output.value.zatoshis())
                .expect("transparent output values are non-negative by type"),
        ),
        lock_script: Some(Bytes::copy_from_slice(output.lock_script.as_raw_bytes())),
        address: output
            .address(context.network)
            .map(|address| address.to_string()),
        height,
        from_coinbase,
    }
}

fn encode_unmined_transaction_id(
    transaction_id: transaction::Hash,
    auth_digest: Option<transaction::AuthDigest>,
) -> String {
    auth_digest.map_or_else(
        || transaction_id.to_string(),
        |digest| format!("{transaction_id}{digest}"),
    )
}

#[derive(Clone, Debug)]
struct UpdateMetadata {
    schema_version: u32,
    session_id: Bytes,
    observed_at: Timestamp,
    source_sequence: u64,
}

impl UpdateMetadata {
    fn new(event: &EventEnvelope) -> Self {
        Self {
            schema_version: event.schema_version,
            session_id: Bytes::copy_from_slice(&event.session_id.0.to_be_bytes()),
            observed_at: timestamp(event.observed_at),
            source_sequence: event.sequence,
        }
    }
}

fn new_update(
    metadata: &UpdateMetadata,
    event_type: EventType,
    update: subscribe_update::Update,
) -> SubscribeUpdate {
    SubscribeUpdate {
        schema_version: metadata.schema_version,
        session_id: metadata.session_id.clone(),
        sequence: 0,
        observed_at: Some(metadata.observed_at),
        event_type: event_type.into(),
        filters: Vec::new(),
        source_sequence: metadata.source_sequence,
        update: Some(update),
    }
}

fn encode_block(block: &BlockEvent, finalized: bool) -> Result<BlockUpdate, PluginError> {
    let block_bytes = block
        .block
        .zcash_serialize_to_vec()
        .map_err(|error| PluginError::new(format!("failed to encode block: {error}")))?;

    Ok(BlockUpdate {
        height: block.height.0,
        hash: block.hash.to_string(),
        block: block_bytes.into(),
        receipt_order: block.receipt_order,
        finalized,
        payload: BlockPayload::Full.into(),
    })
}

fn encode_best_chain(change: &BestChainChange) -> BestChainUpdate {
    match change {
        BestChainChange::Grow {
            height,
            hash,
            previous_block_hash,
            transaction_ids,
        } => BestChainUpdate {
            height: height.0,
            hash: hash.to_string(),
            change: Some(best_chain_update::Change::Grow(BestChainGrow {
                previous_block_hash: previous_block_hash.to_string(),
                transaction_ids: transaction_ids.iter().map(ToString::to_string).collect(),
            })),
        },
        BestChainChange::Reset {
            height,
            hash,
            disconnected_blocks,
            connected_blocks,
            diff_complete,
        } => BestChainUpdate {
            height: height.0,
            hash: hash.to_string(),
            change: Some(best_chain_update::Change::Reset(BestChainReset {
                disconnected_blocks: disconnected_blocks
                    .iter()
                    .map(encode_canonical_block)
                    .collect(),
                connected_blocks: connected_blocks
                    .iter()
                    .map(encode_canonical_block)
                    .collect(),
                diff_complete: *diff_complete,
            })),
        },
    }
}

fn encode_canonical_block(
    block: &zakura_geyser_plugin_interface::CanonicalBlock,
) -> CanonicalBlock {
    CanonicalBlock {
        height: block.height.0,
        hash: block.hash.to_string(),
        previous_block_hash: block.previous_block_hash.to_string(),
    }
}

fn timestamp(time: SystemTime) -> Timestamp {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(duration.subsec_nanos())
            .expect("nanoseconds fit in i32 because they are always below one billion"),
    }
}

pub(crate) fn block_height(update: &SubscribeUpdate) -> Option<u32> {
    match update.update.as_ref()? {
        subscribe_update::Update::Block(block) => Some(block.height),
        subscribe_update::Update::BestChain(change) => Some(change.height),
        subscribe_update::Update::Transaction(transaction) => Some(transaction.height),
        subscribe_update::Update::Utxo(utxo) => Some(utxo.height),
        subscribe_update::Update::Mempool(_)
        | subscribe_update::Update::MempoolTransaction(_)
        | subscribe_update::Update::Ping(_)
        | subscribe_update::Update::Pong(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use zakura_chain::{
        amount::{Amount, NonNegative},
        block::Height,
        transaction::{LockTime, VerifiedUnminedTx},
        transparent::{Address, Input, OrderedUtxo, OutPoint, Output, Script},
    };
    use zakura_geyser_plugin_interface::{
        BestChainChange, CanonicalBlock as InterfaceCanonicalBlock, EventEnvelope, MempoolEvent,
        MempoolEventKind, PluginEvent, SessionId, EVENT_SCHEMA_VERSION,
    };

    use super::*;

    #[test]
    fn encodes_atomic_canonical_reorg_diff() {
        let disconnected = InterfaceCanonicalBlock {
            height: Height(11),
            hash: zakura_chain::block::Hash([11; 32]),
            previous_block_hash: zakura_chain::block::Hash([10; 32]),
        };
        let connected = InterfaceCanonicalBlock {
            height: Height(11),
            hash: zakura_chain::block::Hash([21; 32]),
            previous_block_hash: zakura_chain::block::Hash([10; 32]),
        };
        let event = EventEnvelope {
            schema_version: EVENT_SCHEMA_VERSION,
            session_id: SessionId(7),
            sequence: 11,
            observed_at: SystemTime::UNIX_EPOCH,
            payload: PluginEvent::BestChainChanged(BestChainChange::Reset {
                height: connected.height,
                hash: connected.hash,
                disconnected_blocks: vec![disconnected].into(),
                connected_blocks: vec![connected].into(),
                diff_complete: true,
            }),
        };

        let updates = encode_event(&event, true, true, true, None, 32).unwrap();
        let Some(subscribe_update::Update::BestChain(best_chain)) = updates[0].update.as_ref()
        else {
            panic!("best-chain event must encode as a best-chain update");
        };
        let Some(best_chain_update::Change::Reset(reset)) = best_chain.change.as_ref() else {
            panic!("reset event must retain its canonical-chain diff");
        };

        assert!(reset.diff_complete);
        assert_eq!(reset.disconnected_blocks.len(), 1);
        assert_eq!(reset.disconnected_blocks[0].height, 11);
        assert_eq!(
            reset.disconnected_blocks[0].hash,
            disconnected.hash.to_string()
        );
        assert_eq!(reset.connected_blocks.len(), 1);
        assert_eq!(reset.connected_blocks[0].hash, connected.hash.to_string());
    }

    #[test]
    fn encodes_transaction_and_ordered_utxo_changes() {
        let network = Network::Mainnet;
        let previous = OutPoint {
            hash: transaction::Hash([7; 32]),
            index: 3,
        };
        let previous_address = Address::from_pub_key_hash(network.t_addr_kind(), [8; 20]);
        let output_address = Address::from_pub_key_hash(network.t_addr_kind(), [9; 20]);
        let previous_address_string = previous_address.to_string();
        let output_address_string = output_address.to_string();
        let spent_outputs = HashMap::from([(
            previous,
            OrderedUtxo::new(
                Output::new(Amount::<NonNegative>::new(456), previous_address.script()),
                Height(12),
                0,
            ),
        )]);
        let transaction = Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint: previous,
                unlock_script: Script::new(&[0x51]),
                sequence: 42,
            }],
            outputs: vec![Output::new(
                Amount::<NonNegative>::new(123),
                output_address.script(),
            )],
            lock_time: LockTime::unlocked(),
        };
        let transaction_id = transaction.hash();

        assert_eq!(
            encode_unmined_transaction_id(transaction_id, None),
            transaction_id.to_string()
        );
        assert_eq!(
            encode_unmined_transaction_id(transaction_id, Some(transaction::AuthDigest([8; 32])))
                .len(),
            128
        );

        let encoded = encode_transparent_updates(
            &transaction,
            &transaction_id.to_string(),
            &network,
            PreviousOutputs::Block(&spent_outputs),
            true,
            true,
        )
        .unwrap();
        assert_eq!(encoded.transaction_inputs.len(), 1);
        assert_eq!(encoded.transaction_outputs.len(), 1);
        assert_eq!(encoded.utxo_changes.len(), 2);
        assert_eq!(encoded.transparent_input_value_zat, Some(456));
        assert_eq!(encoded.transparent_output_value_zat, 123);

        let input = &encoded.transaction_inputs[0];
        assert_eq!(input.input_index, 0);
        assert_eq!(input.sequence, 42);
        let Some(transparent_input::Input::Prevout(prevout)) = &input.input else {
            panic!("the transparent input must preserve its previous output");
        };
        assert_eq!(prevout.unlock_script.as_ref(), [0x51]);
        assert_eq!(prevout.previous_output.as_ref().unwrap().output_index, 3);
        assert_eq!(prevout.previous_value_zat, Some(456));
        assert_eq!(
            prevout.previous_address.as_deref(),
            Some(previous_address_string.as_str())
        );
        assert_eq!(prevout.previous_height, Some(12));
        assert_eq!(prevout.previous_from_coinbase, Some(true));

        let output = &encoded.transaction_outputs[0];
        assert_eq!(output.output_index, 0);
        assert_eq!(output.value_zat, 123);
        assert_eq!(
            output.address.as_deref(),
            Some(output_address_string.as_str())
        );

        let Some(utxo_change::Change::Spent(spent)) = &encoded.utxo_changes[0].change else {
            panic!("the input must produce a spent UTXO change");
        };
        assert_eq!(spent.input_index, 0);
        assert_eq!(spent.sequence, 42);
        assert_eq!(spent.unlock_script.as_ref(), [0x51]);
        assert_eq!(spent.outpoint.as_ref().unwrap().output_index, 3);

        let Some(utxo_change::Change::Created(created)) = &encoded.utxo_changes[1].change else {
            panic!("the output must produce a created UTXO change");
        };
        assert_eq!(created.value_zat, 123);
        assert_eq!(
            created.address.as_deref(),
            Some(output_address_string.as_str())
        );
        assert_eq!(created.outpoint.as_ref().unwrap().output_index, 0);
        assert_eq!(
            created.outpoint.as_ref().unwrap().transaction_id,
            transaction_id.to_string()
        );
    }

    #[test]
    fn encodes_coinbase_input_without_a_spent_utxo() {
        let transaction = Transaction::V1 {
            inputs: vec![Input::Coinbase {
                height: Height(99),
                data: vec![1, 2, 3],
                sequence: 7,
            }],
            outputs: Vec::new(),
            lock_time: LockTime::unlocked(),
        };

        let encoded = encode_transparent_updates(
            &transaction,
            &transaction.hash().to_string(),
            &Network::Mainnet,
            PreviousOutputs::Block(&HashMap::new()),
            true,
            true,
        )
        .unwrap();
        assert!(encoded.transaction_outputs.is_empty());
        assert!(encoded.utxo_changes.is_empty());
        assert_eq!(encoded.transparent_input_value_zat, Some(0));
        assert_eq!(encoded.transparent_output_value_zat, 0);

        let input = &encoded.transaction_inputs[0];
        assert_eq!(input.input_index, 0);
        assert_eq!(input.sequence, 7);
        let Some(transparent_input::Input::Coinbase(coinbase)) = &input.input else {
            panic!("the transparent input must preserve its coinbase data");
        };
        assert_eq!(coinbase.height, 99);
        assert_eq!(coinbase.data.as_ref(), [1, 2, 3]);
    }

    #[test]
    fn encodes_full_mempool_transaction_with_previous_output_context() {
        let network = Network::Mainnet;
        let previous = OutPoint {
            hash: transaction::Hash([3; 32]),
            index: 1,
        };
        let previous_address = Address::from_pub_key_hash(network.t_addr_kind(), [4; 20]);
        let output_address = Address::from_pub_key_hash(network.t_addr_kind(), [5; 20]);
        let previous_output = Output::new(
            Amount::<NonNegative>::new(10_123),
            previous_address.script(),
        );
        let transaction = Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint: previous,
                unlock_script: Script::new(&[0x51]),
                sequence: 9,
            }],
            outputs: vec![Output::new(
                Amount::<NonNegative>::new(123),
                output_address.script(),
            )],
            lock_time: LockTime::unlocked(),
        };
        let mut verified = VerifiedUnminedTx::new(
            transaction.into(),
            Amount::<NonNegative>::new(10_000),
            2,
            3,
            Arc::new(vec![previous_output]),
        )
        .unwrap();
        verified.height = Some(Height(55));
        let transaction_id = verified.transaction.id();
        let event = EventEnvelope {
            schema_version: EVENT_SCHEMA_VERSION,
            session_id: SessionId(7),
            sequence: 11,
            observed_at: SystemTime::UNIX_EPOCH,
            payload: PluginEvent::MempoolChanged(MempoolEvent::new(
                network,
                MempoolEventKind::Added,
                vec![transaction_id].into(),
                vec![verified].into(),
            )),
        };

        let summary_only = encode_event(&event, true, true, false, None, 32).unwrap();
        assert_eq!(summary_only.len(), 1);
        assert_eq!(summary_only[0].event_type, EventType::MempoolChanged as i32);

        let updates = encode_event(&event, true, true, true, None, 32).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].event_type, EventType::MempoolChanged as i32);
        let Some(subscribe_update::Update::MempoolTransaction(transaction)) =
            updates[1].update.as_ref()
        else {
            panic!("added mempool transactions must include their full payload");
        };
        assert_eq!(updates[1].event_type, EventType::MempoolTransaction as i32);
        assert_eq!(transaction.miner_fee_zat, 10_000);
        assert_eq!(transaction.admitted_height, Some(55));
        assert_eq!(transaction.transparent_input_value_zat, Some(10_123));
        assert_eq!(transaction.transparent_output_value_zat, 123);
        assert!(!transaction.coinbase);
        assert!(transaction.has_transparent);
        assert!(!transaction.has_sapling);
        assert!(!transaction.has_orchard);
        assert_eq!(transaction.legacy_sigop_count, 2);
        assert_eq!(transaction.p2sh_sigop_count, 3);

        let Some(transparent_input::Input::Prevout(input)) =
            transaction.transparent_inputs[0].input.as_ref()
        else {
            panic!("the mempool input must retain its previous output");
        };
        assert_eq!(input.previous_value_zat, Some(10_123));
        assert_eq!(
            input.previous_address.as_deref(),
            Some(previous_address.to_string().as_str())
        );
        assert_eq!(input.previous_height, None);
        assert_eq!(input.previous_from_coinbase, None);
    }
}
