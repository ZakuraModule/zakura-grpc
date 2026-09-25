use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use prost_types::Timestamp;
use zakura_chain::{
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::{self, Transaction},
    transparent,
};
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, EventEnvelope, MempoolEventKind, PluginError, PluginEvent,
};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, transparent_input, utxo_change, BestChainGrow,
    BestChainReset, BestChainUpdate, BlockCommitment, BlockUpdate, EventType, MempoolAction,
    MempoolUpdate, Outpoint, SubscribeUpdate, TransactionUpdate, TransparentCoinbaseInput,
    TransparentInput, TransparentOutput, TransparentPrevoutInput, UtxoChange, UtxoCreated,
    UtxoSpent, UtxoUpdate,
};

pub(crate) fn encode_event(
    event: &EventEnvelope,
    transaction_updates: bool,
    utxo_updates: bool,
) -> Result<Vec<SubscribeUpdate>, PluginError> {
    let mut updates = match &event.payload {
        PluginEvent::BlockAccepted(block) => vec![new_update(
            event,
            EventType::BlockAccepted,
            subscribe_update::Update::Block(encode_block(block, false)?),
        )],
        PluginEvent::BlockFinalized(block) => vec![new_update(
            event,
            EventType::BlockFinalized,
            subscribe_update::Update::Block(encode_block(block, true)?),
        )],
        PluginEvent::BestChainChanged(change) => vec![new_update(
            event,
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
                event,
                EventType::MempoolChanged,
                subscribe_update::Update::Mempool(MempoolUpdate {
                    action: action.into(),
                    transaction_ids: change
                        .transaction_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                }),
            )]
        }
    };

    match &event.payload {
        PluginEvent::BlockAccepted(block) => append_block_updates(
            &mut updates,
            event,
            block,
            BlockCommitment::Accepted,
            transaction_updates,
            utxo_updates,
        )?,
        PluginEvent::BlockFinalized(block) => append_block_updates(
            &mut updates,
            event,
            block,
            BlockCommitment::Finalized,
            transaction_updates,
            utxo_updates,
        )?,
        PluginEvent::BestChainChanged(_) | PluginEvent::MempoolChanged(_) => {}
    }

    Ok(updates)
}

fn append_block_updates(
    updates: &mut Vec<SubscribeUpdate>,
    event: &EventEnvelope,
    block: &BlockEvent,
    commitment: BlockCommitment,
    transaction_updates: bool,
    utxo_updates: bool,
) -> Result<(), PluginError> {
    if !transaction_updates && !utxo_updates {
        return Ok(());
    }

    for (transaction_index, transaction) in block.block.transactions.iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            PluginError::new("transaction index exceeds the gRPC protocol's u32 range")
        })?;
        let (transaction_id, auth_digest) = transaction.txid_and_auth_digest();
        let EncodedTransparentUpdates {
            transaction_inputs,
            transaction_outputs,
            utxo_changes,
            transparent_input_value_zat,
            transparent_output_value_zat,
        } = encode_transparent_updates(
            transaction,
            transaction_id,
            &block.network,
            &block.spent_outputs,
            transaction_updates,
            utxo_updates,
        )?;

        if transaction_updates {
            let unmined_transaction_id = encode_unmined_transaction_id(transaction_id, auth_digest);
            let transaction_bytes = transaction.zcash_serialize_to_vec().map_err(|error| {
                PluginError::new(format!("failed to encode transaction: {error}"))
            })?;
            updates.push(new_update(
                event,
                EventType::Transaction,
                subscribe_update::Update::Transaction(TransactionUpdate {
                    transaction_id: transaction_id.to_string(),
                    unmined_transaction_id,
                    auth_digest: auth_digest.map(|digest| digest.to_string()),
                    transaction: transaction_bytes.into(),
                    height: block.height.0,
                    block_hash: block.hash.to_string(),
                    transaction_index,
                    commitment: commitment.into(),
                    coinbase: transaction.is_coinbase(),
                    transparent_inputs: transaction_inputs,
                    transparent_outputs: transaction_outputs,
                    version: transaction.version(),
                    lock_time: transaction.raw_lock_time(),
                    lock_time_is_time: transaction.lock_time_is_time(),
                    expiry_height: transaction.expiry_height().map(|height| height.0),
                    network: block.network.to_string(),
                    transparent_input_value_zat,
                    transparent_output_value_zat,
                }),
            ));
        }

        if utxo_updates && !utxo_changes.is_empty() {
            updates.push(new_update(
                event,
                EventType::Utxo,
                subscribe_update::Update::Utxo(UtxoUpdate {
                    height: block.height.0,
                    block_hash: block.hash.to_string(),
                    transaction_id: transaction_id.to_string(),
                    transaction_index,
                    commitment: commitment.into(),
                    changes: utxo_changes,
                }),
            ));
        }
    }

    Ok(())
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
    spent_outputs: &'a HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    transaction_updates: bool,
    utxo_updates: bool,
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
    transaction_id: transaction::Hash,
    network: &Network,
    spent_outputs: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    transaction_updates: bool,
    utxo_updates: bool,
) -> Result<EncodedTransparentUpdates, PluginError> {
    let context = TransparentEncodingContext {
        network,
        spent_outputs,
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
    let mut transparent_input_value_zat = transaction_updates.then_some(0u64);
    let mut transparent_output_value_zat = 0u64;

    for (input_index, input) in transaction.inputs().iter().enumerate() {
        let input_index = u32::try_from(input_index)
            .map_err(|_| PluginError::new("input index exceeds the gRPC protocol's u32 range"))?;
        let input_value_zat = encode_transparent_input(
            &context,
            input,
            input_index,
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
            transaction_id,
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
    input_index: u32,
    transaction_inputs: &mut Vec<TransparentInput>,
    utxo_changes: &mut Vec<UtxoChange>,
) -> Option<u64> {
    match input {
        transparent::Input::PrevOut {
            outpoint,
            unlock_script,
            sequence,
        } => {
            let previous = encode_previous_output(context, outpoint);
            let grpc_outpoint = Outpoint {
                transaction_id: outpoint.hash.to_string(),
                output_index: outpoint.index,
            };
            let unlock_script = Bytes::copy_from_slice(unlock_script.as_raw_bytes());

            if context.transaction_updates {
                transaction_inputs.push(TransparentInput {
                    input_index,
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
                        input_index,
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
                    input_index,
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
    transaction_id: transaction::Hash,
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
                    transaction_id: transaction_id.to_string(),
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
) -> EncodedPreviousOutput {
    let Some(previous) = context.spent_outputs.get(outpoint) else {
        return EncodedPreviousOutput::default();
    };
    let output = &previous.utxo.output;
    EncodedPreviousOutput {
        value_zat: Some(
            u64::try_from(output.value.zatoshis())
                .expect("transparent output values are non-negative by type"),
        ),
        lock_script: Some(Bytes::copy_from_slice(output.lock_script.as_raw_bytes())),
        address: output
            .address(context.network)
            .map(|address| address.to_string()),
        height: Some(previous.utxo.height.0),
        from_coinbase: Some(previous.utxo.from_coinbase),
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

fn new_update(
    event: &EventEnvelope,
    event_type: EventType,
    update: subscribe_update::Update,
) -> SubscribeUpdate {
    SubscribeUpdate {
        schema_version: event.schema_version,
        session_id: Bytes::copy_from_slice(&event.session_id.0.to_be_bytes()),
        sequence: 0,
        observed_at: Some(timestamp(event.observed_at)),
        event_type: event_type.into(),
        filters: Vec::new(),
        source_sequence: event.sequence,
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
        BestChainChange::Reset { height, hash } => BestChainUpdate {
            height: height.0,
            hash: hash.to_string(),
            change: Some(best_chain_update::Change::Reset(BestChainReset {})),
        },
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
        subscribe_update::Update::Mempool(_) | subscribe_update::Update::Pong(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{
        amount::{Amount, NonNegative},
        block::Height,
        transaction::LockTime,
        transparent::{Address, Input, OrderedUtxo, OutPoint, Output, Script},
    };

    use super::*;

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
            transaction_id,
            &network,
            &spent_outputs,
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
            transaction.hash(),
            &Network::Mainnet,
            &HashMap::new(),
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
}
