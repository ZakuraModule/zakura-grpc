use std::time::{SystemTime, UNIX_EPOCH};

use prost_types::Timestamp;
use zakura_chain::{
    serialization::ZcashSerialize,
    transaction::{self, Transaction},
    transparent,
};
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, EventEnvelope, MempoolEventKind, PluginError, PluginEvent,
};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, utxo_change, BestChainGrow, BestChainReset,
    BestChainUpdate, BlockCommitment, BlockUpdate, EventType, MempoolAction, MempoolUpdate,
    Outpoint, SubscribeUpdate, TransactionUpdate, UtxoChange, UtxoCreated, UtxoSpent, UtxoUpdate,
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
                    transaction: transaction_bytes,
                    height: block.height.0,
                    block_hash: block.hash.to_string(),
                    transaction_index,
                    commitment: commitment.into(),
                    coinbase: transaction.is_coinbase(),
                }),
            ));
        }

        if utxo_updates {
            let changes = encode_utxo_changes(transaction, transaction_id)?;
            if !changes.is_empty() {
                updates.push(new_update(
                    event,
                    EventType::Utxo,
                    subscribe_update::Update::Utxo(UtxoUpdate {
                        height: block.height.0,
                        block_hash: block.hash.to_string(),
                        transaction_id: transaction_id.to_string(),
                        transaction_index,
                        commitment: commitment.into(),
                        changes,
                    }),
                ));
            }
        }
    }

    Ok(())
}

fn encode_utxo_changes(
    transaction: &Transaction,
    transaction_id: transaction::Hash,
) -> Result<Vec<UtxoChange>, PluginError> {
    let mut changes = Vec::with_capacity(
        transaction
            .inputs()
            .len()
            .saturating_add(transaction.outputs().len()),
    );

    for (input_index, input) in transaction.inputs().iter().enumerate() {
        let transparent::Input::PrevOut {
            outpoint,
            unlock_script,
            sequence,
        } = input
        else {
            continue;
        };
        let input_index = u32::try_from(input_index)
            .map_err(|_| PluginError::new("input index exceeds the gRPC protocol's u32 range"))?;
        changes.push(UtxoChange {
            change: Some(utxo_change::Change::Spent(UtxoSpent {
                outpoint: Some(Outpoint {
                    transaction_id: outpoint.hash.to_string(),
                    output_index: outpoint.index,
                }),
                input_index,
                unlock_script: unlock_script.as_raw_bytes().to_vec(),
                sequence: *sequence,
            })),
        });
    }

    for (output_index, output) in transaction.outputs().iter().enumerate() {
        let output_index = u32::try_from(output_index)
            .map_err(|_| PluginError::new("output index exceeds the gRPC protocol's u32 range"))?;
        let value_zat = u64::try_from(output.value.zatoshis())
            .expect("transparent output values are non-negative by type");
        changes.push(UtxoChange {
            change: Some(utxo_change::Change::Created(UtxoCreated {
                outpoint: Some(Outpoint {
                    transaction_id: transaction_id.to_string(),
                    output_index,
                }),
                value_zat,
                lock_script: output.lock_script.as_raw_bytes().to_vec(),
            })),
        });
    }

    Ok(changes)
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
        session_id: event.session_id.0.to_be_bytes().to_vec(),
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
        block: block_bytes,
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
        transaction::LockTime,
        transparent::{Input, OutPoint, Output, Script},
    };

    use super::*;

    #[test]
    fn encodes_transaction_and_ordered_utxo_changes() {
        let previous = OutPoint {
            hash: transaction::Hash([7; 32]),
            index: 3,
        };
        let transaction = Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint: previous,
                unlock_script: Script::new(&[0x51]),
                sequence: 42,
            }],
            outputs: vec![Output::new(
                Amount::<NonNegative>::new(123),
                Script::new(&[0x52]),
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

        let changes = encode_utxo_changes(&transaction, transaction_id).unwrap();
        assert_eq!(changes.len(), 2);

        let Some(utxo_change::Change::Spent(spent)) = &changes[0].change else {
            panic!("the input must produce a spent UTXO change");
        };
        assert_eq!(spent.input_index, 0);
        assert_eq!(spent.sequence, 42);
        assert_eq!(spent.unlock_script, vec![0x51]);
        assert_eq!(spent.outpoint.as_ref().unwrap().output_index, 3);

        let Some(utxo_change::Change::Created(created)) = &changes[1].change else {
            panic!("the output must produce a created UTXO change");
        };
        assert_eq!(created.value_zat, 123);
        assert_eq!(created.lock_script, vec![0x52]);
        assert_eq!(created.outpoint.as_ref().unwrap().output_index, 0);
        assert_eq!(
            created.outpoint.as_ref().unwrap().transaction_id,
            transaction_id.to_string()
        );
    }
}
