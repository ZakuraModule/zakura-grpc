use std::time::{SystemTime, UNIX_EPOCH};

use prost_types::Timestamp;
use zakura_chain::serialization::ZcashSerialize;
use zakura_geyser_plugin_interface::{
    BestChainChange, EventEnvelope, MempoolEventKind, PluginError, PluginEvent,
};
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, BestChainGrow, BestChainReset, BestChainUpdate,
    BlockUpdate, EventType, MempoolAction, MempoolUpdate, SubscribeUpdate,
};

pub(crate) fn encode_event(event: &EventEnvelope) -> Result<SubscribeUpdate, PluginError> {
    let (event_type, update) = match &event.payload {
        PluginEvent::BlockAccepted(block) => (
            EventType::BlockAccepted,
            subscribe_update::Update::Block(encode_block(block, false)?),
        ),
        PluginEvent::BlockFinalized(block) => (
            EventType::BlockFinalized,
            subscribe_update::Update::Block(encode_block(block, true)?),
        ),
        PluginEvent::BestChainChanged(change) => (
            EventType::BestChainChanged,
            subscribe_update::Update::BestChain(encode_best_chain(change)),
        ),
        PluginEvent::MempoolChanged(change) => {
            let action = match change.kind {
                MempoolEventKind::Added => MempoolAction::Added,
                MempoolEventKind::Invalidated => MempoolAction::Invalidated,
                MempoolEventKind::Mined => MempoolAction::Mined,
            };
            (
                EventType::MempoolChanged,
                subscribe_update::Update::Mempool(MempoolUpdate {
                    action: action.into(),
                    transaction_ids: change
                        .transaction_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                }),
            )
        }
    };

    Ok(SubscribeUpdate {
        schema_version: event.schema_version,
        session_id: event.session_id.0.to_be_bytes().to_vec(),
        sequence: event.sequence,
        observed_at: Some(timestamp(event.observed_at)),
        event_type: event_type.into(),
        filters: Vec::new(),
        update: Some(update),
    })
}

fn encode_block(
    block: &zakura_geyser_plugin_interface::BlockEvent,
    finalized: bool,
) -> Result<BlockUpdate, PluginError> {
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
        subscribe_update::Update::Mempool(_) | subscribe_update::Update::Pong(_) => None,
    }
}
