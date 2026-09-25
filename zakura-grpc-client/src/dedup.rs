use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use zakura_grpc_proto::geyser::{subscribe_update, SubscribeUpdate};

/// Number of block heights retained for reconnect duplicate detection.
pub const DEFAULT_HEIGHT_RETENTION: usize = 250;

type EventKey = (Bytes, u64);

#[derive(Clone, Debug)]
struct HeightBucket {
    height: u32,
    keys: Vec<EventKey>,
}

/// Bounded set of block-scoped events already delivered to the application.
#[derive(Clone, Debug)]
pub struct DedupState {
    seen: HashSet<EventKey>,
    heights: VecDeque<HeightBucket>,
    height_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self::with_height_retention(DEFAULT_HEIGHT_RETENTION)
    }
}

impl DedupState {
    /// Creates deduplication state retaining keys from this many block heights.
    #[must_use]
    pub fn with_height_retention(height_retention: usize) -> Self {
        Self {
            seen: HashSet::new(),
            heights: VecDeque::new(),
            height_retention,
        }
    }

    /// Returns `true` once for each block-scoped `(session_id, sequence)` key.
    ///
    /// Non-block events are not replayed by the server and are always accepted.
    pub fn observe(&mut self, update: &SubscribeUpdate) -> bool {
        let Some(height) = update_height(update) else {
            return true;
        };
        if self.height_retention == 0 {
            return true;
        }

        let key = (update.session_id.clone(), update.sequence);
        if !self.seen.insert(key.clone()) {
            return false;
        }

        if let Some(bucket) = self
            .heights
            .iter_mut()
            .find(|bucket| bucket.height == height)
        {
            bucket.keys.push(key);
        } else {
            self.heights.push_back(HeightBucket {
                height,
                keys: vec![key],
            });
        }

        while self.heights.len() > self.height_retention {
            let bucket = self
                .heights
                .pop_front()
                .expect("a height bucket exists because retention was exceeded");
            for key in bucket.keys {
                self.seen.remove(&key);
            }
        }
        true
    }
}

pub(crate) fn update_height(update: &SubscribeUpdate) -> Option<u32> {
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
    use zakura_grpc_proto::geyser::{subscribe_update, BlockUpdate};

    use super::*;

    fn update(session: u8, height: u32, sequence: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            session_id: Bytes::from(vec![session]),
            sequence,
            update: Some(subscribe_update::Update::Block(BlockUpdate {
                height,
                ..BlockUpdate::default()
            })),
            ..SubscribeUpdate::default()
        }
    }

    #[test]
    fn suppresses_replayed_key() {
        let mut state = DedupState::default();
        let update = update(1, 10, 20);
        assert!(state.observe(&update));
        assert!(!state.observe(&update));
    }

    #[test]
    fn same_sequence_from_new_server_session_is_new() {
        let mut state = DedupState::default();
        assert!(state.observe(&update(1, 10, 20)));
        assert!(state.observe(&update(2, 10, 20)));
    }

    #[test]
    fn evicted_height_can_be_observed_again() {
        let mut state = DedupState::with_height_retention(1);
        assert!(state.observe(&update(1, 10, 20)));
        assert!(state.observe(&update(1, 11, 21)));
        assert!(state.observe(&update(1, 10, 20)));
    }
}
