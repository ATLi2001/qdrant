//! ACVDB replica trace: the log positions an ACVDB-traced read runs at.

use std::ops::Deref as _;

use segment::types::SeqNumberType;
use tokio::sync::OwnedRwLockReadGuard;

use super::ShardReplicaSet;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard::Shard;

impl ShardReplicaSet {
    /// `LocalShard::acvdb_read_positions` of this replica's local shard. Fails without a plain
    /// local shard (none, or a proxy during a shard transfer, which ACVDB runs don't do).
    pub async fn acvdb_read_positions(
        &self,
    ) -> CollectionResult<(OwnedRwLockReadGuard<SeqNumberType>, SeqNumberType)> {
        let local = self.local.read().await;
        match local.deref() {
            Some(Shard::Local(local_shard)) => Ok(local_shard.acvdb_read_positions().await),
            Some(Shard::ForwardProxy(_) | Shard::QueueProxy(_) | Shard::Dummy(_)) | None => {
                Err(CollectionError::service_error(format!(
                    "ACVDB-traced read: shard {} has no plain local shard on this peer",
                    self.shard_id,
                )))
            }
        }
    }
}
