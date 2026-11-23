//! ACVDB replica trace: the log positions an ACVDB-traced read runs at.

use segment::types::SeqNumberType;
use tokio::sync::OwnedRwLockReadGuard;

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard::ShardId;

impl Collection {
    /// `ShardReplicaSet::acvdb_read_positions` of this peer's replica of `shard_id`.
    pub async fn acvdb_read_positions(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<(OwnedRwLockReadGuard<SeqNumberType>, SeqNumberType)> {
        let shard = self
            .shards_holder
            .read()
            .await
            .get_shard(shard_id)
            .cloned()
            .ok_or_else(|| {
                CollectionError::service_error(format!("ACVDB-traced read: no shard {shard_id}"))
            })?;
        shard.acvdb_read_positions().await
    }
}
