//! ACVDB replica trace: the log positions an ACVDB-traced read runs at.

use std::sync::Arc;

use segment::types::SeqNumberType;
use tokio::sync::OwnedRwLockReadGuard;

use super::LocalShard;

impl LocalShard {
    /// Hold the update operation lock for reading and return it with the read's `received`
    /// position. While the guard lives no operation is half-applied, and `*guard` (the highest
    /// op_num applied, see `UpdateOperationLock`) is the read's `applied` position, with
    /// `applied <= received`. The read run under the guard must not take the lock itself: a
    /// queued writer would block it behind this guard.
    pub async fn acvdb_read_positions(
        &self,
    ) -> (OwnedRwLockReadGuard<SeqNumberType>, SeqNumberType) {
        // The WAL is read before taking the guard: `try_recover` holds the WAL lock while it
        // waits for the update lock, so taking the WAL lock under the guard could deadlock.
        let wal_last_index = self.wal.wal.lock().await.last_index();
        let applied = Arc::clone(&self.update_operation_lock).read_owned().await;
        // An operation applied since is in the WAL too: it's appended before it's applied.
        let received = wal_last_index.max(*applied);
        (applied, received)
    }
}
