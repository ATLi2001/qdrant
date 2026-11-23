//! ACVDB replica trace (ACVDB's `common/workload/replica-trace.h`).
//!
//! With `ACVDB_REPLICA_TRACE=<path>` and `ACVDB_NODE=<i>` set, each REST upsert, delete, search
//! and point get carrying the `x-acvdb-client` and `x-acvdb-req` headers appends one CSV line to
//! `<path>`. A write is logged by the peer the client sent it to, which ACVDB requires be the
//! leader under `ordering=strong` (the highest peer id), with `position` = the leader's WAL
//! op_num (`UpdateResult::operation_id`). A read runs on this peer's local replica only, under
//! the update operation lock, and logs `applied`/`received` (`acvdb_read_positions`).
//!
//! Positions are raw WAL op_nums: ACVDB's experiment script rebases them so the first traced
//! write is position 1. Without the environment variables the headers are ignored and requests
//! run as in stock Qdrant.

use std::convert::Infallible;
use std::fs::File;
use std::future::{Ready, ready};
use std::io::Write as _;
use std::sync::LazyLock;

use actix_web::{FromRequest, HttpRequest};
use collection::shards::shard::ShardId;
use parking_lot::Mutex;
use segment::types::SeqNumberType;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::{AccessRequirements, Auth};
use tokio::sync::OwnedRwLockReadGuard;

const CLIENT_HEADER: &str = "x-acvdb-client";
const REQ_HEADER: &str = "x-acvdb-req";

/// ACVDB runs use one shard, so a traced read selects it by id (which also makes it local-only).
pub const TRACED_SHARD_ID: ShardId = 0;

/// The replica trace's header line, as `kReplicaTraceHeader`.
const TRACE_HEADER: &str = "kind,client_id,req_id,node,position,applied,received,sync\n";

struct Trace {
    node: u32,
    file: Mutex<File>,
}

/// `None` when tracing is off. Opened by `init` at startup, so a bad path fails there.
static TRACE: LazyLock<Option<Trace>> = LazyLock::new(|| {
    let path = std::env::var("ACVDB_REPLICA_TRACE").ok()?;
    let node = std::env::var("ACVDB_NODE")
        .expect("ACVDB_REPLICA_TRACE is set but ACVDB_NODE is not")
        .parse()
        .expect("ACVDB_NODE is not a node index");
    let mut file = File::create(&path)
        .unwrap_or_else(|err| panic!("Can't create ACVDB replica trace {path}: {err}"));
    file.write_all(TRACE_HEADER.as_bytes())
        .unwrap_or_else(|err| panic!("Can't write ACVDB replica trace {path}: {err}"));
    log::info!("Writing ACVDB replica trace for node {node} to {path}");
    Some(Trace {
        node,
        file: Mutex::new(file),
    })
});

/// Open the trace if `ACVDB_REPLICA_TRACE` is set.
pub fn init() {
    LazyLock::force(&TRACE);
}

/// The request's client and request ids, as in ACVDB's trace (`OpRecord`).
#[derive(Clone, Copy, Debug)]
pub struct TraceIds {
    client_id: u32,
    req_id: u64,
}

/// Actix extractor: `Some` iff tracing is on and the request carries both id headers.
/// A request with a missing or malformed id isn't traced (with a warning), so it can't
/// silently join the wrong client-trace record.
pub struct AcvdbIds(pub Option<TraceIds>);

impl FromRequest for AcvdbIds {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        ready(Ok(AcvdbIds(
            TRACE.as_ref().and_then(|_| ids_from_request(req)),
        )))
    }
}

fn ids_from_request(req: &HttpRequest) -> Option<TraceIds> {
    let header = |name| req.headers().get(name).map(|value| value.to_str());
    match (header(CLIENT_HEADER), header(REQ_HEADER)) {
        (None, None) => None,
        (Some(Ok(client)), Some(Ok(req))) => match (client.parse(), req.parse()) {
            (Ok(client_id), Ok(req_id)) => Some(TraceIds { client_id, req_id }),
            (Err(_), _) | (_, Err(_)) => {
                log::warn!("Malformed ACVDB trace ids {client:?}/{req:?}; not traced");
                None
            }
        },
        (Some(_), _) | (_, Some(_)) => {
            log::warn!("Request lacks one of the ACVDB trace id headers; not traced");
            None
        }
    }
}

/// The trace's op kinds (ACVDB's `OpTypeName`).
#[derive(Clone, Copy, Debug)]
pub enum TracedOp {
    Upsert,
    Delete,
    Search,
    Get,
}

impl TracedOp {
    fn name(self) -> &'static str {
        match self {
            TracedOp::Upsert => "upsert",
            TracedOp::Delete => "delete",
            TracedOp::Search => "search",
            TracedOp::Get => "get",
        }
    }
}

fn append(line: std::fmt::Arguments) {
    let Some(trace) = TRACE.as_ref() else {
        return;
    };
    // Written through at once, so the trace survives however the server exits.
    if let Err(err) = trace.file.lock().write_fmt(line) {
        log::error!("Can't write ACVDB replica trace: {err}");
    }
}

/// Log a write the leader took up at WAL op_num `position`.
pub fn record_write(op: TracedOp, ids: TraceIds, position: Option<SeqNumberType>) {
    let Some(position) = position else {
        log::warn!(
            "ACVDB-traced {} {ids:?} has no operation id; not traced",
            op.name()
        );
        return;
    };
    let node = TRACE.as_ref().map_or(0, |trace| trace.node);
    let TraceIds { client_id, req_id } = ids;
    append(format_args!(
        "{},{client_id},{req_id},{node},{position},,,0\n",
        op.name(),
    ));
}

/// Log a read this peer ran with the writes up to `applied` applied and up to `received` taken
/// up.
pub fn record_read(op: TracedOp, ids: TraceIds, applied: SeqNumberType, received: SeqNumberType) {
    let node = TRACE.as_ref().map_or(0, |trace| trace.node);
    let TraceIds { client_id, req_id } = ids;
    append(format_args!(
        "{},{client_id},{req_id},{node},,{applied},{received},\n",
        op.name(),
    ));
}

/// For a traced read: hold this peer's replica of the one shard at a fixed applied position
/// (see `LocalShard::acvdb_read_positions`). Run the read while the guard lives.
pub async fn read_positions(
    toc: &TableOfContent,
    auth: &Auth,
    collection_name: &str,
) -> Result<(OwnedRwLockReadGuard<SeqNumberType>, SeqNumberType), StorageError> {
    let pass = auth.check_collection_access(
        collection_name,
        AccessRequirements::new(),
        "acvdb_read_positions",
    )?;
    let collection = toc.get_collection(&pass).await?;
    Ok(collection.acvdb_read_positions(TRACED_SHARD_ID).await?)
}
