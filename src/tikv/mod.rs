use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use kvproto::{kvrpcpb, metapb, pdpb, tikvpb::TikvClient};

use crate::{
    pd::{Pd, Region},
    Result,
};

/// A key value store which provides transaction related functionality.
///
/// It can be a local mock key value store, or it refers to a remote TiKV server.
#[async_trait]
pub trait TxnKv {
    /// Returns the value corresponding to the supplied key at the given timestamp.
    async fn mvcc_get(&self, key: Bytes, ts: u64) -> Result<Bytes>;

    /// Returns a `Stream` of `KvPair`s in the range [start_key, end_key) at the given timestamp.
    ///
    /// If `key_only` is true, the values in the returned `KvPair`s are empty.
    fn mvcc_scan(
        &self,
        start_key: Bytes,
        end_key: Bytes,
        key_only: bool,
        reverse: bool,
        ts: u64,
    ) -> BoxStream<Result<KvPair>>;

    /// Performs the prewrite phase.
    async fn prewrite(
        &self,
        mutations: Vec<Mutation>,
        primary_lock: Bytes,
        start_ts: u64,
        lock_ttl: Duration,
    ) -> Result<()>;

    /// Performs the commit phase.
    async fn commit(&self, keys: Vec<Bytes>, start_ts: u64, commit_ts: u64) -> Result<()>;

    /// Returns the commit ts if the key at `start_ts` is committed. Otherwise, rolls back the
    /// key.
    async fn get_commit_ts_or_rollback(&self, key: Bytes, start_ts: u64) -> Result<Option<u64>>;

    /// Resolves all locks at `start_ts`.
    ///
    /// Rolls back the keys if `commit_ts` is `0`. Otherwise, commits the keys with `commit_ts`.
    async fn resolve_locks(&self, start_ts: u64, commit_ts: u64) -> Result<()>;

    /// Rolls back the supplied keys at `start_ts`.
    async fn batch_rollback(&self, keys: Vec<Bytes>, start_ts: u64) -> Result<()>;
}

pub struct Client<P: Pd, Kv: TxnKv> {
    pd: P,
    kv: Kv,
}

impl<P: Pd, Kv: TxnKv> Client<P, Kv> {
    pub async fn start_transaction(&self) -> Result<Transaction<P, Kv>> {
        unimplemented!()
    }

    /// Creates a read-only snapshot at the given timestamp.
    pub async fn snapshot(&self, ts: u64) -> Result<Snapshot<P, Kv>> {
        unimplemented!()
    }
}

pub struct Snapshot<P: Pd, Kv: TxnKv> {
    pd: P,
    kv: Kv,
    start_ts: u64,
    // in-memory cache to be added
}

impl<P: Pd, Kv: TxnKv> Snapshot<P, Kv> {
    pub async fn get(&mut self, key: Bytes) -> Result<Bytes> {
        unimplemented!()
    }

    pub fn scan(
        &mut self,
        start_key: Bytes,
        end_key: Bytes,
        key_only: bool,
        reverse: bool,
    ) -> BoxStream<Result<KvPair>> {
        unimplemented!()
    }

    // This function does not break the read-only property of a snapshot beacuse no new data is
    // written to the database. Rolling back uncommitted data is not really a modification.
    pub async fn resolve_locks(&mut self, locks: &[kvrpcpb::LockInfo]) -> Result<()> {
        unimplemented!()
    }
}

pub struct Transaction<P: Pd, Kv: TxnKv> {
    snapshot: Snapshot<P, Kv>,
    mutations: Vec<Mutation>,
    // indexed mutations for fast lookup to be added
}

impl<P: Pd, Kv: TxnKv> Transaction<P, Kv> {
    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        unimplemented!()
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        unimplemented!()
    }

    pub fn lock_keys(&mut self, keys: &[Bytes]) -> Result<()> {
        unimplemented!()
    }

    pub async fn commit(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl<P: Pd, Kv: TxnKv> Deref for Transaction<P, Kv> {
    type Target = Snapshot<P, Kv>;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl<P: Pd, Kv: TxnKv> DerefMut for Transaction<P, Kv> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.snapshot
    }
}

pub struct TransactionCommiter<P: Pd, Kv: TxnKv> {
    pd: P,
    kv: Kv,
}

struct SingleRegionSender<'a> {
    region: &'a Region,
    tikv_rpc_client: &'a TikvClient,
}

struct MultiRegionSender<P: Pd> {
    pd: P,
    store_client: HashMap<u64, TikvClient>,
}

impl<P: Pd> for MultiRegionSender<P> {
    fn 
}

#[async_trait]
impl<'a> TxnKv for TikvRegion<'a> {
    async fn mvcc_get(&self, key: Bytes, ts: u64) -> Result<Bytes> {
        unimplemented!()
    }

    fn mvcc_scan(
        &self,
        start_key: Bytes,
        end_key: Bytes,
        key_only: bool,
        reverse: bool,
        ts: u64,
    ) -> BoxStream<Result<KvPair>> {
        unimplemented!()
    }

    async fn prewrite(
        &self,
        mutations: Vec<Mutation>,
        primary_lock: Bytes,
        start_ts: u64,
        lock_ttl: Duration,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn commit(&self, keys: Vec<Bytes>, start_ts: u64, commit_ts: u64) -> Result<()> {
        unimplemented!()
    }

    async fn get_commit_ts_or_rollback(&self, key: Bytes, start_ts: u64) -> Result<Option<u64>> {
        unimplemented!()
    }

    async fn resolve_locks(&self, start_ts: u64, commit_ts: u64) -> Result<()> {
        unimplemented!()
    }

    async fn batch_rollback(&self, keys: Vec<Bytes>, start_ts: u64) -> Result<()> {
        unimplemented!()
    }
}

pub struct KvPair {
    pub key: Bytes,
    pub value: Bytes,
}

pub enum Mutation {
    Put(KvPair),
    Del(Bytes),
    Lock(Bytes),
    Rollback(Bytes),
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use grpcio::{ChannelBuilder, Environment};
//     use test::{black_box, Bencher};

//     #[bench]
//     fn get_by_region_bench(b: &mut Bencher) {
//         let env = Arc::new(Environment::new(1));
//         let channel = ChannelBuilder::new(env).connect("127.0.0.1:2379");
//         let client = Arc::new(TikvClient::new(channel));
//         let kv = TikvKv { rpc: client };
//         let region = Region {
//             id: 1,
//             start_key: Bytes::new(),
//             end_key: Bytes::new(),
//             region_epoch: metapb::RegionEpoch {
//                 conf_ver: 1,
//                 version: 1,
//             },
//             leader: metapb::Peer {
//                 id: 1,
//                 store_id: 1,
//                 is_learner: false,
//             },
//         };
//         b.iter(|| {
//             black_box(Kv.get_by_region(&region));
//         })
//     }
// }
