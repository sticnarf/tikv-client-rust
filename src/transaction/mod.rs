// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Transactional related functionality.
//!
//! Using the [`TransactionClient`](TransactionClient) you can utilize TiKV's transactional interface.
//!
//! This interface offers SQL-like transactions on top of the raw interface.
//!
//! **Warning:** It is not advisable to use both raw and transactional functionality in the same keyspace.
//!

pub use self::client::{Client, Connect};

use crate::{
    kv_client::requests::{mvcc::*, KvRequest},
    pd::{PdClient, PdRpcClient},
    Key, KvPair, Result, Value,
};
use derive_new::new;
use futures::stream::BoxStream;
use kvproto::kvrpcpb;
use std::{collections::BTreeMap, ops::RangeBounds, sync::Arc};

mod client;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub struct Timestamp {
    pub physical: i64,
    pub logical: i64,
}

impl Timestamp {
    pub fn version(&self) -> u64 {
        assert!(self.logical > 0 && self.logical < (1 << 18));
        // `physical` is the current millis since Epoch so we don't need to care about overflow
        ((self.physical << 18) + self.logical) as u64
    }
}

#[derive(Debug, Clone)]
enum Mutation {
    Put(Value),
    Del,
    Lock,
}

impl Mutation {
    fn into_proto_with_key(self, key: Key) -> kvrpcpb::Mutation {
        let mut pb = kvrpcpb::Mutation {
            key: key.into(),
            ..Default::default()
        };
        match self {
            Mutation::Put(v) => {
                pb.set_op(kvrpcpb::Op::Put);
                pb.set_value(v.into());
            }
            Mutation::Del => pb.set_op(kvrpcpb::Op::Del),
            Mutation::Lock => pb.set_op(kvrpcpb::Op::Lock),
        };
        pb
    }

    fn get_value(&self) -> MutationValue {
        match self {
            Mutation::Put(value) => MutationValue::Determined(Some(value.clone())),
            Mutation::Del => MutationValue::Determined(None),
            Mutation::Lock => MutationValue::Undetermined,
        }
    }
}

enum MutationValue {
    Determined(Option<Value>),
    Undetermined,
}

/// A undo-able set of actions on the dataset.
///
/// Using a transaction you can prepare a set of actions (such as `get`, or `set`) on data at a
/// particular timestamp obtained from the placement driver.
///
/// Once a transaction is commited, a new commit timestamp is obtained from the placement driver.
///
/// Create a new transaction from a snapshot using `new`.
///
/// ```rust,no_run
/// # #![feature(async_await)]
/// use tikv_client::{Config, TransactionClient};
/// use futures::prelude::*;
/// # futures::executor::block_on(async {
/// let connect = TransactionClient::connect(Config::default());
/// let client = connect.await.unwrap();
/// let txn = client.begin().await.unwrap();
/// # });
/// ```
#[derive(new)]
pub struct Transaction<Pd: PdClient = PdRpcClient> {
    snapshot: Snapshot<Pd>,
    #[new(default)]
    mutations: BTreeMap<Key, Mutation>,
}

impl<Pd: PdClient> Transaction<Pd> {
    /// Gets the value associated with the given key.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Value, Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = Client::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Option<Value> = txn.get(key).await.unwrap();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let key = key.into();
        match self.get_from_mutations(&key) {
            MutationValue::Determined(value) => Ok(value),
            MutationValue::Undetermined => self.snapshot.get(key).await,
        }
    }

    /// Gets the values associated with the given keys. The returned iterator is in the same order
    /// as the given keys.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Key, Value, Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = Client::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let result: HashMap<Key, Value> = txn
    ///     .batch_get(keys)
    ///     .await
    ///     .unwrap()
    ///     .filter_map(|(k, v)| v.map(move |v| (k, v))).collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = (Key, Option<Value>)>> {
        let mut undetermined_keys = Vec::new();
        let mut results_in_buffer = Vec::new();
        for key in keys {
            let key = key.into();
            let mutation_value = self.get_from_mutations(&key);
            // If the value cannot be determined according to the buffered mutations, we need to
            // query from the snapshot.
            if let MutationValue::Undetermined = mutation_value {
                undetermined_keys.push(key.clone());
            }
            results_in_buffer.push((key, mutation_value));
        }
        let mut results_from_snapshot = self.snapshot.batch_get(undetermined_keys).await?;
        Ok(results_in_buffer
            .into_iter()
            .map(move |(key, mutation_value)| match mutation_value {
                MutationValue::Determined(value) => (key, value),
                // `results_from_snapshot` should contain all undetermined keys. If not, it's a bug
                // in `Snapshot::batch_get`.
                MutationValue::Undetermined => results_from_snapshot
                    .next()
                    .expect("not enough results from snapshot"),
            }))
    }

    pub fn scan(&self, _range: impl RangeBounds<Key>) -> BoxStream<Result<KvPair>> {
        unimplemented!()
    }

    pub fn scan_reverse(&self, _range: impl RangeBounds<Key>) -> BoxStream<Result<KvPair>> {
        unimplemented!()
    }

    /// Sets the value associated with the given key.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Key, Value, Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = Client::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let val = "TiKV".to_owned();
    /// txn.set(key, val);
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub fn set(&mut self, key: impl Into<Key>, value: impl Into<Value>) {
        self.mutations
            .insert(key.into(), Mutation::Put(value.into()));
    }

    /// Deletes the given key.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Key, Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = Client::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// txn.delete(key);
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub fn delete(&mut self, key: impl Into<Key>) {
        self.mutations.insert(key.into(), Mutation::Del);
    }

    /// Locks the given keys.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connect = Client::connect(Config::default());
    /// # let connected_client = connect.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// txn.lock_keys(vec!["TiKV".to_owned(), "Rust".to_owned()]);
    /// // ... Do some actions.
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub fn lock_keys(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) {
        for key in keys {
            let key = key.into();
            // Mutated keys don't need a lock.
            self.mutations.entry(key).or_insert(Mutation::Lock);
        }
    }

    /// Commits the actions of the transaction.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Config, transaction::Client};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connect = Client::connect(Config::default());
    /// # let connected_client = connect.await.unwrap();
    /// let mut txn = connected_client.begin().await.unwrap();
    /// // ... Do some actions.
    /// let req = txn.commit();
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn commit(&mut self) -> Result<()> {
        self.prewrite().await?;
        self.commit_primary().await?;
        // FIXME: return from this method once the primary key is committed
        let _ = self.commit_secondary().await;
        Ok(())
    }

    /// Returns the timestamp which the transaction started at.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Config, transaction::{Client, Timestamp}};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connect = Client::connect(Config::default());
    /// # let connected_client = connect.await.unwrap();
    /// let txn = connected_client.begin().await.unwrap();
    /// // ... Do some actions.
    /// let ts: Timestamp = txn.start_ts();
    /// # });
    /// ```
    pub fn start_ts(&self) -> Timestamp {
        self.snapshot().timestamp
    }

    /// Gets the `Snapshot` the transaction is operating on.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Config, transaction::{Client, Snapshot}};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connect = Client::connect(Config::default());
    /// # let connected_client = connect.await.unwrap();
    /// let txn = connected_client.begin().await.unwrap();
    /// // ... Do some actions.
    /// let snap: &Snapshot = txn.snapshot();
    /// # });
    /// ```
    pub fn snapshot(&self) -> &Snapshot<Pd> {
        &self.snapshot
    }

    async fn prewrite(&mut self) -> Result<()> {
        // TODO: Too many clones. Consider using bytes::Byte.
        let _rpc_mutations: Vec<_> = self
            .mutations
            .iter()
            .map(|(k, v)| v.clone().into_proto_with_key(k.clone()))
            .collect();
        unimplemented!()
    }

    async fn commit_primary(&mut self) -> Result<()> {
        unimplemented!()
    }

    async fn commit_secondary(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn get_from_mutations(&self, key: &Key) -> MutationValue {
        self.mutations
            .get(key)
            .map(Mutation::get_value)
            .unwrap_or(MutationValue::Undetermined)
    }
}

pub struct TxnInfo {
    pub txn: u64,
    pub status: u64,
}

/// A snapshot of dataset at a particular point in time.
#[derive(new)]
pub struct Snapshot<Pd: PdClient = PdRpcClient> {
    pd: Arc<Pd>,
    timestamp: Timestamp,
}

impl<Pd: PdClient> Snapshot<Pd> {
    /// Gets the value associated with the given key.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = TransactionClient::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let snapshot = connected_client.snapshot().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Option<Value> = snapshot.get(key).await.unwrap();
    /// # });
    /// ```
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        // Note: Here we use batch_get to implement get because we cannot distinguish an empty
        // value from a non-existent key using the kv_get interface.
        // See: https://github.com/tikv/client-rust/issues/99
        Ok(self
            .batch_get(std::iter::once(key))
            .await?
            .next()
            .map(|(_, v)| v)
            .expect("no result from batch_get"))
    }

    /// Gets the values associated with the given keys. The returned iterator is in the same order
    /// as the given keys.
    ///
    /// ```rust,no_run
    /// # #![feature(async_await)]
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let connecting_client = TransactionClient::connect(Config::new(vec!["192.168.0.100", "192.168.0.101"]));
    /// # let connected_client = connecting_client.await.unwrap();
    /// let snapshot = connected_client.snapshot().await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let result: HashMap<Key, Value> = snapshot
    ///     .batch_get(keys)
    ///     .await
    ///     .unwrap()
    ///     .filter_map(|(k, v)| v.map(move |v| (k, v))).collect();
    /// # });
    /// ```
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = (Key, Option<Value>)>> {
        let keys: Vec<_> = keys.into_iter().map(Into::into).collect();
        let mut result_pairs = MvccBatchGet {
            keys: keys.clone(),
            version: self.timestamp.version(),
        }
        .execute(self.pd.clone())
        .await?
        .into_iter()
        .peekable();

        let mut keys = keys.into_iter();
        Ok(std::iter::from_fn(move || {
            // BatchGet does not return pairs whose key does not exist but the order is retained.
            // Here we keep two pointers: one points to the supplied keys and the other points to
            // the result pairs. Every time the first pointer moves forward, we check if the second
            // pointer points the same key. If so, we return the pair and moves forward the second
            // pointer. Otherwise, `None` is returned as the value since the current key does not
            // exist in the snapshot.
            let key = keys.next()?;
            match result_pairs.peek() {
                Some(KvPair(result_key, _)) if &key == result_key => result_pairs
                    .next()
                    .map(|KvPair(key, value)| (key, Some(value))),
                _ => Some((key, None)),
            }
        }))
    }

    pub fn scan(&self, range: impl RangeBounds<Key>) -> BoxStream<Result<KvPair>> {
        drop(range);
        unimplemented!()
    }

    pub fn scan_reverse(&self, range: impl RangeBounds<Key>) -> BoxStream<Result<KvPair>> {
        drop(range);
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::MockPdClient;
    use futures::executor::block_on;

    #[test]
    fn set_and_get_from_buffer() {
        let mut txn = mock_txn();
        txn.set(b"key1".to_vec(), b"value1".to_vec());
        txn.set(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(
            block_on(txn.get(b"key1".to_vec())).unwrap().unwrap(),
            b"value1".to_vec().into()
        );

        txn.delete(b"key2".to_vec());
        txn.set(b"key1".to_vec(), b"value".to_vec());
        assert_eq!(
            block_on(txn.batch_get(vec![b"key2".to_vec(), b"key1".to_vec()]))
                .unwrap()
                .collect::<Vec<_>>(),
            vec![
                (Key::from(b"key2".to_vec()), None),
                (
                    Key::from(b"key1".to_vec()),
                    Some(Value::from(b"value".to_vec()))
                ),
            ]
        );
    }

    fn mock_txn() -> Transaction<MockPdClient> {
        let snapshot = Snapshot {
            pd: Arc::new(MockPdClient),
            timestamp: Timestamp {
                physical: 0,
                logical: 0,
            },
        };
        Transaction {
            snapshot,
            mutations: Default::default(),
        }
    }
}
