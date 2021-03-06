// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    backoff::Backoff,
    pd::PdClient,
    request::{store_stream_for_key, store_stream_for_keys, store_stream_for_range, KvRequest},
    store::Store,
    timestamp::TimestampExt,
    transaction::HasLocks,
    BoundRange, Error, Key, KvPair, Result, Value,
};
use async_trait::async_trait;
use futures::{prelude::*, stream::BoxStream};
use std::{iter, mem, sync::Arc};
use tikv_client_proto::{kvrpcpb, pdpb::Timestamp};

#[async_trait]
impl KvRequest for kvrpcpb::GetRequest {
    type Result = Option<Value>;
    type RpcResponse = kvrpcpb::GetResponse;
    type KeyData = Key;
    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        let key = mem::take(&mut self.key).into();
        store_stream_for_key(key, pd_client)
    }

    fn make_rpc_request(&self, key: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_key(key.into());
        req.set_version(self.version);

        Ok(req)
    }

    fn map_result(mut resp: Self::RpcResponse) -> Self::Result {
        if resp.not_found {
            None
        } else {
            Some(resp.take_value())
        }
    }

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .into_future()
            .map(|(f, _)| f.expect("no results should be impossible"))
            .await
    }
}

impl HasLocks for kvrpcpb::GetResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.error
            .as_mut()
            .and_then(|error| error.locked.take())
            .into_iter()
            .collect()
    }
}

pub fn new_mvcc_get_request(key: impl Into<Key>, timestamp: Timestamp) -> kvrpcpb::GetRequest {
    let mut req = kvrpcpb::GetRequest::default();
    req.set_key(key.into().into());
    req.set_version(timestamp.version());
    req
}

#[async_trait]
impl KvRequest for kvrpcpb::BatchGetRequest {
    type Result = Vec<KvPair>;
    type RpcResponse = kvrpcpb::BatchGetResponse;
    type KeyData = Vec<Key>;
    fn make_rpc_request(&self, keys: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_keys(keys.into_iter().map(Into::into).collect());
        req.set_version(self.version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.keys.sort();
        let keys = mem::take(&mut self.keys);
        store_stream_for_keys(keys, pd_client)
    }

    fn map_result(mut resp: Self::RpcResponse) -> Self::Result {
        resp.take_pairs().into_iter().map(Into::into).collect()
    }

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results.try_concat().await
    }
}

impl HasLocks for kvrpcpb::BatchGetResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.pairs
            .iter_mut()
            .filter_map(|pair| pair.error.as_mut().and_then(|error| error.locked.take()))
            .collect()
    }
}

pub fn new_mvcc_get_batch_request(
    keys: Vec<Key>,
    timestamp: Timestamp,
) -> kvrpcpb::BatchGetRequest {
    let mut req = kvrpcpb::BatchGetRequest::default();
    req.set_keys(keys.into_iter().map(Into::into).collect());
    req.set_version(timestamp.version());
    req
}

#[async_trait]
impl KvRequest for kvrpcpb::ScanRequest {
    type Result = Vec<KvPair>;
    type RpcResponse = kvrpcpb::ScanResponse;
    type KeyData = (Key, Key);
    fn make_rpc_request(&self, (start_key, end_key): Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_start_key(start_key.into());
        req.set_end_key(end_key.into());
        req.set_limit(self.limit);
        req.set_key_only(self.key_only);
        req.set_version(self.version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        let start_key = mem::take(&mut self.start_key);
        let end_key = mem::take(&mut self.end_key);
        let range = BoundRange::from((start_key, end_key));
        store_stream_for_range(range, pd_client)
    }

    fn map_result(mut resp: Self::RpcResponse) -> Self::Result {
        resp.take_pairs().into_iter().map(Into::into).collect()
    }

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results.try_concat().await
    }
}

pub fn new_mvcc_scan_request(
    range: impl Into<BoundRange>,
    timestamp: Timestamp,
    limit: u32,
    key_only: bool,
) -> kvrpcpb::ScanRequest {
    let (start_key, end_key) = range.into().into_keys();
    let mut req = kvrpcpb::ScanRequest::default();
    req.set_start_key(start_key.into());
    req.set_end_key(end_key.unwrap_or_default().into());
    req.set_limit(limit);
    req.set_key_only(key_only);
    req.set_version(timestamp.version());
    req
}

impl HasLocks for kvrpcpb::ScanResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.pairs
            .iter_mut()
            .filter_map(|pair| pair.error.as_mut().and_then(|error| error.locked.take()))
            .collect()
    }
}

// For a ResolveLockRequest, it does not contain enough information about which region it should be
// sent to. Therefore, the context must be specified every time the request is created and we don't
// retry the request automatically on region errors.
#[async_trait]
impl KvRequest for kvrpcpb::ResolveLockRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::ResolveLockResponse;
    type KeyData = (kvrpcpb::Context, Vec<Vec<u8>>);
    fn make_rpc_request(&self, (context, keys): Self::KeyData, _store: &Store) -> Result<Self> {
        let mut req = Self::default();
        req.set_context(context);
        req.set_start_version(self.start_version);
        req.set_commit_version(self.commit_version);
        req.set_txn_infos(self.txn_infos.clone());
        req.set_keys(keys);

        Ok(req)
    }

    fn on_region_error(
        self,
        region_error: Error,
        _pd_client: Arc<impl PdClient>,
        _region_backoff: impl Backoff,
        _lock_backoff: impl Backoff,
    ) -> BoxStream<'static, Result<Self::RpcResponse>> {
        stream::once(future::err(region_error)).boxed()
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        let context = self
            .context
            .take()
            .expect("ResolveLockRequest context must be given ");
        let keys = mem::take(&mut self.keys);
        pd_client
            .store_for_id(context.region_id)
            .map_ok(move |store| ((context, keys), store))
            .into_stream()
            .boxed()
    }

    fn map_result(_: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results.try_collect().await
    }
}

pub fn new_resolve_lock_request(
    context: kvrpcpb::Context,
    start_version: u64,
    commit_version: u64,
) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.set_context(context);
    req.set_start_version(start_version);
    req.set_commit_version(commit_version);

    req
}

// TODO: Add lite resolve lock (resolve specified locks only)

#[async_trait]
impl KvRequest for kvrpcpb::CleanupRequest {
    /// Commit version if the key is committed, 0 otherwise.
    type Result = u64;
    type RpcResponse = kvrpcpb::CleanupResponse;
    type KeyData = Key;
    fn make_rpc_request(&self, key: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_key(key.into());
        req.set_start_version(self.start_version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        let key = mem::take(&mut self.key).into();
        store_stream_for_key(key, pd_client)
    }

    fn map_result(resp: Self::RpcResponse) -> Self::Result {
        resp.commit_version
    }

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .into_future()
            .map(|(f, _)| f.expect("no results should be impossible"))
            .await
    }
}

pub fn new_cleanup_request(key: impl Into<Key>, start_version: u64) -> kvrpcpb::CleanupRequest {
    let mut req = kvrpcpb::CleanupRequest::default();
    req.set_key(key.into().into());
    req.set_start_version(start_version);

    req
}

#[async_trait]
impl KvRequest for kvrpcpb::PrewriteRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::PrewriteResponse;
    type KeyData = Vec<kvrpcpb::Mutation>;
    fn make_rpc_request(&self, mutations: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_mutations(mutations);
        req.set_primary_lock(self.primary_lock.clone());
        req.set_start_version(self.start_version);
        req.set_lock_ttl(self.lock_ttl);
        req.set_skip_constraint_check(self.skip_constraint_check);
        req.set_txn_size(self.txn_size);
        req.set_for_update_ts(self.for_update_ts);
        req.set_is_pessimistic_lock(self.is_pessimistic_lock.clone());

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.mutations.sort_by(|a, b| a.key.cmp(&b.key));
        let mutations = mem::take(&mut self.mutations);
        store_stream_for_keys(mutations, pd_client)
    }

    fn map_result(_: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .try_for_each_concurrent(None, |_| future::ready(Ok(())))
            .await
    }
}

impl HasLocks for kvrpcpb::PrewriteResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: u64,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    let mut req = kvrpcpb::PrewriteRequest::default();
    req.set_mutations(mutations);
    req.set_primary_lock(primary_lock.into());
    req.set_start_version(start_version);
    req.set_lock_ttl(lock_ttl);
    // TODO: Lite resolve lock is currently disabled
    req.set_txn_size(std::u64::MAX);

    req
}

pub fn new_pessimistic_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
) -> kvrpcpb::PrewriteRequest {
    let len = mutations.len();
    let mut req = new_prewrite_request(mutations, primary_lock, start_version, lock_ttl);
    req.set_for_update_ts(for_update_ts);
    req.set_is_pessimistic_lock(iter::repeat(true).take(len).collect());
    req
}

#[async_trait]
impl KvRequest for kvrpcpb::CommitRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::CommitResponse;
    type KeyData = Vec<Vec<u8>>;
    fn make_rpc_request(&self, keys: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_keys(keys);
        req.set_start_version(self.start_version);
        req.set_commit_version(self.commit_version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.keys.sort();
        let keys = mem::take(&mut self.keys);
        store_stream_for_keys(keys, pd_client)
    }

    fn map_result(_: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .try_for_each_concurrent(None, |_| future::ready(Ok(())))
            .await
    }
}

pub fn new_commit_request(
    keys: Vec<Key>,
    start_version: u64,
    commit_version: u64,
) -> kvrpcpb::CommitRequest {
    let mut req = kvrpcpb::CommitRequest::default();
    req.set_keys(keys.into_iter().map(Into::into).collect());
    req.set_start_version(start_version);
    req.set_commit_version(commit_version);

    req
}

#[async_trait]
impl KvRequest for kvrpcpb::BatchRollbackRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::BatchRollbackResponse;
    type KeyData = Vec<Vec<u8>>;
    fn make_rpc_request(&self, keys: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_keys(keys);
        req.set_start_version(self.start_version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.keys.sort();
        let keys = mem::take(&mut self.keys);
        store_stream_for_keys(keys, pd_client)
    }

    fn map_result(_: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .try_for_each_concurrent(None, |_| future::ready(Ok(())))
            .await
    }
}

pub fn new_batch_rollback_request(
    keys: Vec<Key>,
    start_version: u64,
) -> kvrpcpb::BatchRollbackRequest {
    let mut req = kvrpcpb::BatchRollbackRequest::default();
    req.set_keys(keys.into_iter().map(Into::into).collect());
    req.set_start_version(start_version);

    req
}

#[async_trait]
impl KvRequest for kvrpcpb::PessimisticRollbackRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::PessimisticRollbackResponse;
    type KeyData = Vec<Vec<u8>>;

    fn make_rpc_request(&self, keys: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_keys(keys);
        req.set_start_version(self.start_version);

        Ok(req)
    }

    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.keys.sort();
        let keys = mem::take(&mut self.keys);
        store_stream_for_keys(keys, pd_client)
    }

    fn map_result(_: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .try_for_each_concurrent(None, |_| future::ready(Ok(())))
            .await
    }
}

pub fn new_pessimistic_rollback_request(
    keys: Vec<Key>,
    start_version: u64,
    for_update_ts: u64,
) -> kvrpcpb::PessimisticRollbackRequest {
    let mut req = kvrpcpb::PessimisticRollbackRequest::default();
    req.set_keys(keys.into_iter().map(Into::into).collect());
    req.set_start_version(start_version);
    req.set_for_update_ts(for_update_ts);

    req
}

#[async_trait]
impl KvRequest for kvrpcpb::PessimisticLockRequest {
    type Result = ();
    type RpcResponse = kvrpcpb::PessimisticLockResponse;
    type KeyData = Vec<kvrpcpb::Mutation>;
    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        self.mutations.sort_by(|a, b| a.key.cmp(&b.key));
        let mutations = mem::take(&mut self.mutations);
        store_stream_for_keys(mutations, pd_client)
    }

    fn make_rpc_request(&self, mutations: Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_mutations(mutations);
        req.set_primary_lock(self.primary_lock.clone());
        req.set_start_version(self.start_version);
        req.set_lock_ttl(self.lock_ttl);
        req.set_for_update_ts(self.for_update_ts);
        req.set_is_first_lock(self.is_first_lock);
        req.set_wait_timeout(self.wait_timeout);
        req.set_force(self.force);
        req.set_return_values(self.return_values);
        req.set_min_commit_ts(self.min_commit_ts);

        Ok(req)
    }

    fn map_result(_result: Self::RpcResponse) -> Self::Result {}

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results
            .try_for_each_concurrent(None, |_| future::ready(Ok(())))
            .await
    }
}

pub fn new_pessimistic_lock_request(
    keys: Vec<Vec<u8>>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
) -> kvrpcpb::PessimisticLockRequest {
    let mut req = kvrpcpb::PessimisticLockRequest::default();
    let mutations = keys
        .into_iter()
        .map(|key| {
            let mut mutation = kvrpcpb::Mutation::default();
            mutation.set_op(kvrpcpb::Op::PessimisticLock);
            mutation.set_key(key);
            mutation
        })
        .collect();
    req.set_mutations(mutations);
    req.set_primary_lock(primary_lock);
    req.set_start_version(start_version);
    req.set_lock_ttl(lock_ttl);
    req.set_for_update_ts(for_update_ts);
    // todo: make them configurable
    req.set_is_first_lock(false);
    req.set_wait_timeout(0);
    req.set_force(false);
    req.set_return_values(false);
    // todo: support large transaction
    req.set_min_commit_ts(0);

    req
}

#[async_trait]
impl KvRequest for kvrpcpb::ScanLockRequest {
    type Result = Vec<kvrpcpb::LockInfo>;
    type RpcResponse = kvrpcpb::ScanLockResponse;
    type KeyData = (Key, Key); // end_key should always be empty. Used to satisfy `store_stream_for_range`
    fn store_stream<PdC: PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
    ) -> BoxStream<'static, Result<(Self::KeyData, Store)>> {
        let start_key = mem::take(&mut self.start_key);
        let range = BoundRange::from((start_key, vec![]));
        store_stream_for_range(range, pd_client)
    }

    fn make_rpc_request(&self, (start_key, _): Self::KeyData, store: &Store) -> Result<Self> {
        let mut req = self.request_from_store(store)?;
        req.set_max_version(self.max_version);
        req.set_start_key(start_key.into());
        req.set_limit(self.limit);
        Ok(req)
    }

    fn map_result(mut result: Self::RpcResponse) -> Self::Result {
        result.take_locks()
    }

    async fn reduce(results: BoxStream<'static, Result<Self::Result>>) -> Result<Self::Result> {
        results.try_concat().await
    }
}

pub fn new_scan_lock_request(
    start_key: impl Into<Key>,
    safepoint: Timestamp,
    limit: u32,
) -> kvrpcpb::ScanLockRequest {
    let mut req = kvrpcpb::ScanLockRequest::default();
    req.set_start_key(start_key.into().into());
    req.set_max_version(safepoint.version());
    req.set_limit(limit);
    req
}

impl HasLocks for kvrpcpb::CommitResponse {}

impl HasLocks for kvrpcpb::CleanupResponse {}

impl HasLocks for kvrpcpb::BatchRollbackResponse {}

impl HasLocks for kvrpcpb::PessimisticRollbackResponse {}

impl HasLocks for kvrpcpb::ResolveLockResponse {}

impl HasLocks for kvrpcpb::ScanLockResponse {}

impl HasLocks for kvrpcpb::PessimisticLockResponse {}
