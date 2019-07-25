use async_trait::async_trait;
use bytes::Bytes;
use kvproto::metapb;

use crate::Result;

#[async_trait]
pub trait Pd {
    /// Returns a timestamp allocated from PD.
    ///
    /// The `u64` timestamp is calculated from `pdpb::Timestamp` using the formula:
    /// ts = physical << 18 + logical
    async fn get_ts(&self) -> Result<u64>;

    /// Returns information of the region containing the key.
    async fn locate_region(&self, key: Bytes) -> Result<Region>;

    /// Returns the address of the store with the given id.
    async fn get_store_address(&self, store_id: u64) -> Result<String>;
}

pub struct Region {
    pub id: u64,
    pub start_key: Bytes,
    pub end_key: Bytes,
    pub region_epoch: metapb::RegionEpoch,
    pub leader: metapb::Peer,
}
