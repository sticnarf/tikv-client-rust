use super::Pd;
use crate::{Error, Result};
use futures::{compat::*, prelude::*};
use grpcio::{ClientDuplexReceiver, ClientDuplexSender, WriteFlags};
use kvproto::pdpb::{PdClient as RpcClient, *};

type TsoRequestSender = Compat01As03Sink<ClientDuplexSender<TsoRequest>, (TsoRequest, WriteFlags)>;
type TsoRequestReceiver = Compat01As03<ClientDuplexReceiver<TsoResponse>>;

pub struct Tso {
    cluster_id: u64,
    sender: TsoRequestSender,
    receiver: TsoRequestReceiver,
}

impl Tso {
    pub fn new(cluster_id: u64, rpc_client: &RpcClient) -> Result<Tso> {
        let (tso_sender, tso_receiver) = rpc_client.tso()?;
        Ok(Tso {
            cluster_id,
            sender: tso_sender.sink_compat(),
            receiver: tso_receiver.compat(),
        })
    }

    pub async fn get_ts(&mut self) -> Result<Timestamp> {
        // TODO: add batch get_ts support
        let req = TsoRequest {
            header: Some(RequestHeader {
                cluster_id: self.cluster_id,
            }),
            count: 1,
        };
        let write_flags = WriteFlags::default().buffer_hint(false);

        self.sender.send((req, write_flags)).await?;

        let resp = self
            .receiver
            .next()
            .await
            .ok_or_else(|| Error::internal_error("Tso stream is closed"))??;
        // Batch get_ts is not supported yet
        assert_eq!(resp.count, 1);
        resp.timestamp
            .ok_or_else(|| Error::internal_error("No timestamp received"))
    }
}
