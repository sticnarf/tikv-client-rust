use super::Pd;
use crate::{Error, Result};

use std::{
    cell::RefCell,
    convert::TryInto,
    pin::Pin,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
    thread, u32,
};

use crate::compat::ClientFutureExt;
use crossbeam::{
    atomic::AtomicCell,
    queue::{ArrayQueue, PushError, SegQueue},
};
use futures::{
    channel::{mpsc, oneshot},
    compat::*,
    executor::{LocalPool, LocalSpawner},
    poll,
    prelude::*,
    select,
    stream::FuturesUnordered,
    task::{Context, LocalSpawnExt, Poll, Waker},
};
use futures_timer::Interval;
use grpcio::{ClientDuplexReceiver, ClientDuplexSender, WriteFlags};
use kvproto::pdpb::{PdClient as RpcClient, *};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

const MAX_TSO_PENDING_COUNT: usize = 128;

#[derive(Clone)]
pub struct Tso {
    cluster_id: u64,
    result_sender_tx: mpsc::Sender<oneshot::Sender<Timestamp>>,
}

impl Tso {
    pub async fn new(cluster_id: u64, rpc_client: &RpcClient) -> Result<Tso> {
        // FIXME: use tso_opt
        let (rpc_sender, rpc_receiver) = rpc_client.tso()?;
        let (result_sender_tx, result_sender_rx) = mpsc::channel(MAX_TSO_PENDING_COUNT);
        let worker = TsWorker {
            cluster_id,
            result_sender_rx,
            rpc_sender: rpc_sender.sink_compat(),
            rpc_receiver: rpc_receiver.compat(),
        };
        worker.run();
        Ok(Tso {
            cluster_id,
            result_sender_tx,
        })
    }

    pub async fn get_ts(&self) -> Result<Timestamp> {
        let (result_sender, result_receiver) = oneshot::channel();
        let mut result_sender_tx = self.result_sender_tx.clone();
        // FIXME: don't panic
        result_sender_tx
            .send(result_sender)
            .await
            .expect("send error");
        Ok(result_receiver.await?)
    }
}

struct TsWorker {
    cluster_id: u64,
    result_sender_rx: mpsc::Receiver<oneshot::Sender<Timestamp>>,
    rpc_sender: Compat01As03Sink<ClientDuplexSender<TsoRequest>, (TsoRequest, WriteFlags)>,
    rpc_receiver: Compat01As03<ClientDuplexReceiver<TsoResponse>>,
}

impl TsWorker {
    fn run(self) {
        thread::spawn(move || {
            let mut executor = LocalPool::new();
            let mut spawner = executor.spawner();

            let req_stream = RequestStream::new(self.cluster_id, MAX_TSO_PENDING_COUNT);

            let rpc_sender = self.rpc_sender;
            spawner
                .spawn_local(req_stream.clone().forward(rpc_sender).map(|_| ()))
                .expect("spawn error");

            let mut rpc_receiver = self.rpc_receiver.fuse();
            let mut result_sender_rx = self.result_sender_rx.fuse();
            spawner
                .spawn_local(async move {
                    let mut pending = VecDeque::with_capacity(MAX_TSO_PENDING_COUNT);
                    let mut last_count = 0;

                    fn allocate_ts(resp: &TsoResponse, pending: &mut VecDeque<oneshot::Sender<Timestamp>>) {
                        let tail_ts = resp.timestamp.as_ref().expect("No timestamp received");
                        let mut offset = resp.count as i64;
                        while offset > 0 {
                            offset -= 1;
                            if let Some(sender) = pending.pop_front() {
                                let ts = Timestamp {
                                    physical: tail_ts.physical,
                                    logical: tail_ts.logical - offset,
                                };
                                // FIXME: don't panic
                                sender.send(ts).expect("broken channel");
                            } else {
                                break;
                            }
                        }
                    }

                    loop {
                        if pending.len() == MAX_TSO_PENDING_COUNT {
                            let resp = rpc_receiver.next().await.unwrap().unwrap();
                            allocate_ts(&resp, &mut pending);
                            last_count = pending.len();
                        } else {
                            select! {
                                resp = rpc_receiver.next() => {
                                    let resp = resp.unwrap().unwrap();
                                    allocate_ts(&resp, &mut pending);
                                    last_count = pending.len();
                                },
                                result_sender = result_sender_rx.next() => {
                                    // FIXME: don't panic
                                    let result_sender = result_sender.unwrap();
                                    pending.push_back(result_sender);
                                }
                            }
                        }

                        while pending.len() < MAX_TSO_PENDING_COUNT {
                            if let Poll::Ready(Some(sender)) = poll!(result_sender_rx.next()) {
                                pending.push_back(sender);
                            } else {
                                break;
                            }
                        }

                        let count = pending.len() - last_count;
                        if count > 0 {
                            println!("count = {}", count);
                            req_stream.push(count as u32);
                            last_count = pending.len();
                        }
                    }
                })
                .expect("spawn error");

            executor.run();
        });
    }
}

#[derive(Clone)]
struct RequestStream {
    inner: Rc<RefCell<RequestStreamInner>>
}

impl RequestStream {
    fn new(cluster_id: u64, capacity: usize) -> RequestStream {
        let inner = RequestStreamInner {
            cluster_id,
            count_queue: VecDeque::with_capacity(capacity),
            waker: None
        };
        RequestStream {
            inner: Rc::new(RefCell::new(inner))
        }
    }

    fn push(&self, count: u32) {
        let mut inner = self.inner.borrow_mut();
        inner.count_queue.push_back(count);
        if let Some(waker) = &inner.waker {
            waker.wake_by_ref();
        }
    }
}

impl Stream for RequestStream {
    type Item = grpcio::Result<(TsoRequest, WriteFlags)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.borrow_mut();

        if inner.waker.is_none() {
            inner.waker = Some(cx.waker().clone());
        }

        match inner.count_queue.pop_front() {
            Some(count) => {
                let req = TsoRequest {
                    header: Some(RequestHeader {
                        cluster_id: inner.cluster_id,
                    }),
                    count,
                };
                let write_flags = WriteFlags::default().buffer_hint(false);
                Poll::Ready(Some(Ok((req, write_flags))))
            }
            None => Poll::Pending,
        }
    }
}

struct RequestStreamInner {
    cluster_id: u64,
    count_queue: VecDeque<u32>,
    waker: Option<Waker>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_size() {
        println!("{}", std::mem::size_of::<TsBuffer>());
        println!("{}", std::mem::size_of::<Option<Result<Timestamp>>>());
        println!("{}", std::mem::size_of::<ClientDuplexSender<TsoRequest>>());
        println!(
            "{}",
            std::mem::size_of::<futures::channel::oneshot::Sender<Result<Timestamp>>>()
        );
    }
}
