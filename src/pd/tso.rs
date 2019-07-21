use super::Pd;
use crate::{Error, Result};

use std::{
    convert::TryInto,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    thread, u32,
};

use crate::compat::ClientFutureExt;
use crossbeam::{
    atomic::AtomicCell,
    channel,
    queue::{ArrayQueue, PushError, SegQueue},
};
use futures::{
    channel::oneshot,
    compat::*,
    executor::{LocalPool, LocalSpawner},
    prelude::*,
    task::{Context, LocalSpawnExt, Poll, Waker},
};
use futures_timer::Interval;
use grpcio::{ClientDuplexReceiver, ClientDuplexSender, WriteFlags};
use kvproto::pdpb::{PdClient as RpcClient, *};
use parking_lot::Mutex;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub struct Tso {
    cluster_id: u64,
    ts_buffer_handle: TsBufferHandle,
    worker_waker: Waker,
}

impl Tso {
    pub fn new(cluster_id: u64, rpc_client: &RpcClient) -> impl Future<Output = Result<Tso>> {
        let (sender_tx, sender_rx) = channel::unbounded();
        let ts_buffer_handle = TsBufferHandle {
            sender_tx,
            enqueued: Arc::new(AtomicU64::new(0)),
            dequeued: Arc::new(AtomicU64::new(0)),
        };

        // FIXME: use tso_opt
        future::ready(rpc_client.tso())
            .err_into()
            .and_then(move |(sender, receiver)| {
                let worker = TsWorker {
                    cluster_id,
                    ts_buffer_handle: ts_buffer_handle.clone(),
                    sender_rx,
                    rpc_sender: sender.sink_compat(),
                    rpc_receiver: receiver.compat(),
                };
                worker.run().map_ok(move |worker_waker| Tso {
                    cluster_id,
                    ts_buffer_handle,
                    worker_waker,
                })
            })
    }

    pub fn get_ts(&self) -> impl Future<Output = Result<Timestamp>> {
        let (sender, receiver) = oneshot::channel();
        future::ready(
            self.ts_buffer_handle
                .enqueue(sender)
                .map(|_| self.worker_waker.wake_by_ref()),
        )
        .and_then(|_| receiver.err_into())
    }
}

struct TsWorker {
    cluster_id: u64,
    ts_buffer_handle: TsBufferHandle,
    sender_rx: channel::Receiver<oneshot::Sender<Timestamp>>,
    rpc_sender: Compat01As03Sink<ClientDuplexSender<TsoRequest>, (TsoRequest, WriteFlags)>,
    rpc_receiver: Compat01As03<ClientDuplexReceiver<TsoResponse>>,
}

impl TsWorker {
    fn run(self) -> impl Future<Output = Result<Waker>> {
        let (waker_tx, waker_rx) = oneshot::channel();
        thread::spawn(move || {
            let mut executor = LocalPool::new();
            let mut spawner = executor.spawner();

            let dequeued = Arc::clone(&self.ts_buffer_handle.dequeued);

            let tso_request_stream = TsoRequestStream {
                cluster_id: self.cluster_id,
                waker_tx: Some(waker_tx),
                ts_buffer_handle: self.ts_buffer_handle,
            };

            spawner
                .spawn_local(
                    tso_request_stream
                        .forward(self.rpc_sender)
                        .map(|e| eprintln!("{:?}", e)),
                )
                .expect("spawn error");

            let sender_rx = self.sender_rx;
            spawner
                .spawn_local(self.rpc_receiver.for_each(move |resp| {
                    if let Ok(resp) = resp {
                        // FIXME: should not panic
                        let tail_ts = resp.timestamp.expect("No timestamp received");
                        let mut offset = resp.count as i64;
                        while offset > 0 {
                            match sender_rx.try_recv() {
                                Ok(sender) => {
                                    dequeued.fetch_sub(1, Ordering::SeqCst);
                                    offset -= 1;
                                    let ts = Timestamp {
                                        physical: tail_ts.physical,
                                        logical: tail_ts.logical - offset,
                                    };
                                    sender.send(ts).expect("broken channel");
                                }
                                _ => break,
                            }
                        }
                    }
                    future::ready(())
                }))
                .expect("spawn error");

            //            spawner
            //                .spawn_local(
            //                    inner_waker_rx
            //                        // FIXME: No unwrap
            //                        .map(|worker_waker| worker_waker.unwrap())
            //                        .never_error()
            //                        .and_then(move |worker_waker| {
            //                            waker_tx.send(worker_waker.clone()).ok();
            //                            Interval::new(Duration::from_millis(1))
            //                                .map(move |_| {
            //                                    worker_waker.wake_by_ref();
            //                                    Ok::<(), Infallible>(())
            //                                })
            //                                .forward(sink::drain())
            //                        })
            //                        .map(|_| ()),
            //                )
            //                .expect("spawn error");
            executor.run();
        });
        waker_rx.err_into()
    }
}

#[derive(Clone)]
struct TsBufferHandle {
    sender_tx: channel::Sender<oneshot::Sender<Timestamp>>,
    enqueued: Arc<AtomicU64>,
    dequeued: Arc<AtomicU64>,
}

impl TsBufferHandle {
    fn enqueue(&self, sender: oneshot::Sender<Timestamp>) -> Result<()> {
        self.sender_tx.send(sender).expect("send sender error");
        self.enqueued.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn send_prepare(&self) -> u32 {
        let dequeued = self.dequeued.load(Ordering::SeqCst);
        let enqueued = self.enqueued.load(Ordering::SeqCst);
        (enqueued - dequeued).try_into().unwrap_or(u32::MAX)
    }
}

//impl TsBuffer {
//    fn enqueue(&self, handle: TsFutureHandle) -> bool {
//        //        self.queued.push(handle);
//        //        true
//        //                if let Ok(_) = self.queued.push(handle) {
//        self.queued.push(handle);
//        self.queue_head_instant
//            .compare_and_swap(None, Some(Instant::now()));
//        true
//        //                } else {
//        //                    false
//        //                }
//    }
//
//    fn prepare(&self) -> Option<u32> {
//        let waiting_time = self.queue_head_instant.load()?.elapsed();
//        if waiting_time < Duration::from_millis(1) {
//            println!("too short: {:?}", waiting_time);
//            None
//        } else {
//            self.queue_head_instant.store(None);
//            let mut count = 0;
//            while let Ok(handle) = self.queued.pop() {
//                self.waiting.push(handle);
//                count += 1;
//            }
//            Some(count)
//        }
//    }
//
//    fn allocate(&self, tail_ts: Timestamp, count: u32) {
//        let mut offset = count as i64;
//        while offset > 0 {
//            offset -= 1;
//            let handle = self.waiting.pop().expect("no enough handle");
//            let ts = Timestamp {
//                physical: tail_ts.physical,
//                logical: tail_ts.logical - offset,
//            };
//            handle.result.store(Some(Ok(ts)));
//            handle.waker.wake_by_ref();
//        }
//    }
//}

struct TsoRequestStream {
    cluster_id: u64,
    waker_tx: Option<oneshot::Sender<Waker>>,
    ts_buffer_handle: TsBufferHandle,
}

impl Stream for TsoRequestStream {
    type Item = grpcio::Result<(TsoRequest, WriteFlags)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(waker_tx) = self.waker_tx.take() {
            // FIXME: don't panic
            waker_tx.send(cx.waker().clone()).expect("send waker error");
        }

        let count = self.ts_buffer_handle.send_prepare();
        if count == 0 {
            Poll::Pending
        } else {
            println!("{}", count);
            let req = TsoRequest {
                header: Some(RequestHeader {
                    cluster_id: self.cluster_id,
                }),
                count,
            };
            let write_flags = WriteFlags::default().buffer_hint(false);
            Poll::Ready(Some(Ok((req, write_flags))))
        }
    }
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
