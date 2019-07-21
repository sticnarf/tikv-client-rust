// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(async_await, await_macro)]
#![type_length_limit = "8165158"]

use std::{
    time::{Duration, Instant},
    thread::sleep,
    sync::atomic::{Ordering,AtomicU64}
};
use futures::prelude::*;
use tikv_client::{
    pd::{Pd, PdClient},
    Config, Result,
};

#[runtime::main(runtime_tokio::Tokio)]
async fn main() -> Result<()> {
    let config = Config::new(vec!["127.0.0.1:2381", "127.0.0.1:2379", "127.0.0.1:2383"]);
    let mut pd = Pd::connect(&config).await?;
//    sleep(Duration::from_secs(1));

    let total = AtomicU64::new(0);
    let futures = (0..10000i32).map(|_| {
        async {
            let inst = future::lazy(|_| Instant::now()).await;
            let ts = pd.get_ts().await.ok();
            total.fetch_add(inst.elapsed().as_micros() as u64, Ordering::SeqCst);
        }
    });
    let beg = Instant::now();
    future::join_all(futures).await;
    println!("{:?}", beg.elapsed());
    println!("{:?}", total);

//    let mut total = Duration::default();
//    for _ in 0..100 {
//        let inst = Instant::now();
//        let ts = pd.get_ts().await?;
//        total += inst.elapsed();
//    }
//    println!("{:?}", total / 100);
//        let inst = Instant::now();
//        let ts = pd.get_ts().await?;
//        println!("{:?}", inst.elapsed());
//        println!("{:#?}", ts);
//        let ts = pd.get_ts().await?;
//        println!("{:?}", inst.elapsed());
//        println!("{:#?}", ts);
    // Cleanly exit.
    Ok(())
}
