// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(async_await, await_macro)]
#![type_length_limit = "8165158"]

use futures::prelude::*;
use futures_timer::Interval;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    thread::sleep,
    time::{Duration, Instant},
};
use tikv_client::{
    pd::{Pd, PdClient},
    Config, Result,
};

#[runtime::main(runtime_tokio::Tokio)]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::new(vec!["127.0.0.1:2381", "127.0.0.1:2379", "127.0.0.1:2383"]);
    let pd = Pd::connect(&config).await?;
    //    sleep(Duration::from_secs(1));

    let mut interval = Interval::new(Duration::from_millis(1000));

    loop {
        interval.next().await;
        match pd.get_ts().await {
            Ok(ts) => println!("{:?}", ts),
            Err(e) => println!("{:?}", e),
        }
    }

    //    let total = AtomicU64::new(0);
    //    let futures = (0..1000i32).map(|_| {
    //        async {
    //            let inst = future::lazy(|_| Instant::now()).await;
    //            let ts = pd.get_ts().await.ok();
    //            total.fetch_add(inst.elapsed().as_micros() as u64, Ordering::SeqCst);
    //        }
    //    });
    //    let beg = Instant::now();
    //    future::join_all(futures).await;
    //    println!("{:?}", beg.elapsed());
    //    println!("{:?}", total);

    //            let inst = Instant::now();
    //            let ts = pd.get_ts().await?;
    //            println!("{:?}", inst.elapsed());
    //            println!("{:#?}", ts);
    //            let inst = Instant::now();
    //            let ts = pd.get_ts().await?;
    //            println!("{:?}", inst.elapsed());
    //            println!("{:#?}", ts);
    // Cleanly exit.
    Ok(())
}
