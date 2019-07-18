// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(async_await, await_macro)]
#![type_length_limit = "8165158"]

use tikv_client::{pd::{Pd, PdClient}, Result, Config};
use std::time::{Instant, Duration};

#[runtime::main(runtime_tokio::Tokio)]
async fn main() -> Result<()> {
    let config = Config::new(vec!["127.0.0.1:2381", "127.0.0.1:2379", "127.0.0.1:2383"]);
    let mut pd = Pd::connect(&config).await?;
    let mut total = Duration::default();
    for _ in 0..100000 {
        let inst = Instant::now();
        let ts = pd.get_ts().await?;
        total += inst.elapsed();
    }
    println!("{:?}", total / 100000);
//    let inst = Instant::now();
//    let ts = pd.get_ts().await?;
//    println!("{:?}", inst.elapsed());
//    println!("{:#?}", ts);
//    let ts = pd.get_ts().await?;
//    println!("{:?}", inst.elapsed());
//    println!("{:#?}", ts);
    // Cleanly exit.
    Ok(())
}
