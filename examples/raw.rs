use futures::future::Future;
use std::path::PathBuf;
use tikv_client::*;

fn main() {
    let config = Config::new(vec!["127.0.0.1:3379"]).with_security(
        PathBuf::from("/path/to/ca.pem"),
        PathBuf::from("/path/to/client.pem"),
        PathBuf::from("/path/to/client-key.pem"),
    );
    let raw = raw::Client::new(&config)
        .wait()
        .expect("Could not connect to tikv");

    let key: Key = b"Company".to_vec().into();
    let value: Value = b"PingCAP".to_vec().into();

    raw.put(key.clone(), value.clone())
        .cf("test_cf")
        .wait()
        .expect("Could not put kv pair to tikv");
    println!("Successfully put {:?}:{:?} to tikv", key, value);

    let value = raw
        .get(key.clone())
        .cf("test_cf")
        .wait()
        .expect("Could not get value");
    println!("Found val: {:?} for key: {:?}", value, key);

    raw.delete(key.clone())
        .cf("test_cf")
        .wait()
        .expect("Could not delete value");
    println!("Key: {:?} deleted", key);

    raw.get(key.clone())
        .cf("test_cf")
        .wait()
        .expect_err("Get returned value for not existing key");

    let keys = vec!["k1", "k2"];

    let pairs = raw
        .batch_get(keys)
        .cf("test_cf")
        .wait()
        .expect("Could not get values");
    println!("Found kv pairs: {:?}", pairs);

    let start = "k1";
    let end = "k2";
    raw.scan(start..end, 10)
        .cf("test_cf")
        .key_only()
        .wait()
        .expect("Could not scan");

    let ranges = vec![start..end, start..end];
    raw.batch_scan(ranges, 10)
        .cf("test_cf")
        .key_only()
        .wait()
        .expect("Could not batch scan");
}
