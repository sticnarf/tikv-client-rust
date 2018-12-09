use crate::Result;
use grpcio::{Channel, ChannelBuilder, ChannelCredentialsBuilder, Environment};
use serde_derive::*;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityConfig {
    pub ca_path: PathBuf,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

fn load_key(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let mut f = File::open(path)?;
    let mut v = Vec::new();
    let _ = f.read_to_end(&mut v)?;
    Ok(v)
}

pub fn channel(
    addr: &str,
    security: Option<Arc<SecurityConfig>>,
    env: Arc<Environment>,
) -> Result<Channel> {
    // TODO: Configure more properties
    let cb = ChannelBuilder::new(env);
    let channel = match security.as_ref().map(AsRef::as_ref) {
        Some(SecurityConfig {
            ca_path,
            cert_path,
            key_path,
        }) => {
            let cred = ChannelCredentialsBuilder::new()
                .root_cert(load_key(ca_path)?)
                .cert(load_key(cert_path)?, load_key(key_path)?)
                .build();
            cb.secure_connect(addr, cred)
        }
        None => cb.connect(addr),
    };
    Ok(channel)
}
