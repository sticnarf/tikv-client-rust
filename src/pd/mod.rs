use self::tso::Tso;
use crate::{rpc::security::SecurityManager, Config, Error, Result};
use futures::{compat::*, future::BoxFuture, prelude::*};
use grpcio::{CallOption, EnvBuilder, Environment};
use kvproto::pdpb::{PdClient as RpcClient, *};
use parking_lot::{lock_api::RwLock, RawRwLock};
use std::{sync::Arc, time::Duration};

mod tso;

pub trait PdClient {
    fn connect(config: &Config) -> BoxFuture<Result<Self>>
    where
        Self: Sized;

    fn get_ts(&self) -> BoxFuture<Result<Timestamp>>;
}

pub struct Pd {
    cluster_id: u64,
    rpc_client: RpcClient,
    tso: Tso,
    config: PdConfig,
    members: GetMembersResponse,
}

impl PdClient for Pd {
    fn connect(config: &Config) -> BoxFuture<Result<Self>> {
        Pd::connect_impl(config).boxed()
    }

    fn get_ts(&self) -> BoxFuture<Result<Timestamp>> {
        self.get_ts_impl().boxed()
    }
}

impl Pd {
    pub async fn connect_impl(config: &Config) -> Result<Pd> {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.pd_concurrency)
                // .name_prefix(util::new_thread_name(CLIENT_PREFIX))
                .build(),
        );
        let security_mgr = Arc::new(
            if let (Some(ca_path), Some(cert_path), Some(key_path)) =
                (&config.ca_path, &config.cert_path, &config.key_path)
            {
                SecurityManager::load(ca_path, cert_path, key_path)?
            } else {
                SecurityManager::default()
            },
        );
        let pd_config = PdConfig {
            env,
            security_mgr,
            timeout: config.timeout,
        };
        let (rpc_client, members) =
            connect_leader(config.pd_endpoints.iter().map(String::as_str), &pd_config).await?;
        let cluster_id = members
            .header
            .as_ref()
            .ok_or_else(|| Error::internal_error("ResponseHeader is missing"))?
            .cluster_id;
        let tso = Tso::new(cluster_id, &rpc_client).await?;

        Ok(Pd {
            cluster_id,
            rpc_client,
            tso,
            config: pd_config,
            members,
        })
    }

    pub async fn get_ts_impl(&self) -> Result<Timestamp> {
        self.tso.get_ts().await
    }
}

struct PdConfig {
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
}

async fn connect_leader(
    endpoints: impl IntoIterator<Item = &str>,
    config: &PdConfig,
) -> Result<(RpcClient, GetMembersResponse)> {
    let (_, resp) = select_connect_pd(endpoints, &config).await?;

    let header = resp
        .header
        .ok_or_else(|| Error::internal_error("ResponseHeader is missing"))?;
    let leader_info = resp
        .leader
        .ok_or_else(|| Error::internal_error("No PD leader"))?;
    select_connect_pd(leader_info.client_urls.iter().map(String::as_str), &config).await
}

async fn select_connect_pd(
    endpoints: impl IntoIterator<Item = &str>,
    config: &PdConfig,
) -> Result<(RpcClient, GetMembersResponse)> {
    let ((rpc_client, members), _) = future::select_ok(
        endpoints
            .into_iter()
            .map(|addr| connect_pd(addr, config).boxed()),
    )
    .await?;
    Ok((rpc_client, members))
}

async fn connect_pd(addr: &str, config: &PdConfig) -> Result<(RpcClient, GetMembersResponse)> {
    let client = config
        .security_mgr
        .connect(Arc::clone(&config.env), addr, RpcClient::new)?;
    let option = || CallOption::default().timeout(config.timeout);
    let resp = client
        .get_members_async_opt(&GetMembersRequest::default(), option())?
        .compat()
        .await?;
    Ok((client, resp))
}
