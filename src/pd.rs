use crate::util::{channel, SecurityConfig};
use crate::Error;
use futures::try_ready;
use futures::{prelude::*, stream};
use grpcio::{CallOption, Environment};
use kvproto::{pdpb::*, pdpb_grpc::PdClient as GrpcPdClient};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const CQ_COUNT: usize = 1;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

pub struct PdClient {
    grpc_client: GrpcPdClient,
    members: GetMembersResponse,
}

impl fmt::Debug for PdClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PdClient {{ members: {:?} }}", self.members)
    }
}

impl PdClient {
    pub fn connect(endpoints: Vec<String>, security: Option<Arc<SecurityConfig>>) -> PdConnect {
        let env = Arc::new(Environment::new(CQ_COUNT));
        // Connect to endpoints and construct PdClient in sequence
        let pd_stream = stream::iter_ok(endpoints)
            .and_then(move |addr| {
                channel(&addr, security.clone(), Arc::clone(&env))
                    .map(GrpcPdClient::new)
                    .into_future()
                    .and_then(Self::from_grpc_client)
            })
            .inspect_err(|e| {
                // TODO: Log error
            });

        PdConnect {
            inner: Box::new(pd_stream),
        }
    }

    fn from_grpc_client(grpc_client: GrpcPdClient) -> impl Future<Item = PdClient, Error = Error> {
        let option = CallOption::default().timeout(REQUEST_TIMEOUT);
        let members_fut = grpc_client.get_members_async_opt(&GetMembersRequest::new(), option);
        members_fut
            .into_future()
            .flatten()
            .map(move |members| PdClient::new(grpc_client, members))
            .map_err(Into::into)
    }

    fn new(grpc_client: GrpcPdClient, members: GetMembersResponse) -> PdClient {
        PdClient {
            grpc_client,
            members,
        }
    }
}

// PdConnect is a future that returns the first successfully constructed PdClient
pub struct PdConnect {
    inner: Box<dyn Stream<Item = PdClient, Error = Error>>,
}

impl Future for PdConnect {
    type Item = PdClient;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.inner.poll()) {
            Some(pd) => Ok(Async::Ready(pd)),
            None => Err(Error::PdUnavailable),
        }
    }
}
