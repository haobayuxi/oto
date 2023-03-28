use common::{ip_addr_add_prefix, Config};
use rpc::common::{
    cto_service_server::{CtoService, CtoServiceServer},
    Echo, Ts,
};
use serde::Deserialize;
use std::{
    env,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tonic::{transport::Server, Response};

pub struct CTO {
    ts: Arc<AtomicU64>,
}

impl CTO {
    pub fn new() -> Self {
        Self {
            ts: Arc::new(AtomicU64::new(1)),
        }
    }
}

#[tonic::async_trait]
impl CtoService for CTO {
    async fn get_start_ts(
        &self,
        request: tonic::Request<Echo>,
    ) -> Result<tonic::Response<Ts>, tonic::Status> {
        let ts = self.ts.load(Ordering::Relaxed);
        Ok(Response::new(Ts { ts }))
    }

    async fn get_commit_ts(
        &self,
        request: tonic::Request<Echo>,
    ) -> Result<tonic::Response<Ts>, tonic::Status> {
        let ts = self.ts.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(Ts { ts }))
    }
}
#[derive(Deserialize)]
struct CtoConfig {
    pub addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open("cto_config.yml").unwrap();
    // let client_config: CtoConfig = serde_yaml::from_reader(f).unwrap();
    let config = Config::default();
    let addr = config.cto_addr.parse().unwrap();
    let cto = CTO::new();

    println!("CTO listening on {}", addr);

    Server::builder()
        .add_service(CtoServiceServer::new(cto))
        .serve(addr)
        .await?;

    Ok(())
}
