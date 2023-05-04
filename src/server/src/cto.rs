use common::{ip_addr_add_prefix, Config};
use rpc::common::{
    cto_service_server::{CtoService, CtoServiceServer},
    Echo, Ts,
};
use serde::Deserialize;
use std::{
    env,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering, ATOMIC_USIZE_INIT},
        Arc,
    },
};
use tonic::{transport::Server, Response};

static TS: AtomicUsize = ATOMIC_USIZE_INIT;
pub struct CTO {}

impl CTO {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl CtoService for CTO {
    async fn get_start_ts(
        &self,
        request: tonic::Request<Echo>,
    ) -> Result<tonic::Response<Ts>, tonic::Status> {
        unsafe {
            let ts = TS.load(Ordering::Relaxed) as u64;
            Ok(Response::new(Ts { ts }))
        }
    }

    async fn get_commit_ts(
        &self,
        request: tonic::Request<Echo>,
    ) -> Result<tonic::Response<Ts>, tonic::Status> {
        unsafe {
            let ts = TS.fetch_add(1, Ordering::Relaxed) as u64;
            Ok(Response::new(Ts { ts }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let client_config: CtoConfig = serde_yaml::from_reader(f).unwrap();
    let config = Config::default();
    let addr1 = config.cto_addr[0].parse().unwrap();
    let addr2 = config.cto_addr[1].parse().unwrap();
    tokio::spawn(async move {
        let cto = CTO::new();

        println!("CTO listening on {}", addr1);

        Server::builder()
            .add_service(CtoServiceServer::new(cto))
            .serve(addr1)
            .await;
    });
    let cto = CTO::new();

    println!("CTO listening on {}", addr2);

    Server::builder()
        .add_service(CtoServiceServer::new(cto))
        .serve(addr2)
        .await?;

    Ok(())
}
