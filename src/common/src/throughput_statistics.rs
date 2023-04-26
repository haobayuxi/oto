use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rpc::common::{
    throughput_statistics_service_client::ThroughputStatisticsServiceClient,
    throughput_statistics_service_server::ThroughputStatisticsService, Echo, Throughput,
};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tonic::{transport::Channel, Request, Response, Status};

use crate::GLOBAL_COMMITTED;

//

pub struct ThroughputStatisticsServer {}

impl ThroughputStatisticsServer {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl ThroughputStatisticsService for ThroughputStatisticsServer {
    async fn get(&self, request: Request<Echo>) -> Result<Response<Throughput>, Status> {
        Ok(Response::new(Throughput {
            committed: GLOBAL_COMMITTED.load(Ordering::Relaxed) as u64,
        }))
    }
}

//

pub struct ThroughputStatistics {
    committed: u64,
    clients: Vec<ThroughputStatisticsServiceClient<Channel>>,
}

impl ThroughputStatistics {
    pub async fn new(data_ip: Vec<String>) -> Self {
        let mut clients = Vec::new();
        for iter in data_ip {
            loop {
                match ThroughputStatisticsServiceClient::connect(iter.clone()).await {
                    Ok(data_client) => clients.push(data_client),
                    Err(_) => sleep(Duration::from_millis(10)).await,
                }
            }
        }
        Self {
            committed: 0,
            clients,
        }
    }

    pub async fn run(&mut self) {
        let echo = Echo::default();
        loop {
            // sleep
            sleep(Duration::from_millis(10)).await;
            let (sender, mut recv) = unbounded_channel::<Throughput>();

            for i in 0..self.clients.len() {
                let e = echo.clone();
                let mut client = self.clients[i].clone();
                let s_ = sender.clone();
                tokio::spawn(async move {
                    s_.send(client.get(e).await.unwrap().into_inner());
                });
            }

            let mut committed = 0;
            for i in 0..self.clients.len() {
                let reply = recv.recv().await.unwrap();
                committed += reply.committed;
            }
            let throughput_per_second = committed - self.committed;
            self.committed = committed;
            //
        }
    }
}
