use common::{ip_addr_add_prefix, txn::DtxCoordinator, Config, ConfigInFile};
use common::{DbType, TXNS_PER_CLIENT};
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio::sync::RwLock;
use workload::micro_txn::micro_run_transactions;
use workload::small_bank_txn::small_bank_run_transactions;
use workload::tatp_txn::tatp_run_transactions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let db_type: DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let local_ts = Arc::new(RwLock::new(0));
    let config = Config::default();
    let (result_sender, mut recv) = channel::<(Vec<u128>, f64)>(10000);
    for i in 0..config.client_num {
        let loca_ts_bk = local_ts.clone();
        let cto_addr = config.cto_addr.clone();
        let server_addr = config.server_addr.clone();
        let zipf = server_config.zipf;
        let sender = result_sender.clone();
        tokio::spawn(async move {
            let mut dtx_coordinator = DtxCoordinator::new(
                i,
                loca_ts_bk,
                dtx_type,
                ip_addr_add_prefix(cto_addr),
                ip_addr_add_prefix(server_addr),
            )
            .await;
            match db_type {
                DbType::micro => {
                    sender
                        .send(micro_run_transactions(&mut dtx_coordinator, zipf).await)
                        .await;
                }
                DbType::tatp => {
                    sender
                        .send(tatp_run_transactions(&mut dtx_coordinator).await)
                        .await;
                }
                DbType::smallbank => {
                    sender
                        .send(small_bank_run_transactions(&mut dtx_coordinator).await)
                        .await;
                }
            }
        });
    }
    let mut total_latency: Vec<u128> = Vec::new();
    let mut total_throuthput = 0.0;
    for _ in 0..config.client_num {
        let (latency_result, throughput) = recv.recv().await.unwrap();
        total_latency.extend(latency_result.iter());
        total_throuthput += throughput;
    }
    total_latency.sort();
    println!(
        "mean latency = {}",
        total_latency[(config.client_num * TXNS_PER_CLIENT / 2) as usize]
    );
    // println!("")
    println!("throughtput = {}", total_throuthput);
    Ok(())
}
