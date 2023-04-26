use common::throughput_statistics::ThroughputStatisticsServer;
use common::{ip_addr_add_prefix, txn::DtxCoordinator, Config, ConfigInFile};
use common::{DbType, GLOBAL_COMMITTED};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio::sync::RwLock;
use tokio::time::sleep;
use workload::micro_txn::micro_run_transactions;
use workload::small_bank_txn::small_bank_run_transactions;
use workload::tatp_txn::tatp_run_transactions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open("config.yml").unwrap();
    let id = 0;
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let db_type: DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let local_ts = Arc::new(RwLock::new(0));
    let config = Config::default();
    // init throughput statistics rpc server
    // if id == 0 {
    //     // init client
    // } else {
    //     let statistics_ = ThroughputStatisticsServer::new(committed.clone());
    // }
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            let result = GLOBAL_COMMITTED.load(std::sync::atomic::Ordering::Relaxed);
            println!("{}", result);
        }
    });
    let (result_sender, mut recv) = channel::<(Vec<u128>, f64)>(10000);
    for i in 0..config.client_num {
        let loca_ts_bk = local_ts.clone();
        let cto_addr = config.cto_addr.clone();
        let server_addr = config.server_addr.clone();
        let zipf = server_config.zipf;
        let sender = result_sender.clone();
        // let committed_ = committed.clone();
        tokio::spawn(async move {
            let mut dtx_coordinator = DtxCoordinator::new(
                i,
                loca_ts_bk,
                dtx_type,
                ip_addr_add_prefix(cto_addr),
                server_addr,
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
    let success_num = total_latency.len();
    println!(
        "mean latency = {}",
        total_latency[success_num / 2 as usize],
        // success_num
    );
    println!(
        ".99 latency = {}",
        total_latency[success_num / 100 * 99 as usize]
    );
    // println!("")
    println!("throughtput = {}", total_throuthput);
    sleep(Duration::from_secs(2)).await;
    Ok(())
}
