use common::throughput_statistics::{
    run_get_throughput_server, ThroughputStatistics, ThroughputStatisticsServer,
};
use common::{ip_addr_add_prefix, txn::DtxCoordinator, Config, ConfigInFile};
use common::{DbType, GLOBAL_COMMITTED};
use std::env;
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
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let db_type: DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let local_ts = Arc::new(RwLock::new(0));
    let config = Config::default();
    // init throughput statistics rpc server
    if id == 0 {
        // init client
        println!("init throughput client");
        let mut get_throughput_client = ThroughputStatistics::new(config.client_addr.clone()).await;
        tokio::spawn(async move {
            // loop {
            get_throughput_client.run().await;
            // }
        });
    } else {
        let get_throughput_server =
            ThroughputStatisticsServer::new(config.client_addr.get(id as usize).unwrap().clone());
        tokio::spawn(async move {
            run_get_throughput_server(get_throughput_server).await;
        });
    }

    // tokio::spawn(async move {
    //     let mut last_committed = 0;
    //     loop {
    //         sleep(Duration::from_millis(100)).await;
    //         let result: usize = GLOBAL_COMMITTED.load(std::sync::atomic::Ordering::Relaxed);
    //         println!("{}", result - last_committed);
    //         last_committed = result;
    //     }
    // });
    let (result_sender, mut recv) = channel::<(Vec<u128>, f64)>(100);
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
    // println!("throughtput = {}", total_throuthput);
    sleep(Duration::from_secs(2)).await;
    Ok(())
}
