use common::{txn::DtxCoordinator, TXNS_PER_CLIENT};
use rpc::common::{ReadStruct, WriteStruct};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, time::Instant};

use crate::micro_db::MicroQuery;

pub async fn micro_run_transactions(coordinator: &mut DtxCoordinator, theta: f64) {
    // init workload
    let mut query = MicroQuery::new(theta, 4, 10);
    // run transaction
    let mut latency_result = Vec::new();
    let total_start = Instant::now();
    for i in 0..TXNS_PER_CLIENT {
        let start = Instant::now();
        let (read_set, write_set) = query.generate();
        run_transacntion(coordinator, read_set, write_set).await;
        let end_time = start.elapsed().as_micros();
        latency_result.push(end_time);
    }
    let total_end = (total_start.elapsed().as_millis() as f64) / 1000.0;
    let throughput_result = TXNS_PER_CLIENT as f64 / total_end;
    println!("throughput = {}", throughput_result);
    // write results to file
    let latency_file_name = coordinator.id.to_string() + "latency.data";
    let mut latency_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(latency_file_name)
        .await
        .unwrap();
    for iter in latency_result {
        latency_file.write(iter.to_string().as_bytes()).await;
        latency_file.write("\n".as_bytes()).await;
    }
    let throughput_file_name = coordinator.id.to_string() + "throughput.data";
    let mut throughput_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(throughput_file_name)
        .await
        .unwrap();
    throughput_file
        .write(throughput_result.to_string().as_bytes())
        .await;
    throughput_file.write("\n".as_bytes()).await;
}

async fn run_transacntion(
    coordinator: &mut DtxCoordinator,
    read_set: Vec<ReadStruct>,
    write_set: Vec<WriteStruct>,
) -> bool {
    coordinator.tx_begin().await;
    coordinator.read_set = read_set;
    coordinator.write_set = write_set;
    if !coordinator.tx_exe().await {
        coordinator.tx_abort().await;
        return false;
    }
    if !coordinator.tx_commit().await {
        coordinator.tx_abort().await;
        return false;
    }
    true
}
