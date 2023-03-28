use common::txn::DtxCoordinator;

pub async fn small_bank_run_transactions(coordinator: &mut DtxCoordinator) -> (Vec<u128>, f64) {
    let mut latency_result = Vec::new();
    (latency_result, 0.0)
}
