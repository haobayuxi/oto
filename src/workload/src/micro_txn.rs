use common::txn::DtxCoordinator;
use rpc::common::{ReadStruct, WriteStruct};

pub async fn micro_run_transactions() {
    // init

    // run transaction
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
