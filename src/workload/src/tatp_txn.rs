use common::txn::DtxCoordinator;

pub async fn run_tatp_transactions() {}

async fn run_transaction() {}

async fn tx_get_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;

    true
}

async fn tx_get_new_destination() -> bool {
    true
}

async fn tx_get_access_data() -> bool {
    true
}

async fn tx_update_subscriber_data() -> bool {
    true
}

async fn tx_update_lcoation() {}

async fn tx_insert_call_forwarding() {}

async fn tx_delete_call_forwarding() {}
