use common::{txn::DtxCoordinator, SUBSCRIBER};

use crate::tatp_db::get_sid;

pub async fn run_tatp_transactions() {}

async fn run_transaction() {}

async fn tx_get_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;

    // build key
    let s_id = get_sid();
    coordinator.add_read_to_execute(s_id, SUBSCRIBER);

    let commit_status = coordinator.tx_commit().await;

    return commit_status;
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
