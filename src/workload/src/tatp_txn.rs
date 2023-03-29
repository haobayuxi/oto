use common::{
    txn::DtxCoordinator, u64_rand, ACCESS_INFO_TABLE, CALL_FORWARDING_TABLE,
    SPECIAL_FACILITY_TABLE, SUBSCRIBER_TABLE,
};

use crate::tatp_db::{
    ai_key, cf_key, get_sid, rnd, sf_key, CallForwarding, SpecialFacility, Subscriber,
};

pub async fn tatp_run_transactions(coordinator: &mut DtxCoordinator) -> (Vec<u128>, f64) {
    let mut latency_result = Vec::new();
    (latency_result, 0.0)
}

async fn run_transaction() {}

async fn tx_get_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;

    // build key
    let s_id = get_sid();
    coordinator.add_read_to_execute(s_id, SUBSCRIBER_TABLE);

    let (status, result) = coordinator.tx_exe().await;
    return status;
}

async fn tx_get_new_destination(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;

    // build key
    let s_id = get_sid();
    let sf_type = u64_rand(1, 4);
    let start_time = u64_rand(0, 2) * 8;
    let end_time = u64_rand(1, 24);
    let cf_to_fetch = start_time / 8 + 1;

    coordinator.add_read_to_execute(sf_key(s_id, sf_type), SPECIAL_FACILITY_TABLE);

    let (status, specfac_result) = coordinator.tx_exe().await;
    if specfac_result.len() == 0 || !status {
        // fail to get
        return false;
    }
    let specfac: SpecialFacility =
        bincode::deserialize(&specfac_result[0].value().as_bytes()).unwrap();
    if !specfac.is_active {
        return false;
    }

    // fetch call forwarding records
    for i in 0..cf_to_fetch {
        let cf_key = cf_key(s_id, sf_type, start_time);
        coordinator.add_read_to_execute(cf_key, CALL_FORWARDING_TABLE);
    }

    let (status, cf_result) = coordinator.tx_exe().await;
    if !status {
        return false;
    }

    for iter in cf_result {
        let cf_obj: CallForwarding = bincode::deserialize(&iter.value().as_bytes()).unwrap();
        if cf_obj.start_time as u64 > start_time || cf_obj.end_time as u64 <= end_time {
            return false;
        }
    }

    true
}

async fn tx_get_access_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;

    // build key
    let s_id = get_sid();
    let ai_type = rnd("ai_type") as u64;
    let a_id = ai_key(s_id, ai_type);
    coordinator.add_read_to_execute(a_id, ACCESS_INFO_TABLE);

    let (status, result) = coordinator.tx_exe().await;
    if !status || result.len() == 0 {
        return false;
    }
    return true;
}

async fn tx_update_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;
    // build key
    let s_id = get_sid();
    let sub_obj = Subscriber::new(s_id);
    // coordinator.add_write_to_execute(key, table_id, value)
    let sf_type = rnd("sf_type") as u64;

    true
}

async fn tx_update_lcoation(coordinator: &mut DtxCoordinator) -> bool {
    true
}

// async fn tx_insert_call_forwarding() {}

// async fn tx_delete_call_forwarding() {}
