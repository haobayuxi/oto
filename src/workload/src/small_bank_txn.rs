use common::{txn::DtxCoordinator, u64_rand, CHECKING_TABLE, SAVING_TABLE};

use crate::small_bank_db::{get_c_id, Checking, Saving, MAX_BALANCE, MIN_BALANCE};

pub async fn small_bank_run_transactions(coordinator: &mut DtxCoordinator) -> (Vec<u128>, f64) {
    let mut latency_result = Vec::new();
    (latency_result, 0.0)
}

// It returns the sum of savings and checking balances for the specified customer
async fn balance(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;
    let c_id = get_c_id();
    coordinator.add_read_to_execute(c_id, SAVING_TABLE);
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    status
}

async fn deposit_checking(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;
    let c_id = get_c_id();
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);

    let check_obj = coordinator.add_write_to_execute(c_id, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut check_record: Checking = bincode::deserialize(&result[0].value().as_bytes()).unwrap();
    check_record.balance += u64_rand(MIN_BALANCE, MAX_BALANCE);
    check_obj.write().await.value =
        Some(String::from_utf8(bincode::serialize(&check_record).unwrap()).unwrap());
    coordinator.tx_commit().await;
    true
}

async fn transac_saving(coordinator: &mut DtxCoordinator) -> bool {
    true
}

async fn amalgamate(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;
    let c_from = get_c_id();
    let c_to = get_c_id();
    coordinator.add_read_to_execute(c_from, SAVING_TABLE);
    coordinator.add_read_to_execute(c_from, CHECKING_TABLE);
    coordinator.add_read_to_execute(c_to, CHECKING_TABLE);

    let save_obj_from = coordinator.add_write_to_execute(c_from, CHECKING_TABLE, "".to_string());
    let check_obj_from = coordinator.add_write_to_execute(c_from, CHECKING_TABLE, "".to_string());
    let check_obj_to = coordinator.add_write_to_execute(c_to, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }

    let mut save_record_from: Saving = bincode::deserialize(&result[0].value().as_bytes()).unwrap();
    let mut check_record_from: Saving =
        bincode::deserialize(&result[1].value().as_bytes()).unwrap();
    let mut check_record_to: Saving = bincode::deserialize(&result[2].value().as_bytes()).unwrap();

    check_record_to.balance += save_record_from.balance + check_record_from.balance;
    save_record_from.balance = 0;
    check_record_from.balance = 0;

    save_obj_from.write().await.value =
        Some(String::from_utf8(bincode::serialize(&save_record_from).unwrap()).unwrap());

    check_obj_from.write().await.value =
        Some(String::from_utf8(bincode::serialize(&check_record_from).unwrap()).unwrap());
    check_obj_to.write().await.value =
        Some(String::from_utf8(bincode::serialize(&check_record_to).unwrap()).unwrap());
    coordinator.tx_commit().await;
    true
}

async fn write_check(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin().await;
    let c_id = get_c_id();
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);

    let check_obj = coordinator.add_write_to_execute(c_id, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut check_record: Checking = bincode::deserialize(&result[0].value().as_bytes()).unwrap();
    check_record.balance += u64_rand(MIN_BALANCE, MAX_BALANCE);
    check_obj.write().await.value =
        Some(String::from_utf8(bincode::serialize(&check_record).unwrap()).unwrap());
    coordinator.tx_commit().await;
    true
}
