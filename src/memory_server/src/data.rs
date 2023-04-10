use std::collections::HashMap;

use common::{DbType, Tuple};
use rpc::common::{ReadStruct, WriteStruct};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use workload::{micro_db::init_micro_db, tatp_db::init_tatp_data};

pub static mut DATA: Vec<HashMap<u64, RwLock<Tuple>>> = Vec::new();

pub fn init_data(txn_type: DbType) {
    unsafe {
        match txn_type {
            DbType::micro => {
                DATA = init_micro_db();
            }
            DbType::tatp => DATA = init_tatp_data(),
            DbType::smallbank => todo!(),
        }
    }
}

pub async fn validate_read_set(read_set: Vec<ReadStruct>) -> (bool, Vec<ReadStruct>) {
    let mut result = Vec::new();
    unsafe {
        for iter in read_set {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key).unwrap().try_read() {
                Ok(guard) => {
                    // insert into result
                    let read_struct = ReadStruct {
                        key: iter.key,
                        table_id: iter.table_id,
                        value: None,
                        timestamp: Some(guard.ts),
                    };
                    result.push(read_struct);
                }
                Err(_) => {
                    // has been locked
                    return (false, result);
                }
            }
        }
        (true, result)
    }
}

pub async fn get_read_set(read_set: Vec<ReadStruct>) -> (bool, Vec<ReadStruct>) {
    let mut result = Vec::new();
    unsafe {
        for iter in read_set {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(rwlock) => {
                    match rwlock.try_read() {
                        Ok(guard) => {
                            // insert into result
                            let read_struct = ReadStruct {
                                key: iter.key,
                                table_id: iter.table_id,
                                value: Some(guard.data.clone()),
                                timestamp: Some(guard.ts),
                            };
                            result.push(read_struct);
                        }
                        Err(_) => {
                            // has been locked
                            return (false, result);
                        }
                    }
                }
                None => continue,
            }
        }
        (true, result)
    }
}

pub async fn lock_write_set(write_set: Vec<WriteStruct>, txn_id: u64) -> bool {
    unsafe {
        for iter in write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    if !guard.set_lock(txn_id) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }
}

pub async fn update_and_release_locks(write_set: Vec<WriteStruct>, txn_id: u64) {
    unsafe {
        for iter in write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            let mut guard = table.get_mut(&iter.key).unwrap().write().await;
            guard.release_lock(txn_id);
            guard.data = iter.value.clone().unwrap();
        }
    }
}

pub async fn releass_locks(write_set: Vec<WriteStruct>, txn_id: u64) {
    unsafe {
        for iter in write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            let mut guard = table.get_mut(&iter.key).unwrap().write().await;
            guard.release_lock(txn_id)
        }
    }
}

// pub struct Data {
//     pub txn_type: DbType,
//     pub tables: HashMap<i32, HashMap<u64, RwLock<Tuple>>>,
// }

// impl Data {
//     pub fn new(txn_type: DbType) -> Self {
//         let mut tables = HashMap::new();

//         match txn_type {
//             DbType::micro => {
//                 tables = init_micro_db();
//             }
//             DbType::tatp => todo!(),
//             DbType::smallbank => todo!(),
//         }

//         Self { txn_type, tables }
//     }

//     pub async fn validate_read_set(
//         &mut self,
//         read_set: Vec<ReadStruct>,
//     ) -> (bool, Vec<ReadStruct>) {
//         let mut result = Vec::new();
//         for iter in read_set {
//             let guard = self
//                 .tables
//                 .get_mut(&iter.table_id)
//                 .unwrap()
//                 .get_mut(&iter.key)
//                 .unwrap()
//                 .read()
//                 .await;

//             if guard.lock_txn_id != 0 {
//                 // has been locked
//                 return (false, result);
//             }
//             // insert into result
//             let read_struct = ReadStruct {
//                 key: iter.key,
//                 table_id: iter.table_id,
//                 value: None,
//                 timestamp: Some(guard.ts),
//             };
//             result.push(read_struct);
//         }
//         (true, result)
//     }

//     pub async fn get_read_set(&mut self, read_set: Vec<ReadStruct>) -> (bool, Vec<ReadStruct>) {
//         let mut result = Vec::new();
//         for iter in read_set {
//             let guard = self
//                 .tables
//                 .get_mut(&iter.table_id)
//                 .unwrap()
//                 .get_mut(&iter.key)
//                 .unwrap()
//                 .read()
//                 .await;

//             if guard.lock_txn_id != 0 {
//                 // has been locked
//                 return (false, result);
//             }
//             // insert into result
//             let read_struct = ReadStruct {
//                 key: iter.key,
//                 table_id: iter.table_id,
//                 value: Some(guard.data.clone()),
//                 timestamp: Some(guard.ts),
//             };
//             result.push(read_struct);
//         }
//         (true, result)
//     }

//     pub async fn lock_write_set(&mut self, write_set: Vec<WriteStruct>, txn_id: u64) -> bool {
//         for iter in write_set.iter() {
//             let mut guard = self
//                 .tables
//                 .get_mut(&iter.table_id)
//                 .unwrap()
//                 .get_mut(&iter.key)
//                 .unwrap()
//                 .write()
//                 .await;
//             if guard.lock_txn_id == 0 {
//                 guard.lock_txn_id = txn_id;
//             } else {
//                 return false;
//             }
//         }
//         true
//     }

//     pub async fn update_and_release_locks(&mut self, write_set: Vec<WriteStruct>) {
//         for iter in write_set.iter() {
//             let mut guard = self
//                 .tables
//                 .get_mut(&iter.table_id)
//                 .unwrap()
//                 .get_mut(&iter.key)
//                 .unwrap()
//                 .write()
//                 .await;
//             guard.lock_txn_id = 0;
//             guard.data = iter.value.clone().unwrap();
//         }
//     }

//     pub async fn releass_locks(&mut self, write_set: Vec<WriteStruct>, txn_id: u64) {
//         for iter in write_set.iter() {
//             let mut guard = self
//                 .tables
//                 .get_mut(&iter.table_id)
//                 .unwrap()
//                 .get_mut(&iter.key)
//                 .unwrap()
//                 .write()
//                 .await;
//             if guard.lock_txn_id == txn_id {
//                 guard.lock_txn_id = 0;
//             }
//         }
//     }
// }
