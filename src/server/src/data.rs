use std::collections::HashMap;

use common::Tuple;
use rpc::common::{ReadStruct, WriteStruct};
use tokio::sync::RwLock;
use workload::micro_db::init_micro_db;

pub enum DbType {
    micro,
    tatp,
    smallbank,
}

pub struct Data {
    pub txn_type: DbType,
    pub tables: HashMap<i32, HashMap<u64, RwLock<Tuple>>>,
}

impl Data {
    pub fn new(txn_type: DbType) -> Self {
        let mut tables = HashMap::new();

        match txn_type {
            DbType::micro => {
                tables = init_micro_db();
            }
            DbType::tatp => todo!(),
            DbType::smallbank => todo!(),
        }

        Self { txn_type, tables }
    }

    pub async fn get_read(&mut self, read_set: Vec<ReadStruct>) -> (bool, Vec<ReadStruct>) {
        let mut result = Vec::new();
        for iter in read_set {
            let guard = self
                .tables
                .get_mut(&iter.table_id)
                .unwrap()
                .get_mut(&iter.key)
                .unwrap()
                .read()
                .await;

            if guard.lock_txn_id != 0 {
                // has been locked
                return (false, result);
            }
            // insert into result
            let read_struct = ReadStruct {
                key: iter.key,
                table_id: iter.table_id,
                value: Some(guard.data.clone()),
                timestamp: Some(guard.ts),
            };
            result.push(read_struct);
        }
        (true, result)
    }

    pub async fn lock(&mut self, write_set: WriteStruct) -> bool {
        let table_id = 0;
        let key = 0;
        let tuple = self
            .tables
            .get_mut(&table_id)
            .unwrap()
            .get_mut(&key)
            .unwrap();
        let write_guard = tuple.write().await;
        if write_guard.lock_txn_id != 0 {
            return false;
        }
        true
    }

    pub fn update_and_release(&mut self) {}
}
