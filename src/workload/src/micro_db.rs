use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common::Tuple;
use tokio::sync::RwLock;

pub fn init_micro_db() -> HashMap<i32, HashMap<u64, RwLock<Tuple>>> {
    let value: Vec<char> = vec!['a'; 40];
    let mut write_value = String::from("");
    write_value.extend(value.iter());
    let mut tables = HashMap::new();
    let mut table = HashMap::new();
    for i in 0..100 {
        table.insert(
            i,
            RwLock::new(Tuple {
                lock_txn_id: 0,
                ts: 0,
                data: write_value.clone(),
            }),
        );
    }
    tables.insert(0, table);
    tables
}
