use std::collections::HashMap;

use common::txn::DtxType;
use tokio::sync::RwLock;

pub struct Tuple {
    pub lock: bool,
    pub ts: u64,
    pub data: String,
}
pub struct Data {
    pub txn_type: DtxType,
    pub tables: HashMap<i32, HashMap<u64, RwLock<Tuple>>>,
}

impl Data {
    pub fn get(&self) {}

    pub fn lock(&mut self) {}

    pub fn update_and_release(&mut self) {}
}
