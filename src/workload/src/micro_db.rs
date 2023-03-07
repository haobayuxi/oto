use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::RwLock;

pub struct MicroDb {
    pub store: HashMap<u64, Arc<RwLock<String>>>,
}

impl MicroDb {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    // pub fn get(&self, key: u64) -> String {
    //     // self.store.g
    // }
}
