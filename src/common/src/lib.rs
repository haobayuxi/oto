// pub mod shmem;
pub mod throughput_statistics;
pub mod txn;

use std::{
    collections::{BTreeSet, HashSet},
    sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT},
    vec,
};

use rand::*;
use rpc::common::Msg;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender as OneShotSender;

pub static GLOBAL_COMMITTED: AtomicUsize = ATOMIC_USIZE_INIT;

pub static SUBSCRIBER_TABLE: i32 = 0;
pub static SPECIAL_FACILITY_TABLE: i32 = 1;
pub static ACCESS_INFO_TABLE: i32 = 2;
pub static CALL_FORWARDING_TABLE: i32 = 3;
pub static SAVING_TABLE: i32 = 0;
pub static CHECKING_TABLE: i32 = 1;
pub static ACCOUNT_TABLE: i32 = 2;

pub static TXNS_PER_CLIENT: u64 = 10000;
pub static CID_LEN: u32 = 50;

#[derive(Clone)]
pub struct Tuple {
    lock_txn_id: u64, // 0 state for no lock
    read_lock: HashSet<u64>,
    // janus meta
    pub last_accessed: u64,
    pub ts: u64,
    pub data: String,
    // meerkat meta
    pub prepared_read: BTreeSet<u64>,
    pub prepared_write: BTreeSet<u64>,
    pub rts: u64,
}

impl Tuple {
    pub fn new(data: String) -> Self {
        Self {
            lock_txn_id: 0,
            ts: 0,
            data,
            prepared_read: BTreeSet::new(),
            prepared_write: BTreeSet::new(),
            rts: 0,
            read_lock: HashSet::new(),
            last_accessed: 0,
        }
    }

    pub fn is_locked(&self) -> bool {
        if self.lock_txn_id == 0 {
            return false;
        }
        true
    }
    pub fn set_lock(&mut self, txn_id: u64) -> bool {
        if self.lock_txn_id == txn_id {
            return true;
        } else if self.lock_txn_id == 0 {
            self.lock_txn_id = txn_id;
            return true;
        }
        false
    }
    pub fn release_lock(&mut self, txn_id: u64) {
        if self.lock_txn_id == txn_id {
            self.lock_txn_id = 0;
        }
    }

    pub fn set_read_lock(&mut self, txn_id: u64) -> bool {
        if self.lock_txn_id != 0 {
            return false;
        }
        self.read_lock.insert(txn_id);
        return true;
    }
    pub fn release_read_lock(&mut self, txn_id: u64) -> bool {
        self.read_lock.remove(&txn_id);
        return true;
    }
}

#[derive(Serialize, Deserialize)]
pub struct ConfigInFile {
    pub db_type: String,
    pub dtx_type: String,
    // pub zipf: f64,
    pub client_num: u64,
    pub read_only: bool,
}

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Copy)]
pub enum DtxType {
    rocc,
    r2pl,
    spanner,
    ford,
    meerkat,
    janus,
    rjanus,
}

#[derive(Clone, Serialize, Deserialize, Copy)]
pub enum DbType {
    micro,
    tatp,
    smallbank,
}

pub struct Config {
    pub server_addr: Vec<String>,
    pub cto_addr: String,
    pub client_addr: Vec<String>,
    // pub
    pub executor_num: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: vec![
                "192.168.1.88:10001".to_string(), //optane08
                "192.168.1.71:10001".to_string(), //optane11
                "192.168.1.72:10001".to_string(), //optane12
            ],
            cto_addr: "192.168.1.89:10001".to_string(),
            // "192.168.1.89:10002".to_string(),
            // ],
            executor_num: 30,
            client_addr: vec![
                "192.168.1.70:10001".to_string(), //optane10
                "192.168.1.74:10001".to_string(), //optane14
                "192.168.1.75:10001".to_string(), //optane15
            ],
        }
    }
}

pub fn ip_addr_add_prefix(ip: String) -> String {
    let prefix = String::from("http://");
    prefix + ip.clone().as_str()
}

pub fn u64_rand(lower_bound: u64, upper_bound: u64) -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}
pub fn f64_rand(lower_bound: f64, upper_bound: f64, precision: f64) -> f64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(
        (lower_bound / precision) as u64,
        (upper_bound / precision) as u64 + 1,
    ) as f64
        * precision
}

pub struct CoordnatorMsg {
    pub msg: Msg,
    pub call_back: OneShotSender<Msg>,
}
