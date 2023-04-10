pub mod txn;

use rand::*;
use rpc::common::Msg;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender as OneShotSender;

pub static SUBSCRIBER_TABLE: i32 = 0;
pub static SPECIAL_FACILITY_TABLE: i32 = 1;
pub static ACCESS_INFO_TABLE: i32 = 2;
pub static CALL_FORWARDING_TABLE: i32 = 3;
pub static SAVING_TABLE: i32 = 0;
pub static CHECKING_TABLE: i32 = 1;

pub static TXNS_PER_CLIENT: u64 = 1000;

#[derive(Clone)]
pub struct Tuple {
    lock_txn_id: u64, // 0 state for no lock
    pub ts: u64,
    pub data: String,
}

impl Tuple {
    pub fn new(data: String) -> Self {
        Self {
            lock_txn_id: 0,
            ts: 0,
            data,
        }
    }

    pub fn is_locked(&self) -> bool {
        if self.lock_txn_id == 0 {
            return false;
        }
        true
    }
    pub fn set_lock(&mut self, txn_id: u64) -> bool {
        if self.lock_txn_id == 0 {
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
}

#[derive(Serialize, Deserialize)]
pub struct ConfigInFile {
    pub db_type: String,
    pub dtx_type: String,
    pub zipf: f64,
}

#[derive(PartialEq, Eq, Deserialize, Clone, Copy)]
pub enum DtxType {
    oto,
    occ,
    to,
}

#[derive(Clone, Serialize, Deserialize, Copy)]
pub enum DbType {
    micro,
    tatp,
    smallbank,
}

pub struct Config {
    pub server_addr: String,
    pub cto_addr: String,
    pub executor_num: u64,
    pub client_num: u64,
}

// optane08 192.168.1.88 client
// optane09 192.168.1.89 server
// optane10 192.168.1.70 cto

impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: "192.168.1.89:8001".to_string(),
            cto_addr: "192.168.1.70:8001".to_string(),
            executor_num: 20,
            client_num: 60,
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
