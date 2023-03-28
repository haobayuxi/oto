pub mod txn;

use rand::*;
use rpc::common::Msg;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender as OneShotSender;

pub static SUBSCRIBER: i32 = 0;
pub static DERSTINATION: i32 = 1;
pub static ACCESS_INFO: i32 = 2;
pub static LOCATION: i32 = 3;

pub static TXNS_PER_CLIENT: u64 = 1000;

#[derive(Clone)]
pub struct Tuple {
    pub lock_txn_id: u64, // 0 state for no lock
    pub ts: u64,
    pub data: String,
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

impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: "166.111.69.11:500001".to_string(),
            cto_addr: "192.168.50.12:500001".to_string(),
            executor_num: 20,
            client_num: 100,
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
