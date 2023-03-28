pub mod txn;

use rand::*;
use rpc::common::Msg;
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

pub struct Config {
    pub server_addr: String,
    pub executor_num: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: "166.111.69.11".to_string(),
            executor_num: 20,
        }
    }
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
