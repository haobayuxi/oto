pub mod txn;

use rand::*;

pub struct Tuple {
    pub lock_txn_id: u64, // 0 state for no lock
    pub ts: u64,
    pub data: String,
}

pub struct Config {
    pub server_addr: String,
    pub executor_num: i32,
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
