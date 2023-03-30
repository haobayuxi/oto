use std::collections::HashMap;

use common::{u64_rand, Tuple};
use tokio::sync::RwLock;

static TX_HOT_PERCENT: u64 = 90;
static HOTSPOT_SIZE: u64 = 100;
static FREQUENCY_AMALGAMATE: u64 = 15;
static FREQUENCY_BALANCE: u64 = 15;
static FREQUENCY_DEPOSIT_CHECKING: u64 = 15;
static FREQUENCY_SEND_PAYMENT: u64 = 25;
static FREQUENCY_TRANSACT_SAVINGS: u64 = 15;
static FREQUENCY_WRITE_CHECK: u64 = 15;

pub static NUM_ACCOUNTS: u64 = 100000;

pub fn init_smallbank_db() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let value: Vec<char> = vec!['a'; 40];
    let mut write_value = String::from("");
    write_value.extend(value.iter());
    let mut tables = Vec::new();
    let mut table = HashMap::new();
    for i in 0..NUM_ACCOUNTS {
        table.insert(i, RwLock::new(Tuple::new(write_value.clone())));
    }
    tables.push(table);
    tables
}

pub struct Saving {
    customer_id: u64,
    balance: u64,
}

pub struct Checking {
    customer_id: u64,
    balance: u64,
}

pub fn get_c_id() -> u64 {
    let is_hotspot = u64_rand(0, 99) < TX_HOT_PERCENT;
    if is_hotspot {
        return u64_rand(HOTSPOT_SIZE, NUM_ACCOUNTS - 1);
    } else {
        return u64_rand(0, HOTSPOT_SIZE - 1);
    }
}
