use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common::{f64_rand, Tuple};
use rpc::common::{ReadStruct, WriteStruct};
use tokio::sync::RwLock;

const MicroTableSize: i32 = 100000;

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

fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum: f64 = 0.0;
    for i in 1..(n + 1) {
        sum += f64::powf(1.0 / (i as f64), theta);
    }
    return sum;
}

pub struct MicroQuery {
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<WriteStruct>,
    zeta_2_theta: f64,
    denom: f64,
    write_value: String,
    req_per_query: i32,
    table_size: i32,
    read_perc: i32,
    theta: f64,
    pub read_only: bool,
}

impl MicroQuery {
    pub fn new(theta: f64, req_per_query: i32, read_perc: i32) -> Self {
        let zeta_2_theta = zeta(2, theta);
        let value: Vec<char> = vec!['a'; 100];
        let mut write_value = String::from("");
        write_value.extend(value.iter());
        Self {
            read_set: Vec::new(),
            write_set: Vec::new(),
            zeta_2_theta,
            denom: zeta(MicroTableSize as u64, theta),
            write_value,
            req_per_query,
            table_size: MicroTableSize,
            read_perc,
            theta,
            read_only: false,
        }
    }

    pub fn generate(&mut self) {
        self.read_only = true;
        self.read_set.clear();
        self.write_set.clear();
        let mut keys = Vec::new();
        for _ in 0..self.req_per_query {
            let op = f64_rand(0.0, 1.0, 0.01);

            let key = self.zipf(self.table_size as u64, self.theta);

            if keys.contains(&key) {
                continue;
            } else {
                keys.push(key);
            }

            if op * 100.0 <= self.read_perc as f64 {
                self.read_set.push(ReadStruct {
                    key: key as u64,
                    value: None,
                    timestamp: None,
                    table_id: todo!(),
                });
            } else {
                self.write_set.push(WriteStruct {
                    key: key as u64,
                    value: Some(self.write_value.clone()),
                    table_id: todo!(),
                    // timestamp: None,
                });
                self.read_only = false;
            }
        }
    }

    fn zipf(&self, n: u64, theta: f64) -> u64 {
        let zetan = self.denom;

        let u = f64_rand(0.0, 1.0, 0.000_000_000_000_001);
        let uz = u * zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + f64::powf(0.5, theta) {
            return 1;
        }
        let alpha = 1.0 / (1.0 - theta);

        let eta =
            (1.0 - f64::powf(2.0 / n as f64, 1.0 - theta)) / (1.0 - self.zeta_2_theta / zetan);
        let mut v = ((n as f64) * f64::powf(eta * u - eta + 1.0, alpha)) as u64;
        if v >= n {
            v = n - 1;
        }
        v
    }
}
