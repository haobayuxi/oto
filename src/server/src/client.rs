use common::txn::DtxCoordinator;
use std::{env, sync::Arc};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let local_ts = Arc::new(RwLock::new(0));
    let client_num = 100;
    for i in 0..100 {

        // let dtx_coordinator = DtxCoordinator::new();
    }
    Ok(())
}
