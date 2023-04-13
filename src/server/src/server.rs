use std::{collections::HashMap, env, sync::Arc};

use common::{Config, ConfigInFile, CoordnatorMsg};
use memory_server::{data_server::DataServer, executor::Executor};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let db_type: common::DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let mut server = DataServer::new(0, Config::default());
    server.init_and_run(db_type, dtx_type).await;
    Ok(())
}
