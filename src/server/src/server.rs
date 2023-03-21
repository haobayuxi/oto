use std::{collections::HashMap, env, sync::Arc};

use common::{Config, CoordnatorMsg};
use memory_server::{data::DbType, data_server::DataServer, executor::Executor};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ServerConfig {
    id: i32,
    db_type: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ServerConfig = serde_yaml::from_reader(f).unwrap();
    let db_type = serde_yaml::from_str(&server_config.db_type).unwrap();
    let mut server = DataServer::new(0, Config::default());
    server.init_and_run(db_type).await;
    Ok(())
}
