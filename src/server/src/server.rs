use std::env;

use rpc::common::data_service_server::DataServiceServer;
use tonic::transport::Server;

async fn run_rpc_server(addr_to_listen: String) -> bool {
    let addr = addr_to_listen.parse().unwrap();
    println!("rpc server listening on: {:?}", addr);

    let server = DataServiceServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
    true
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();

    Ok(())
}
