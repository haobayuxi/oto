use std::{collections::HashMap, env, sync::Arc};

use rpc::common::{
    data_service_server::{DataService, DataServiceServer},
    Msg,
};
use tokio::sync::{
    mpsc::UnboundedSender,
    oneshot::{self, Sender as OneShotSender},
};
use tonic::{transport::Server, Request, Response, Status};

pub struct CoordnatorMsg {
    pub msg: Msg,
    pub call_back: OneShotSender<Msg>,
}
pub struct RpcServer {
    executor_num: u64,
    addr_to_listen: String,
    sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
}

impl RpcServer {
    pub fn new(
        executor_num: u64,
        addr_to_listen: String,
        sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) -> Self {
        Self {
            executor_num,
            sender,
            addr_to_listen,
        }
    }
}

pub async fn run_rpc_server(rpc_server: RpcServer) {
    let addr = rpc_server.addr_to_listen.parse().unwrap();

    println!("rpc server listening on: {:?}", addr);

    let server = DataServiceServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl DataService for RpcServer {
    async fn communication(&self, request: Request<Msg>) -> Result<Response<Msg>, Status> {
        let (callback_sender, receiver) = oneshot::channel::<Msg>();
        let msg = request.into_inner();
        let executor_id = msg.txn_id % self.executor_num;
        let coor_msg = CoordnatorMsg {
            msg,
            call_back: callback_sender,
        };
        self.sender.get(&executor_id).unwrap().send(coor_msg);
        let reply = receiver.await.unwrap();
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();

    Ok(())
}
