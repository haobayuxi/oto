use std::{collections::HashMap, env, sync::Arc};

use common::Config;
use rpc::common::{
    data_service_server::{DataService, DataServiceServer},
    Msg,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
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

struct DataServer {
    server_id: i32,
    executor_num: i32,
    executor_senders: HashMap<i32, UnboundedSender<CoordnatorMsg>>,
}

impl DataServer {
    pub fn new() -> Self {}

    async fn init_rpc(&mut self, config: Config, sender: UnboundedSender<Msg>) {
        // start server for client to connect
        let mut listen_ip = config.server_addr;
        listen_ip = convert_ip_addr(listen_ip, false);
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(listen_ip, sender);
        tokio::spawn(async move {
            run_rpc_server(server).await;
        });
    }

    fn init_executors(&mut self, config: Config) {
        // self.executor_num = config.executor_num;
        self.executor_num = config.executor_num;
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<CoordnatorMsg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(i, self.server_id, receiver, indexs.clone());
            tokio::spawn(async move {
                exec.run().await;
            });
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();

    Ok(())
}
