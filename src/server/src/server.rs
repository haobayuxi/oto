use std::{collections::HashMap, env, sync::Arc};

use common::{Config, CoordnatorMsg};
use memory_server::{data::DbType, executor::Executor};
use rpc::common::{
    data_service_server::{DataService, DataServiceServer},
    Msg,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot::{self, Sender as OneShotSender},
};
use tonic::{transport::Server, Request, Response, Status};

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
    executor_num: u64,
    executor_senders: HashMap<u64, UnboundedSender<CoordnatorMsg>>,
}

impl DataServer {
    pub fn new(server_id: i32, executor_num: u64) -> Self {
        Self {
            server_id,
            executor_num,
            executor_senders: HashMap::new(),
        }
    }

    async fn init_rpc(
        &mut self,
        config: Config,
        executor_senders: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) {
        // start server for client to connect
        let listen_ip = config.server_addr;
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(config.executor_num, listen_ip, executor_senders);
        tokio::spawn(async move {
            run_rpc_server(server).await;
        });
    }

    fn init_executors(&mut self, config: Config, db_type: DbType) {
        // self.executor_num = config.executor_num;
        self.executor_num = config.executor_num;
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<CoordnatorMsg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(i, receiver);
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
