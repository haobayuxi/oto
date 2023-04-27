use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use common::{Config, CoordnatorMsg, DbType, DtxType};
use rpc::common::{
    data_service_server::{DataService, DataServiceServer},
    Echo, Msg, Throughput,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot,
};
use tonic::{transport::Server, Request, Response, Status};

use crate::{data::init_data, executor::Executor};

pub struct RpcServer {
    executor_num: u64,
    addr_to_listen: String,
    sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    // read_only_committed: Arc<AtomicU64>,
    // read_write_committed: Arc<AtomicU64>,
}

impl RpcServer {
    pub fn new(
        executor_num: u64,
        addr_to_listen: String,
        sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
        read_only_committed: Arc<AtomicU64>,
        read_write_committed: Arc<AtomicU64>,
    ) -> Self {
        Self {
            executor_num,
            sender,
            addr_to_listen,
            // read_only_committed,
            // read_write_committed,
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

pub struct DataServer {
    server_id: u32,
    executor_num: u64,
    executor_senders: HashMap<u64, UnboundedSender<CoordnatorMsg>>,
    config: Config,
    read_only_committed: Arc<AtomicU64>,
    read_write_committed: Arc<AtomicU64>,
}

impl DataServer {
    pub fn new(server_id: u32, config: Config) -> Self {
        Self {
            server_id,
            executor_num: config.executor_num,
            executor_senders: HashMap::new(),
            config,
            read_only_committed: Arc::new(AtomicU64::new(0)),
            read_write_committed: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn init_rpc(
        &mut self,
        executor_senders: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) {
        // start server for client to connect
        let listen_ip = self.config.server_addr[self.server_id as usize].clone();
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(
            self.executor_num,
            listen_ip,
            executor_senders,
            self.read_only_committed.clone(),
            self.read_write_committed.clone(),
        );

        run_rpc_server(server).await;
    }

    fn init_executors(&mut self, db_type: DbType, dtx_type: DtxType) {
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<CoordnatorMsg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(
                i, receiver,
                dtx_type,
                // self.read_only_committed.clone(),
                // self.read_write_committed.clone(),
            );
            tokio::spawn(async move {
                exec.run().await;
            });
        }
    }

    pub async fn init_and_run(&mut self, db_type: DbType, dtx_type: DtxType) {
        init_data(db_type);
        self.init_executors(db_type, dtx_type);
        self.init_rpc(Arc::new(self.executor_senders.clone())).await;
    }
}
