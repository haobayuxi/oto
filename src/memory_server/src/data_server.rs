use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use common::{txn::connect_to_peer, Config, CoordnatorMsg, DbType, DtxType};
use rpc::common::{
    data_service_client::DataServiceClient,
    data_service_server::{DataService, DataServiceServer},
    Echo, Msg, Throughput,
};
use tokio::sync::{
    mpsc::{channel, unbounded_channel, UnboundedSender},
    oneshot,
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use crate::{data::init_data, dep_graph::DepGraph, executor::Executor};

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
        let mut reply = Msg::default();
        match receiver.await {
            Ok(r) => reply = r,
            Err(e) => {
                println!("err {:?}", e);
                reply.success = false;
            }
        }
        Ok(Response::new(reply))
    }
}

pub struct DataServer {
    server_id: u32,
    executor_num: u64,
    executor_senders: HashMap<u64, UnboundedSender<CoordnatorMsg>>,
    config: Config,
    client_num: u64,
    //spanner
    peer_senders: Vec<DataServiceClient<Channel>>,
}

impl DataServer {
    pub fn new(server_id: u32, config: Config, client_num: u64) -> Self {
        Self {
            server_id,
            executor_num: config.executor_num,
            executor_senders: HashMap::new(),
            config,
            client_num,
            peer_senders: Vec::new(),
        }
    }

    async fn init_rpc(
        &mut self,
        executor_senders: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) {
        // start server for client to connect
        let listen_ip = self.config.server_addr[self.server_id as usize].clone();
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(self.executor_num, listen_ip, executor_senders);

        run_rpc_server(server).await;
        if self.server_id == 2 {
            //
            let mut data_ip = self.config.server_addr.clone();
            data_ip.pop();
            self.peer_senders = connect_to_peer(data_ip).await;
            println!("peer sender {}", self.peer_senders.len());
        }
    }

    fn init_executors(&mut self, dtx_type: DtxType) {
        let (dep_sender, dep_recv) = channel(1000);
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<CoordnatorMsg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(
                i,
                receiver,
                dtx_type,
                dep_sender.clone(),
                self.peer_senders.clone(),
            );
            tokio::spawn(async move {
                exec.run().await;
            });
        }
        if dtx_type == DtxType::janus {
            let mut dep = DepGraph::new(dep_recv, 0);
            tokio::spawn(async move {
                dep.run().await;
            });
        }
    }

    pub async fn init_and_run(&mut self, db_type: DbType, dtx_type: DtxType) {
        init_data(db_type, self.client_num);
        self.init_executors(dtx_type);
        self.init_rpc(Arc::new(self.executor_senders.clone())).await;
    }
}
