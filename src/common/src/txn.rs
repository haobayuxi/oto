use chrono::Local;
use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, Msg,
    ReadStruct, TxnOp,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::Duration;
use tonic::transport::Channel;

use crate::ip_addr_add_prefix;
use crate::DtxType;
use crate::GLOBAL_COMMITTED;

async fn init_coordinator_rpc(
    cto_ip: String,
    data_ip: Vec<String>,
) -> (CtoServiceClient<Channel>, Vec<DataServiceClient<Channel>>) {
    loop {
        match CtoServiceClient::connect(cto_ip.clone()).await {
            Ok(cto_client) => {
                let mut data_clients = Vec::new();
                for iter in data_ip {
                    let server_ip = ip_addr_add_prefix(iter);
                    // println!("connecting {}", server_ip);
                    loop {
                        match DataServiceClient::connect(server_ip.clone()).await {
                            Ok(data_client) => {
                                data_clients.push(data_client);
                                break;
                            }
                            Err(e) => {
                                // println!("connect error {}-- {:?}", server_ip, e);
                                sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                }
                // println!("connect server done");
                return (cto_client, data_clients);
            }
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    }
}
pub struct DtxCoordinator {
    pub id: u64,
    pub local_ts: Arc<RwLock<u64>>,
    pub dtx_type: DtxType,
    txn_id: u64,
    start_ts: u64,
    commit_ts: u64,
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<Arc<RwLock<ReadStruct>>>,
    read_to_execute: Vec<ReadStruct>,
    write_to_execute: Vec<Arc<RwLock<ReadStruct>>>,
    write_tuple_ts: Vec<u64>,
    cto_client: CtoServiceClient<Channel>,
    data_clients: Vec<DataServiceClient<Channel>>,
    // committed: Arc<AtomicU64>,
}

impl DtxCoordinator {
    pub async fn new(
        id: u64,
        local_ts: Arc<RwLock<u64>>,
        dtx_type: DtxType,
        cto_ip: String,
        data_ip: Vec<String>,
        // committed: Arc<AtomicU64>,
    ) -> Self {
        // init cto client & data client
        let (cto_client, data_clients) = init_coordinator_rpc(cto_ip, data_ip).await;
        // println!("init rpc done {}", id);
        Self {
            id,
            local_ts,
            dtx_type,
            txn_id: id,
            start_ts: 0,
            commit_ts: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            cto_client,
            data_clients,
            read_to_execute: Vec::new(),
            write_to_execute: Vec::new(),
            write_tuple_ts: Vec::new(),
            // committed,
        }
    }

    pub async fn tx_begin(&mut self) {
        // init coordinator
        self.txn_id += self.id;
        self.read_set.clear();
        self.write_set.clear();
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        self.write_tuple_ts.clear();
        self.start_ts = 0;
        self.commit_ts = 0;
        match self.dtx_type {
            DtxType::oto => {
                // get start ts from local
                self.start_ts = self.local_ts.read().await.clone();
            }
            _ => {}
        }
    }

    pub async fn tx_exe(&mut self) -> (bool, Vec<ReadStruct>) {
        if self.read_to_execute.is_empty() && self.write_to_execute.is_empty() {
            return (true, Vec::new());
        }
        let mut write_set = Vec::new();
        for iter in self.write_to_execute.iter() {
            write_set.push(iter.read().await.clone());
        }

        let server_id = self.id % 3;
        let (sender, mut recv) = unbounded_channel::<Msg>();
        let need_lock = if !self.write_to_execute.is_empty() && self.dtx_type != DtxType::meerkat {
            true
        } else {
            false
        };
        if need_lock {
            // lock the write
            let lock = Msg {
                txn_id: self.txn_id,
                read_set: Vec::new(),
                write_set,
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.start_ts),
            };
            // lock the primary
            let mut client = self.data_clients.get_mut(0).unwrap().clone();
            tokio::spawn(async move {
                let reply = client.communication(lock).await.unwrap().into_inner();
                sender.send(reply);
            });
        }
        let wait_for_cto = if self.dtx_type == DtxType::oto && self.commit_ts == 0 {
            true
        } else {
            false
        };
        let (commit_ts_sender, commit_ts_recv) = oneshot::channel();
        if wait_for_cto {
            // get commit ts from cto
            let mut cto_client = self.cto_client.clone();
            tokio::spawn(async move {
                let commit_ts = cto_client
                    .get_commit_ts(Echo::default())
                    .await
                    .unwrap()
                    .into_inner()
                    .ts;
                commit_ts_sender.send(commit_ts);
            });
        }
        let mut success = true;
        let mut result = Vec::new();
        if !self.read_to_execute.is_empty() {
            let read = Msg {
                txn_id: self.txn_id,
                read_set: self.read_to_execute.clone(),
                write_set: Vec::new(),
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.start_ts),
            };
            let client = self.data_clients.get_mut(server_id as usize).unwrap();

            let reply: Msg = client.communication(read).await.unwrap().into_inner();
            success = reply.success;
            result = reply.read_set;
        }
        if need_lock {
            let lock_reply = recv.recv().await.unwrap();
            if !lock_reply.success {
                success = false;
            } else {
                for iter in lock_reply.write_set.iter() {
                    self.write_tuple_ts.push(iter.timestamp());
                }
            }
        }
        if wait_for_cto {
            self.commit_ts = commit_ts_recv.await.unwrap();
        }

        self.read_set.extend(result.clone());
        self.write_set.extend(self.write_to_execute.clone());
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        return (success, result);
    }
    pub async fn tx_commit(&mut self) -> bool {
        // validate
        if self.dtx_type == DtxType::meerkat {
            self.commit_ts = (Local::now().timestamp_nanos() / 1000) as u64;
        }
        if self.validate().await {
            if self.write_set.is_empty() && self.dtx_type != DtxType::meerkat {
                GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            let mut write_set = Vec::new();
            for iter in self.write_set.iter() {
                write_set.push(iter.read().await.clone());
            }
            let mut commit = Msg {
                txn_id: self.txn_id,
                read_set: Vec::new(),
                write_set: write_set.clone(),
                op: TxnOp::Commit.into(),
                success: true,
                ts: Some(self.commit_ts),
            };
            // if self.dtx_type == DtxType::oto {
            //     // get commit ts
            //     let mut cto_client = self.cto_client.clone();
            //     let data_clients = self.data_clients.clone();
            //     let local_ts = self.local_ts.clone();
            //     tokio::spawn(async move {
            //         let commit_ts = cto_client
            //             .get_commit_ts(Echo::default())
            //             .await
            //             .unwrap()
            //             .into_inner()
            //             .ts;
            //         let mut guard = local_ts.write().await;
            //         if *guard < commit_ts {
            //             *guard = commit_ts;
            //         }
            //         commit.ts = Some(commit_ts);
            //         for iter in data_clients.iter() {
            //             let mut client = iter.clone();
            //             let msg_ = commit.clone();
            //             tokio::spawn(async move {
            //                 client.communication(msg_).await;
            //             });
            //         }
            //     });

            //     GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            //     return true;
            //     // tokio::spawn(async move {});
            // } else
            if self.dtx_type == DtxType::ford || self.dtx_type == DtxType::oto {
                // broadcast to lock the back
                let lock = Msg {
                    txn_id: self.txn_id,
                    read_set: Vec::new(),
                    write_set: write_set.clone(),
                    op: TxnOp::Execute.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                };
                self.sync_broadcast(lock).await;
            }

            if self.dtx_type == DtxType::oto {
                // get commit ts
                let mut guard = self.local_ts.write().await;
                if *guard < self.commit_ts {
                    *guard = self.commit_ts;
                }
            }
            // broadcast
            self.async_broadcast_commit(commit).await;

            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            return true;
        } else {
            self.tx_abort().await;
            return false;
        }
    }

    pub async fn tx_abort(&mut self) {
        if self.write_set.is_empty() {
            return;
        }
        let mut write_set = Vec::new();
        for iter in self.write_set.iter() {
            write_set.push(iter.read().await.clone());
        }
        if self.dtx_type == DtxType::meerkat {
            let abort = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set,
                op: TxnOp::Abort.into(),
                success: true,
                ts: Some(self.commit_ts),
            };
            self.async_broadcast_commit(abort).await;
        } else {
            let abort = Msg {
                txn_id: self.txn_id,
                read_set: Vec::new(),
                write_set,
                op: TxnOp::Abort.into(),
                success: true,
                ts: Some(self.commit_ts),
            };
            self.async_broadcast_commit(abort).await;
        }
    }

    pub fn add_read_to_execute(&mut self, key: u64, table_id: i32) {
        let read_struct = ReadStruct {
            key,
            table_id,
            value: None,
            timestamp: None,
        };
        self.read_to_execute.push(read_struct);
    }

    pub fn add_write_to_execute(
        &mut self,
        key: u64,
        table_id: i32,
        value: String,
    ) -> Arc<RwLock<ReadStruct>> {
        let write_struct = ReadStruct {
            key,
            table_id,
            value: Some(value),
            timestamp: None,
        };
        let obj = Arc::new(RwLock::new(write_struct));
        self.write_to_execute.push(obj.clone());
        obj
    }

    async fn validate(&mut self) -> bool {
        match self.dtx_type {
            DtxType::oto => {
                return self.oto_validate().await;
            }
            DtxType::ford => {
                if self.read_set.is_empty() {
                    // println!("read set is null");
                    return true;
                }
                let vadilate_msg = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_set.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Validate.into(),
                    success: true,
                    ts: None,
                };
                let server_id = self.id % 3;
                let client = self.data_clients.get_mut(server_id as usize).unwrap();
                let reply = client
                    .communication(vadilate_msg)
                    .await
                    .unwrap()
                    .into_inner();

                if !reply.success {
                    return false;
                }

                return true;
            }
            DtxType::meerkat => {
                let mut write_set = Vec::new();
                for iter in self.write_set.iter() {
                    write_set.push(iter.read().await.clone());
                }
                let vadilate_msg = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_set.clone(),
                    write_set: write_set.clone(),
                    op: TxnOp::Validate.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                };
                let reply = self.sync_broadcast(vadilate_msg).await;
                // check fast path
                let mut reply_success = 0;
                for iter in reply.iter() {
                    if iter.success {
                        reply_success += 1;
                    }
                }
                if reply_success == 0 {
                    // fast path abort
                    return false;
                } else if reply_success == 3 {
                    // fast path commit
                    return true;
                }
                // slow path
                let success = if reply_success == 1 { false } else { true };
                let vadilate_msg = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_set.clone(),
                    write_set: write_set.clone(),
                    op: TxnOp::Accept.into(),
                    success,
                    ts: Some(self.commit_ts),
                };
                let _reply = self.sync_broadcast(vadilate_msg).await;
                return success;
            }
        }
    }

    async fn oto_validate(&self) -> bool {
        let mut max_tx = self.start_ts;
        for iter in self.read_set.iter() {
            let ts = iter.timestamp();
            if ts > self.start_ts {
                max_tx = ts;
            }
        }
        if max_tx > self.start_ts {
            // update local ts
            let mut guard = self.local_ts.write().await;
            *guard = max_tx;
            return false;
        }
        true
    }

    async fn sync_broadcast(&mut self, msg: Msg) -> Vec<Msg> {
        let mut result = Vec::new();
        let (sender, mut recv) = unbounded_channel::<Msg>();
        for i in 0..self.data_clients.len() {
            let mut client = self.data_clients[i].clone();
            let s_ = sender.clone();
            let msg_ = msg.clone();
            tokio::spawn(async move {
                s_.send(client.communication(msg_).await.unwrap().into_inner());
            });
        }
        for _ in 0..self.data_clients.len() {
            result.push(recv.recv().await.unwrap());
        }
        return result;
    }

    async fn async_broadcast_commit(&mut self, commit: Msg) {
        for i in 0..self.data_clients.len() {
            let mut client = self.data_clients[i].clone();
            let msg_ = commit.clone();
            tokio::spawn(async move {
                client.communication(msg_).await.unwrap().into_inner();
            });
        }
    }
}
