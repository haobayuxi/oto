use chrono::Local;
use rpc::common::Ts;
use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, Msg,
    ReadStruct, TxnOp,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep as STDSleep;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::Duration;
use tonic::transport::Channel;

use crate::DtxType;
use crate::GLOBAL_COMMITTED;
use crate::{ip_addr_add_prefix, CID_LEN};

pub async fn connect_to_peer(data_ip: Vec<String>) -> Vec<DataServiceClient<Channel>> {
    let mut data_clients = Vec::new();
    for iter in data_ip {
        let server_ip = ip_addr_add_prefix(iter);
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
    return data_clients;
}
pub struct DtxCoordinator {
    pub id: u64,
    pub local_ts: Arc<RwLock<u64>>,
    pub dtx_type: DtxType,
    txn_id: u64,
    read_only: bool,
    commit_ts: u64,
    // <shard>
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<Arc<RwLock<ReadStruct>>>,
    read_to_execute: Vec<ReadStruct>,
    write_to_execute: Vec<Arc<RwLock<ReadStruct>>>,
    write_tuple_ts: Vec<u64>,
    // janus
    fast_commit: bool,
    deps: Vec<u64>,

    data_clients: Vec<DataServiceClient<Channel>>,
    // committed: Arc<AtomicU64>,
}

impl DtxCoordinator {
    pub async fn new(
        id: u64,
        local_ts: Arc<RwLock<u64>>,
        dtx_type: DtxType,
        data_ip: Vec<String>,
    ) -> Self {
        // init  data client
        let data_clients = connect_to_peer(data_ip).await;
        Self {
            id,
            local_ts,
            dtx_type,
            txn_id: id << CID_LEN,
            // start_ts: 0,
            commit_ts: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            // cto_client,
            data_clients,
            read_to_execute: Vec::new(),
            write_to_execute: Vec::new(),
            write_tuple_ts: Vec::new(),
            read_only: false,
            fast_commit: true,
            deps: Vec::new(),
            // committed,
        }
    }

    pub async fn tx_begin(&mut self, read_only: bool) {
        // init coordinator
        self.txn_id += 1;
        self.read_set.clear();
        self.write_set.clear();
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        self.write_tuple_ts.clear();
        self.read_only = read_only;
        self.fast_commit = true;
        self.deps.clear();
        self.commit_ts = 0;
        if self.dtx_type == DtxType::rocc {
            self.commit_ts = self.local_ts.read().await.clone();
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
        println!("write set len = {}", write_set.len());
        let mut success = true;
        let mut result = Vec::new();
        let server_id = self.id % 3;
        if self.dtx_type == DtxType::rocc
            || self.dtx_type == DtxType::r2pl
            || self.dtx_type == DtxType::rjanus
            || self.dtx_type == DtxType::spanner
        {
            if self.read_only {
                let read = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_to_execute.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Execute.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                    deps: Vec::new(),
                    read_only: true,
                };
                let client = self.data_clients.get_mut(server_id as usize).unwrap();

                let reply: Msg = client.communication(read).await.unwrap().into_inner();
                success = reply.success;
                result = reply.read_set;
            } else {
                if !self.write_to_execute.is_empty() {
                    println!("write set size = {}", write_set.len());
                    let execute = Msg {
                        txn_id: self.txn_id,
                        read_set: self.read_to_execute.clone(),
                        write_set,
                        op: TxnOp::Execute.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                    };
                    if self.dtx_type == DtxType::spanner {
                        let reply = self.data_clients[2]
                            .communication(execute)
                            .await
                            .unwrap()
                            .into_inner();
                        success = reply.success;
                        result = reply.read_set.clone();
                    } else {
                        let replies = self.sync_broadcast(execute).await;
                        self.deps = replies[0].deps.clone();
                        for i in 0..=2 {
                            if !replies[i].success {
                                success = false;
                            }
                            if replies[i].deps != self.deps {
                                self.fast_commit = false;
                                for iter in replies[i].deps.iter() {
                                    if !self.deps.contains(iter) {
                                        self.deps.push(*iter);
                                    }
                                }
                            }
                        }

                        result = replies[0].read_set.clone();
                    }
                } else {
                    // simple read
                    let read = Msg {
                        txn_id: self.txn_id,
                        read_set: self.read_to_execute.clone(),
                        write_set: Vec::new(),
                        success: true,
                        op: TxnOp::Execute.into(),
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                    };
                    let client = if self.dtx_type == DtxType::spanner {
                        // read lock at leader
                        self.data_clients.get_mut(2 as usize).unwrap()
                    } else {
                        self.data_clients.get_mut(server_id as usize).unwrap()
                    };

                    let reply: Msg = client.communication(read).await.unwrap().into_inner();
                    success = reply.success;
                    result = reply.read_set;
                }
            }
        } else if self.dtx_type == DtxType::ford {
            let (sender, mut recv) = unbounded_channel::<Msg>();
            let need_lock = if !self.write_to_execute.is_empty() {
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
                    ts: Some(self.commit_ts),
                    deps: Vec::new(),
                    read_only: false,
                };
                // lock the primary
                let mut client = self.data_clients.get_mut(2).unwrap().clone();
                tokio::spawn(async move {
                    let reply = client.communication(lock).await.unwrap().into_inner();
                    sender.send(reply);
                });
            }
            if !self.read_to_execute.is_empty() {
                let read = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_to_execute.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Execute.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                    deps: Vec::new(),
                    read_only: false,
                };
                let client: &mut DataServiceClient<Channel> =
                    self.data_clients.get_mut(server_id as usize).unwrap();

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
        } else if self.dtx_type == DtxType::meerkat {
            if !self.read_to_execute.is_empty() {
                let read = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_to_execute.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Execute.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                    deps: Vec::new(),
                    read_only: false,
                };
                let client = self.data_clients.get_mut(server_id as usize).unwrap();

                let reply: Msg = client.communication(read).await.unwrap().into_inner();
                success = reply.success;
                result = reply.read_set;
            }
        } else if self.dtx_type == DtxType::janus {
            let execute = Msg {
                txn_id: self.txn_id,
                read_set: self.read_to_execute.clone(),
                write_set,
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: Vec::new(),
                read_only: false,
            };
            let replies = self.sync_broadcast(execute).await;
            self.deps = replies[0].deps.clone();
            for i in 0..=2 {
                if !replies[i].success {
                    success = false;
                }
                if replies[i].deps != self.deps {
                    self.fast_commit = false;
                    for iter in replies[i].deps.iter() {
                        if !self.deps.contains(iter) {
                            self.deps.push(*iter);
                        }
                    }
                }
            }
            // println!("prepare done {}", self.fast_commit);
            result = replies[0].read_set.clone();
        }

        self.read_set.extend(result.clone());
        self.write_set.extend(self.write_to_execute.clone());
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        return (success, result);
    }
    pub async fn tx_commit(&mut self) -> bool {
        if self.read_only {
            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let mut write_set = Vec::new();
        for iter in self.write_set.iter() {
            write_set.push(iter.read().await.clone());
        }
        if self.dtx_type == DtxType::spanner {
            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            let commit = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set: write_set.clone(),
                op: TxnOp::Commit.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: self.deps.clone(),
                read_only: false,
            };
            let mut client = self.data_clients[2].clone();
            tokio::spawn(async move {
                client.communication(commit).await;
            });
            return true;
        }
        // validate
        self.commit_ts = (Local::now().timestamp_nanos() / 1000) as u64;
        if self.validate().await {
            let commit = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set: write_set.clone(),
                op: TxnOp::Commit.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: self.deps.clone(),
                read_only: false,
            };
            if self.dtx_type == DtxType::ford {
                if !self.write_set.is_empty() {
                    // broadcast to lock the back
                    let lock = Msg {
                        txn_id: self.txn_id,
                        read_set: Vec::new(),
                        write_set: write_set.clone(),
                        op: TxnOp::Execute.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                    };
                    self.sync_broadcast(lock).await;
                }
            } else if self.dtx_type == DtxType::rocc || self.dtx_type == DtxType::r2pl {
                if !self.write_set.is_empty() {
                    let accept = Msg {
                        txn_id: self.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        op: TxnOp::Accept.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                    };
                    self.sync_broadcast(accept).await;
                    STDSleep(Duration::from_micros(1));
                }
            } else if self.dtx_type == DtxType::janus {
                if !self.fast_commit {
                    let accept = Msg {
                        txn_id: self.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        op: TxnOp::Accept.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: self.deps.clone(),
                        read_only: false,
                    };
                    self.sync_broadcast(accept).await;
                }
                self.sync_broadcast(commit).await;
                GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
                return true;
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
        if self.write_set.is_empty() || self.dtx_type == DtxType::rocc {
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
                deps: Vec::new(),
                read_only: false,
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
                deps: Vec::new(),
                read_only: false,
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
        if self.dtx_type == DtxType::r2pl || self.dtx_type == DtxType::janus {
            return true;
        } else if self.dtx_type == DtxType::rocc || self.dtx_type == DtxType::ford {
            if self.read_set.is_empty() {
                // println!("read set is null");
                return true;
            }
            let validate_msg = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set: Vec::new(),
                op: TxnOp::Validate.into(),
                success: true,
                ts: None,
                deps: Vec::new(),
                read_only: false,
            };
            let server_id = self.id % 3;
            let client = self.data_clients.get_mut(server_id as usize).unwrap();
            // let mut aclient = client.clone();
            // // let (sender, recv) = oneshot::channel();
            // let t_msg = validate_msg.clone();
            // // tokio::spawn(async move {
            // //     sender.send(aclient.communication(t_msg).await.unwrap().into_inner());
            // // });
            let reply = client
                .communication(validate_msg)
                .await
                .unwrap()
                .into_inner();

            if !reply.success {
                return false;
            }
            // let r = recv.await.unwrap();
            // if !r.success {
            //     return false;
            // }
            return true;
        } else if self.dtx_type == DtxType::meerkat {
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
                deps: Vec::new(),
                read_only: false,
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
                deps: Vec::new(),
                read_only: false,
            };
            let _reply = self.sync_broadcast(vadilate_msg).await;
            return success;
        }
        return true;
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
