use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use common::{CoordnatorMsg, DtxType};
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender as OneShotSender;
use tonic::transport::Channel;

use crate::{
    data::{
        get_deps, get_read_set, lock_write_set, release_read_set, releass_locks,
        update_and_release_locks, validate,
    },
    dep_graph::{get_txnid, Node, TXNS},
};

pub struct Executor {
    pub id: u64,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
    dtx_type: DtxType,
    // janus
    send_commit_to_dep_graph: Sender<u64>,
    // spanner
    peer_senders: Vec<DataServiceClient<Channel>>,
}

impl Executor {
    pub fn new(
        id: u64,
        recv: UnboundedReceiver<CoordnatorMsg>,
        dtx_type: DtxType,
        sender: Sender<u64>,
        peer_senders: Vec<DataServiceClient<Channel>>,
    ) -> Self {
        Self {
            id,
            recv,
            dtx_type,
            send_commit_to_dep_graph: sender,
            peer_senders,
        }
    }

    async fn accept(&mut self, msg: Msg, call_back: OneShotSender<Msg>) {
        let data_clients = self.peer_senders.clone();
        tokio::spawn(async move {
            let mut accept = msg.clone();
            accept.op = TxnOp::Accept.into();
            accept.success = true;
            // broadcast lock
            sync_broadcast(accept.clone(), data_clients.clone()).await;
            // commit
            call_back.send(accept.clone());
            accept.op = TxnOp::Commit.into();
            async_broadcast_commit(accept, data_clients).await;
        });
    }

    pub async fn run(&mut self) {
        loop {
            unsafe {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = Msg::default();
                            let ts = coor_msg.msg.ts();
                            if coor_msg.msg.read_only {
                                let (success, read_result) = get_read_set(
                                    coor_msg.msg.read_set.clone(),
                                    ts,
                                    coor_msg.msg.txn_id,
                                    self.dtx_type,
                                )
                                .await;
                                reply.success = success;
                                reply.read_set = read_result;

                                coor_msg.call_back.send(reply);
                            } else {
                                if self.dtx_type == DtxType::janus {
                                    // init node
                                    let txn_id = coor_msg.msg.txn_id;
                                    let (client_id, index) = get_txnid(txn_id);
                                    let node = Node::new(coor_msg.msg.clone());
                                    TXNS[client_id as usize].push(node);
                                    let (success, deps, read_results) =
                                        get_deps(coor_msg.msg).await;
                                    reply.success = success;
                                    reply.deps = deps.clone();
                                    reply.read_set = read_results;

                                    coor_msg.call_back.send(reply);
                                } else if self.dtx_type == DtxType::spanner {
                                    // lock the read set
                                    let (success, read_result) = get_read_set(
                                        coor_msg.msg.read_set.clone(),
                                        ts,
                                        coor_msg.msg.txn_id,
                                        self.dtx_type,
                                    )
                                    .await;
                                    // lock the write set
                                    if !success {
                                        // send back failure
                                        reply.success = false;
                                        coor_msg.call_back.send(reply);
                                        continue;
                                    }
                                    reply.read_set = read_result;
                                    // if !coor_msg.msg.write_set.is_empty() {
                                    //     let success = lock_write_set(
                                    //         coor_msg.msg.write_set.clone(),
                                    //         coor_msg.msg.txn_id,
                                    //     )
                                    //     .await;
                                    //     if success {
                                    //         // lock the backup
                                    //         self.accept(coor_msg.msg, coor_msg.call_back).await;
                                    //     } else {
                                    //         reply.success = false;
                                    //         coor_msg.call_back.send(reply);
                                    //     }
                                    // } else {
                                    //     coor_msg.call_back.send(reply);
                                    // }
                                } else {
                                    // get the data and lock the write set
                                    let ts = coor_msg.msg.ts();
                                    let (success, read_result) = get_read_set(
                                        coor_msg.msg.read_set.clone(),
                                        ts,
                                        coor_msg.msg.txn_id,
                                        self.dtx_type,
                                    )
                                    .await;
                                    if !success {
                                        // send back failure
                                        reply.success = false;
                                        coor_msg.call_back.send(reply);
                                        continue;
                                    }
                                    reply.read_set = read_result;
                                    let success =
                                        lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id)
                                            .await;
                                    reply.success = success;

                                    coor_msg.call_back.send(reply);
                                }
                            }
                        }
                        rpc::common::TxnOp::Commit => {
                            // update and release the lock
                            let mut reply = Msg::default();
                            if self.dtx_type != DtxType::janus {
                                if self.dtx_type == DtxType::r2pl
                                    || self.dtx_type == DtxType::spanner
                                {
                                    release_read_set(
                                        coor_msg.msg.read_set.clone(),
                                        coor_msg.msg.txn_id,
                                    )
                                    .await;
                                }
                                update_and_release_locks(coor_msg.msg, self.dtx_type).await;
                                reply.success = true;
                                coor_msg.call_back.send(reply);
                            } else {
                                // insert callback to node
                                // unsafe {
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);
                                let node = &mut TXNS[client_id as usize][index as usize];
                                node.txn = Some(coor_msg.msg);
                                node.committed = true;
                                node.callback = Some(coor_msg.call_back);
                                // println!("commit cid={},index={}", client_id, index);
                                // send commit txn to dep_graph
                                self.send_commit_to_dep_graph.send(txn_id).await;
                                // }
                            }
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock

                            if self.dtx_type == DtxType::janus {
                                // mark as executed
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);
                                let node = &mut TXNS[client_id as usize][index as usize];
                                node.executed = true;
                                node.committed = true;
                                // println!("abort cid={},index={}", client_id, index);
                            } else {
                                releass_locks(coor_msg.msg, self.dtx_type).await;
                            }
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Validate => {
                            // return read set ts
                            let success = validate(coor_msg.msg, self.dtx_type).await;
                            let mut reply = Msg::default();
                            // println!("validate  = {}", success);
                            reply.success = success;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Accept => {
                            let mut reply = Msg::default();
                            reply.success = true;
                            if self.dtx_type == DtxType::spanner {
                                // lock the write set
                                lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id).await;
                            }
                            coor_msg.call_back.send(reply);
                        }
                    },
                    None => {}
                }
            }
        }
    }
}

async fn sync_broadcast(msg: Msg, data_clients: Vec<DataServiceClient<Channel>>) -> Vec<Msg> {
    let mut result = Vec::new();
    let (sender, mut recv) = unbounded_channel::<Msg>();
    for iter in data_clients.iter() {
        let mut client = iter.clone();
        let s_ = sender.clone();
        let msg_ = msg.clone();
        tokio::spawn(async move {
            s_.send(client.communication(msg_).await.unwrap().into_inner());
        });
    }
    for _ in 0..data_clients.len() {
        result.push(recv.recv().await.unwrap());
    }
    return result;
}

async fn async_broadcast_commit(commit: Msg, data_clients: Vec<DataServiceClient<Channel>>) {
    for iter in data_clients.iter() {
        let mut client = iter.clone();
        let msg_ = commit.clone();
        tokio::spawn(async move {
            client.communication(msg_).await.unwrap().into_inner();
        });
    }
}
