use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use common::{get_txnid, CoordnatorMsg, DtxType};
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::{
    sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
use tonic::transport::Channel;

use crate::{
    data::{
        self, delete, get_deps, get_read_only, get_read_set, insert, lock_write_set,
        release_read_set, releass_locks, update_and_release_locks, validate,
    },
    data_server::PEER,
    dep_graph::{Node, TXNS},
};

pub struct Executor {
    pub id: u64,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
    dtx_type: DtxType,
    // janus
    send_commit_to_dep_graph: Sender<u64>,
    // spanner
    // peer_senders: Vec<DataServiceClient<Channel>>,
}

impl Executor {
    pub fn new(
        id: u64,
        recv: UnboundedReceiver<CoordnatorMsg>,
        dtx_type: DtxType,
        sender: Sender<u64>,
        // peer_senders: Vec<DataServiceClient<Channel>>,
    ) -> Self {
        Self {
            id,
            recv,
            dtx_type,
            send_commit_to_dep_graph: sender,
            // peer_senders,
        }
    }

    async fn accept(&mut self, msg: Msg, call_back: OneShotSender<Msg>) {
        unsafe {
            let data_clients = PEER.clone();
            tokio::spawn(async move {
                let mut accept = msg.clone();
                accept.op = TxnOp::Accept.into();
                accept.success = false;
                // broadcast lock
                let start = Instant::now();
                let result = sync_broadcast(accept.clone(), data_clients).await;

                let end_time = start.elapsed().as_millis();
                println!("{}accept{}", accept.txn_id, end_time);
                call_back.send(accept.clone());
            });
        }
    }

    pub async fn run(&mut self) {
        loop {
            unsafe {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = coor_msg.msg.clone();
                            let ts = coor_msg.msg.ts();
                            if coor_msg.msg.read_only
                                && (self.dtx_type == DtxType::r2pl
                                    || self.dtx_type == DtxType::rjanus
                                    || self.dtx_type == DtxType::rocc
                                    || self.dtx_type == DtxType::spanner)
                            {
                                let (success, read_result) =
                                    get_read_only(coor_msg.msg.read_set.clone()).await;
                                reply.success = success;
                                reply.read_set = read_result;

                                coor_msg.call_back.send(reply);
                            } else {
                                if self.dtx_type == DtxType::janus
                                    || self.dtx_type == DtxType::rjanus
                                {
                                    // init node
                                    let txn_id = coor_msg.msg.txn_id;
                                    let (client_id, index) = get_txnid(txn_id);
                                    let mut last_index = TXNS[client_id as usize].len() - 1;
                                    while index > last_index as u64 {
                                        let node = Node::default();
                                        TXNS[client_id as usize].push(node);
                                        last_index += 1;
                                    }
                                    let node = Node::new(coor_msg.msg.clone());
                                    if TXNS[client_id as usize].len() as u64 == index + 1 {
                                        //
                                        TXNS[client_id as usize][index as usize] = node;
                                    } else {
                                        TXNS[client_id as usize].push(node);
                                    }
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
                                        coor_msg.msg.txn_id,
                                        self.dtx_type,
                                    )
                                    .await;
                                    // lock the write set

                                    reply.success = success;
                                    if !success {
                                        // send back failure
                                        reply.success = false;
                                        coor_msg.call_back.send(reply);
                                        continue;
                                    }
                                    reply.read_set = read_result;
                                    if !coor_msg.msg.write_set.is_empty()
                                        || !coor_msg.msg.insert.is_empty()
                                        || !coor_msg.msg.delete.is_empty()
                                    {
                                        let success = lock_write_set(
                                            coor_msg.msg.write_set.clone(),
                                            coor_msg.msg.txn_id,
                                        )
                                        .await;
                                        if success {
                                            // lock the backup
                                            reply.write_set = coor_msg.msg.write_set.clone();
                                            // reply.success = true;
                                            // coor_msg.call_back.send(reply);
                                            self.accept(reply, coor_msg.call_back).await;
                                        } else {
                                            reply.success = false;
                                            coor_msg.call_back.send(reply);
                                        }
                                    } else {
                                        coor_msg.call_back.send(reply);
                                    }
                                } else {
                                    // get the data and lock the write set
                                    let ts = coor_msg.msg.ts();
                                    let (success, read_result) = get_read_set(
                                        coor_msg.msg.read_set.clone(),
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
                            if self.dtx_type == DtxType::janus || self.dtx_type == DtxType::rjanus {
                                // insert callback to node
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);

                                let node = &mut TXNS[client_id as usize][index as usize];
                                node.txn = Some(coor_msg.msg);
                                node.committed = true;
                                node.callback = Some(coor_msg.call_back);
                                // send commit txn to dep_graph
                                self.send_commit_to_dep_graph.send(txn_id).await;

                                // println!("commit cid={},index={}", client_id, index);
                            } else {
                                if self.dtx_type == DtxType::r2pl
                                    || self.dtx_type == DtxType::spanner
                                {
                                    release_read_set(
                                        coor_msg.msg.read_set.clone(),
                                        coor_msg.msg.txn_id,
                                    )
                                    .await;
                                }
                                if self.dtx_type == DtxType::spanner {
                                    let commit = coor_msg.msg.clone();
                                    unsafe {
                                        tokio::spawn(async move {
                                            sync_broadcast(commit, PEER.clone());
                                        });
                                    }
                                }
                                update_and_release_locks(coor_msg.msg.clone(), self.dtx_type).await;
                                // insert & delete
                                insert(coor_msg.msg.insert.clone());
                                delete(coor_msg.msg.delete);
                                reply.success = true;
                                coor_msg.call_back.send(reply);
                            }
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock

                            if self.dtx_type == DtxType::janus || self.dtx_type == DtxType::rjanus {
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
    println!("data client len {}", data_clients.len());
    for _ in 0..data_clients.len() {
        println!("{:?}", recv.recv().await.unwrap());
        // result.push(recv.recv().await.unwrap());
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
