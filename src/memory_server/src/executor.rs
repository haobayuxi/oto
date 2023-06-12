use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use common::{CoordnatorMsg, DtxType};
use rpc::common::Msg;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};

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
    send_commit_to_dep_graph: Sender<u64>,
}

impl Executor {
    pub fn new(
        id: u64,
        recv: UnboundedReceiver<CoordnatorMsg>,
        dtx_type: DtxType,
        sender: Sender<u64>,
    ) -> Self {
        Self {
            id,
            recv,
            dtx_type,
            send_commit_to_dep_graph: sender,
        }
    }
    pub async fn run(&mut self) {
        loop {
            unsafe {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = Msg::default();
                            if self.dtx_type != DtxType::janus {
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
                            } else {
                                // init node
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);
                                println!("cid= {}, index={}", client_id, index);
                                let node = Node::new(coor_msg.msg.clone());
                                TXNS[client_id as usize].push(node);
                                let (success, deps, read_results) = get_deps(coor_msg.msg).await;
                                reply.success = success;
                                reply.deps = deps;
                                reply.read_set = read_results;
                            }

                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Commit => {
                            // update and release the lock
                            let mut reply = Msg::default();
                            if self.dtx_type != DtxType::janus {
                                if self.dtx_type == DtxType::r2pl {
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
                                // send commit txn to dep_graph
                                self.send_commit_to_dep_graph.send(txn_id).await;
                                // }
                            }
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock

                            // let txn_id = coor_msg.msg.txn_id;
                            // let msg = self.txns.remove(&txn_id).unwrap();
                            if self.dtx_type == DtxType::janus {
                                // mark as executed
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);
                                let node = &mut TXNS[client_id as usize][index as usize];
                                node.executed = true;
                                node.committed = true;
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
                            coor_msg.call_back.send(reply);
                        }
                    },
                    None => {}
                }
            }
        }
    }
}
