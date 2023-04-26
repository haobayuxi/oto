use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use common::{CoordnatorMsg, DtxType};
use rpc::common::Msg;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::data::{
    get_read_set, lock_write_set, releass_locks, update_and_release_locks, validate_read_set,
};

pub struct Executor {
    pub id: u64,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
    dtx_type: DtxType,
    // read_only_committed: Arc<AtomicU64>,
    // read_write_committed: Arc<AtomicU64>,
}

impl Executor {
    pub fn new(
        id: u64,
        recv: UnboundedReceiver<CoordnatorMsg>,
        dtx_type: DtxType,
        // read_only_committed: Arc<AtomicU64>,
        // read_write_committed: Arc<AtomicU64>,
    ) -> Self {
        Self {
            id,
            recv,
            dtx_type,
            // read_only_committed,
            // read_write_committed,
        }
    }
    pub async fn run(&mut self) {
        loop {
            unsafe {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = Msg::default();
                            // get the data and lock the write set
                            let start_ts = coor_msg.msg.ts();
                            let (success, read_result) =
                                get_read_set(coor_msg.msg.read_set, start_ts, self.dtx_type).await;
                            if !success {
                                // send back failure
                                reply.success = false;
                                coor_msg.call_back.send(reply);
                                continue;
                            }
                            reply.read_set = read_result;
                            // println!()
                            reply.success =
                                lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id).await;
                            // if coor_msg.msg.write_set.len() != 0 {
                            //     self.txns.insert(coor_msg.msg.txn_id, coor_msg.msg);
                            // }
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Commit => {
                            // update and release the lock
                            // let txn_id = coor_msg.msg.txn_id;
                            // let msg = self.txns.remove(&txn_id).unwrap();
                            update_and_release_locks(coor_msg.msg, self.dtx_type).await;
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock

                            // let txn_id = coor_msg.msg.txn_id;
                            // let msg = self.txns.remove(&txn_id).unwrap();
                            releass_locks(coor_msg.msg, self.dtx_type).await;
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Validate => {
                            // return read set ts
                            let success = validate_read_set(coor_msg.msg, self.dtx_type).await;
                            let mut reply = Msg::default();
                            println!("validate  = {}", success);
                            reply.success = success;
                            coor_msg.call_back.send(reply);
                        }
                    },
                    None => {}
                }
            }
        }
    }
}
