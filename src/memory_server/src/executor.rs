use common::CoordnatorMsg;
use rpc::common::Msg;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::data::{
    get_read_set, lock_write_set, releass_locks, update_and_release_locks, validate_read_set,
};

pub struct Executor {
    pub id: u64,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
}

impl Executor {
    pub fn new(id: u64, recv: UnboundedReceiver<CoordnatorMsg>) -> Self {
        Self { id, recv }
    }
    pub async fn run(&mut self) {
        loop {
            unsafe {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = Msg::default();
                            // get the data and lock the write set
                            let (success, read_result) = get_read_set(coor_msg.msg.read_set).await;
                            if success {
                            } else {
                                // send back failure
                                reply.success = false;
                                coor_msg.call_back.send(reply);
                                continue;
                            }
                            reply.success =
                                lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id).await;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Commit => {
                            // update and release the lock
                            update_and_release_locks(coor_msg.msg.write_set).await;
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock

                            releass_locks(coor_msg.msg.write_set, coor_msg.msg.txn_id).await;
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Validate => {
                            // return read set ts
                            let (success, read_result) =
                                validate_read_set(coor_msg.msg.read_set).await;
                            let mut reply = Msg::default();
                            reply.read_set = read_result;
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
