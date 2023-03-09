use rpc::common::Msg;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{data::Data, server::CoordnatorMsg};

pub struct Executor {
    pub id: i32,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
    data: Data,
}

impl Executor {
    pub fn new(id: i32, recv: UnboundedReceiver<CoordnatorMsg>, data: Data) -> Self {
        Self { id, recv, data }
    }
    pub async fn run(&mut self) -> ! {
        loop {
            match self.recv.recv().await {
                Some(coor_msg) => match coor_msg.msg.op() {
                    rpc::common::TxnOp::Execute => {
                        let mut reply = Msg::default();
                        // get the data and lock the write set
                        let (success, read_result) =
                            self.data.get_read_set(coor_msg.msg.read_set).await;
                        if success {
                        } else {
                            // send back failure
                            reply.success = false;
                            coor_msg.call_back.send(reply);
                            continue;
                        }
                        reply.success = self
                            .data
                            .lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id)
                            .await;
                        coor_msg.call_back.send(reply);
                    }
                    rpc::common::TxnOp::Commit => {
                        // update and release the lock
                        self.data
                            .update_and_release_locks(coor_msg.msg.write_set)
                            .await;
                        let mut reply = Msg::default();
                        reply.success = true;
                        coor_msg.call_back.send(reply);
                    }
                    rpc::common::TxnOp::Abort => {
                        // release the lock
                        self.data
                            .releass_locks(coor_msg.msg.write_set, coor_msg.msg.txn_id)
                            .await;
                        let mut reply = Msg::default();
                        reply.success = true;
                        coor_msg.call_back.send(reply);
                    }
                    rpc::common::TxnOp::Validate => {
                        // return read set ts
                        let (success, read_result) =
                            self.data.validate_read_set(coor_msg.msg.read_set).await;
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
