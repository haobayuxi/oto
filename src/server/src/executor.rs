use rpc::common::Msg;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct Executor {
    pub id: i32,
    pub sender: UnboundedSender<Msg>,
    pub recv: UnboundedReceiver<Msg>,
}

impl Executor {
    pub async fn run(&mut self) {
        loop {
            match self.recv.recv().await {
                Some(msg) => match msg.op() {
                    rpc::common::TxnOp::Execute => {
                        // get the data and lock the write set
                    }
                    rpc::common::TxnOp::Commit => {
                        // update and release the lock
                    }
                    rpc::common::TxnOp::Abort => {
                        // release the lock
                    }
                },
                None => {}
            }
        }
    }
}
