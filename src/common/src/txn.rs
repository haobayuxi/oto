use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, Msg,
    ReadStruct, TxnOp, WriteStruct,
};
use std::{any::Any, convert::TryInto, sync::Arc};
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub enum DtxType {
    oto,
    occ,
    to,
}
pub struct DtxCoordinator {
    pub local_ts: Arc<RwLock<u64>>,
    pub dtx_type: DtxType,
    txn_id: u64,
    start_ts: u64,
    commit_ts: u64,
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<WriteStruct>,
    cto_client: CtoServiceClient<Channel>,
    data_client: DataServiceClient<Channel>,
}

impl DtxCoordinator {
    pub fn new(
        local_ts: Arc<RwLock<u64>>,
        dtx_type: DtxType,
        txn_id: u64,
        cto_client: CtoServiceClient<Channel>,
        data_client: DataServiceClient<Channel>,
    ) -> Self {
        Self {
            local_ts,
            dtx_type,
            txn_id,
            start_ts: 0,
            commit_ts: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            cto_client,
            data_client,
        }
    }
    pub async fn tx_begin(&mut self) {
        match self.dtx_type {
            DtxType::oto => {
                // get start ts from local
                self.start_ts = self.local_ts.read().await.clone();
            }
            DtxType::occ => {}
            DtxType::to => {
                // get start ts from cto
                let reply = self.cto_client.get_start_ts(Echo::default()).await.unwrap();
                self.start_ts = reply.into_inner().ts;
            }
        }
    }

    pub async fn tx_exe(&mut self) -> bool {
        if (self.read_set.is_empty() && self.write_set.is_empty()) {
            return true;
        }

        match self.dtx_type {
            DtxType::oto => {
                // get read set
            }
            DtxType::occ => {}
            DtxType::to => {}
        }
        true
    }
    pub async fn tx_commit(&mut self) -> bool {
        let mut commit = Msg {
            txn_id: self.txn_id,
            read_set: Vec::new(),
            write_set: self.write_set.clone(),
            op: TxnOp::Commit.into(),
            success: true,
            commit_ts: Some(self.commit_ts),
        };
        let reply = self
            .data_client
            .communication(commit)
            .await
            .unwrap()
            .into_inner();
        return reply.success;
    }

    pub async fn tx_abort(&mut self) {
        if self.write_set.is_empty() {
            return;
        }
        let mut abort = Msg {
            txn_id: self.txn_id,
            read_set: Vec::new(),
            write_set: self.write_set.clone(),
            op: TxnOp::Abort.into(),
            success: true,
            commit_ts: Some(self.commit_ts),
        };
        let reply = self
            .data_client
            .communication(abort)
            .await
            .unwrap()
            .into_inner();
    }

    pub fn add_to_read_set(&mut self, key: u64, table_id: i32) {
        let read_struct = ReadStruct {
            key,
            table_id,
            value: None,
            timestamp: None,
        };
        self.read_set.push(read_struct);
    }

    pub async fn validate(&self) -> bool {
        true
    }

    fn oto_validate(&self) -> bool {
        for iter in self.read_set.iter() {
            if iter.timestamp.unwrap() > self.start_ts {
                return false;
            }
        }
        true
    }
}
