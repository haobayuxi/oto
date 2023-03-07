use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, ReadStruct,
    WriteStruct,
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
    read_set: Vec<ReadStruct>,
    write_set: Vec<WriteStruct>,
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

    pub fn tx_exe(&mut self) -> bool {
        match self.dtx_type {
            DtxType::oto => {
                // get read set
            }
            DtxType::occ => {}
            DtxType::to => {}
        }
        true
    }
    pub fn tx_commit(&mut self) {}

    pub fn add_to_read_set(&mut self) {}

    pub async fn oto_validate(&self) -> bool {
        for iter in self.read_set.iter() {
            if iter.timestamp.unwrap() > self.start_ts {
                return false;
            }
        }
        true
    }
}
