use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, Msg,
    ReadStruct, TxnOp, WriteStruct,
};
use serde::Deserialize;
use std::{any::Any, convert::TryInto, sync::Arc};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::Duration;
use tonic::transport::Channel;

use crate::DtxType;

async fn init_coordinator_rpc(
    cto_ip: String,
    data_ip: String,
) -> (CtoServiceClient<Channel>, DataServiceClient<Channel>) {
    loop {
        match CtoServiceClient::connect(cto_ip.clone()).await {
            Ok(cto_client) => loop {
                match DataServiceClient::connect(data_ip.clone()).await {
                    Ok(data_client) => return (cto_client, data_client),
                    Err(_) => sleep(Duration::from_millis(10)).await,
                }
            },
            Err(_) => sleep(Duration::from_millis(10)).await,
        }
    }
}
pub struct DtxCoordinator {
    pub id: u64,
    pub local_ts: Arc<RwLock<u64>>,
    pub dtx_type: DtxType,
    txn_id: u64,
    start_ts: u64,
    commit_ts: u64,
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<WriteStruct>,
    read_to_execute: Vec<ReadStruct>,
    write_to_execute: Vec<WriteStruct>,
    cto_client: CtoServiceClient<Channel>,
    data_client: DataServiceClient<Channel>,
}

impl DtxCoordinator {
    pub async fn new(
        id: u64,
        local_ts: Arc<RwLock<u64>>,
        dtx_type: DtxType,
        cto_ip: String,
        data_ip: String,
    ) -> Self {
        // init cto client & data client
        let (cto_client, data_client) = init_coordinator_rpc(cto_ip, data_ip).await;
        Self {
            id,
            local_ts,
            dtx_type,
            txn_id: id << 40,
            start_ts: 0,
            commit_ts: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            cto_client,
            data_client,
            read_to_execute: Vec::new(),
            write_to_execute: Vec::new(),
        }
    }
    pub async fn tx_begin(&mut self) {
        // init coordinator
        self.commit_ts = 0;
        self.txn_id += 1;
        self.read_set.clear();
        self.write_set.clear();
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        self.start_ts = 0;
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
        if self.read_to_execute.is_empty() && self.write_to_execute.is_empty() {
            return true;
        }
        let exe_msg = Msg {
            txn_id: self.txn_id,
            read_set: self.read_to_execute.clone(),
            write_set: self.write_to_execute.clone(),
            op: TxnOp::Execute.into(),
            success: true,
            commit_ts: None,
        };
        let reply = self
            .data_client
            .communication(exe_msg)
            .await
            .unwrap()
            .into_inner();
        for iter in reply.read_set.iter() {
            let a = iter.timestamp.unwrap();
        }
        self.read_set.extend(reply.read_set);
        self.write_set.extend(reply.write_set);
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        return reply.success;
    }
    pub async fn tx_commit(&mut self) -> bool {
        // validate
        if self.validate().await {
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
            return true;
        } else {
            self.tx_abort().await;
            return false;
        }
    }

    pub async fn tx_abort(&mut self) {
        if self.write_set.is_empty() {
            return;
        }
        let abort = Msg {
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

    pub fn add_read_to_execute(&mut self, key: u64, table_id: i32) {
        let read_struct = ReadStruct {
            key,
            table_id,
            value: None,
            timestamp: None,
        };
        self.read_to_execute.push(read_struct);
    }

    pub fn add_write_to_execute(&mut self, key: u64, table_id: i32, value: String) {
        let write_struct = WriteStruct {
            key,
            table_id,
            value: Some(value),
        };
        self.write_to_execute.push(write_struct);
    }

    async fn validate(&mut self) -> bool {
        match self.dtx_type {
            DtxType::oto => {
                return self.oto_validate().await;
            }
            DtxType::occ => {
                if self.read_set.is_empty() {
                    return true;
                }
                let vadilate_msg = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_set.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Validate.into(),
                    success: true,
                    commit_ts: None,
                };
                let reply = self
                    .data_client
                    .communication(vadilate_msg)
                    .await
                    .unwrap()
                    .into_inner();
                for i in 0..self.read_set.iter().len() {
                    if self.read_set[i].timestamp() != reply.read_set[i].timestamp() {
                        return false;
                    }
                }
                return true;
            }
            DtxType::to => return true,
        }
    }

    async fn oto_validate(&self) -> bool {
        let mut max_tx = self.start_ts;
        for iter in self.read_set.iter() {
            if iter.timestamp.unwrap() > self.start_ts {
                max_tx = iter.timestamp.unwrap();
            }
        }
        if max_tx > self.start_ts {
            // update local ts
            let mut guard = self.local_ts.write().await;
            *guard = max_tx;
            return false;
        }
        true
    }
}
