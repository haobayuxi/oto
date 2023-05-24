use common::{ip_addr_add_prefix, Config};
use rpc::common::{
    cto_service_server::{CtoService, CtoServiceServer},
    update_ts_client::UpdateTsClient,
    Echo, Ts,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc::unbounded_channel, Notify, RwLock},
    time::sleep,
};
use tonic::{
    transport::{Channel, Server},
    Response,
};

static TS: AtomicUsize = ATOMIC_USIZE_INIT;
static mut STATUS: Vec<RwLock<CTO_Status>> = Vec::new();

struct update_coordinator {
    clients: Vec<UpdateTsClient<Channel>>,
}

impl update_coordinator {
    pub async fn new(coordinator_addrs: Vec<String>) -> Self {
        let mut clients = Vec::new();
        for i in coordinator_addrs.iter() {
            let ip = ip_addr_add_prefix(i.clone());
            loop {
                match UpdateTsClient::connect(ip.clone()).await {
                    Ok(c_client) => {
                        clients.push(c_client);
                    }
                    Err(_) => sleep(Duration::from_millis(10)).await,
                }
            }
        }
        Self { clients }
    }

    pub async fn run(&mut self) {
        loop {
            sleep(Duration::from_micros(100)).await;
            unsafe {
                // broadcast
                let ts;
                {
                    let mut guard = STATUS[0].write().await;
                    ts = guard.max_ts;
                    guard.notified_max_ts = ts;
                }
                self.broadcast(ts).await;

                // notify
                {
                    let mut guard = STATUS[0].write().await;
                    // loop {
                    //     let a = guard.wait_list.pop_first();
                    //     match a {
                    //         Some((ts, notify)) => {
                    //             if ts <= guard.notified_max_ts {
                    //                 notify.notify_one();
                    //             } else {
                    //                 guard.wait_list.insert(ts, notify);
                    //                 break;
                    //             }
                    //         }
                    //         None => break,
                    //     }
                    // }
                }
            }
        }
    }

    async fn broadcast(&mut self, ts: u64) {
        let (sender, mut recv) = unbounded_channel::<bool>();
        for i in 0..self.clients.len() {
            let mut client = self.clients[i].clone();
            let s_ = sender.clone();
            let msg = Ts { ts };
            tokio::spawn(async move {
                client.update(msg).await.unwrap().into_inner();
                s_.send(true);
            });
        }
        for _ in 0..self.clients.len() {
            recv.recv().await.unwrap();
        }
    }
}

struct CTO_Status {
    max_ts: u64,
    notified_max_ts: u64,
    // wait_list: BTreeMap<u64, Arc<Notify>>,
    wait_list: Vec<Arc<Notify>>,
}

impl CTO_Status {
    pub fn new() -> Self {
        Self {
            max_ts: 0,
            wait_list: Vec::new(),
            notified_max_ts: 0,
        }
    }
}

struct CTO_communication {}

impl CTO_communication {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl CtoService for CTO_communication {
    async fn get_start_ts(
        &self,
        request: tonic::Request<Echo>,
    ) -> Result<tonic::Response<Ts>, tonic::Status> {
        unsafe {
            let ts = TS.load(Ordering::Relaxed) as u64;

            Ok(Response::new(Ts { ts }))
        }
    }

    async fn set_commit_ts(
        &self,
        request: tonic::Request<Ts>,
    ) -> Result<tonic::Response<Echo>, tonic::Status> {
        unsafe {
            let commit_ts = request.into_inner().ts;
            let mut success = true;
            let notify = Arc::new(Notify::new());
            let notify2 = notify.clone();
            {
                let mut guard = STATUS[0].write().await;
                if guard.notified_max_ts > commit_ts {
                    success = false;
                } else {
                    if guard.max_ts < commit_ts {
                        guard.max_ts = commit_ts;
                    }
                    // insert into waitlist
                    println!("maxts = {}", guard.max_ts);
                    guard.wait_list.push(notify);
                }
            }
            // wait
            if success {
                notify2.notified().await;
            }
            Ok(Response::new(Echo { success }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let client_config: CtoConfig = serde_yaml::from_reader(f).unwrap();
    unsafe {
        STATUS.push(RwLock::new(CTO_Status::new()));
    }
    let config = Config::default();
    let addr = config.cto_addr.parse().unwrap();
    let cto = CTO_communication::new();
    println!("CTO listening on {}", addr);

    // tokio::spawn(async move {
    Server::builder()
        .add_service(CtoServiceServer::new(cto))
        .serve(addr)
        .await;
    // });
    loop {
        sleep(Duration::from_micros(100)).await;
        unsafe {
            // broadcast
            // let ts;
            {
                let mut guard = STATUS[0].write().await;
                guard.notified_max_ts = guard.max_ts;

                // notify
                loop {
                    match guard.wait_list.pop() {
                        Some(notify) => {
                            println!("notify");
                            notify.notify_one();
                        }
                        None => break,
                    }
                }
            }

            // for iter in guard.wait_list.it {
            //     iter.notify_one();
            // }
            // let a = guard.wait_list.pop_first();
            // match a {
            //     Some((ts, notify)) => {
            //         if ts <= guard.notified_max_ts {

            //         } else {
            //             guard.wait_list.insert(ts, notify);
            //             break;
            //         }
            //     }
            //     None => break,
            // }
        }
    }
    // let mut client = update_coordinator::new(config.client_addr).await;
    // client.run().await;

    Ok(())
}
