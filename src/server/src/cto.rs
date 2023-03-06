

pub struct CTO {
    ts: u64,
}

impl CTO {
    pub new()->Self {
        Self{
            ts:0,
        }
    }
}

pub async fn run_propose_server(propose_server: ProposeServer) {
    let addr = propose_server.addr_to_listen.parse().unwrap();

    tracing::info!("propose server listening on: {:?}", addr);

    let server = ClientServiceServer::new(propose_server);

    match Server::builder().add_service(server).serve(addr).await {
        Ok(_) => tracing::info!("propose rpc server start done"),
        Err(e) => panic!("propose rpc server start fail {}", e),
    }
}