#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadStruct {
    #[prost(uint64, tag = "1")]
    pub key: u64,
    #[prost(string, optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "3")]
    pub timestamp: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteStruct {
    #[prost(uint64, tag = "1")]
    pub key: u64,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Msg {
    #[prost(uint64, tag = "1")]
    pub txn_id: u64,
    #[prost(message, repeated, tag = "2")]
    pub read_set: ::prost::alloc::vec::Vec<ReadStruct>,
    #[prost(message, repeated, tag = "3")]
    pub write_set: ::prost::alloc::vec::Vec<WriteStruct>,
    #[prost(enumeration = "TxnOp", tag = "5")]
    pub op: i32,
    #[prost(uint32, tag = "6")]
    pub from: u32,
    #[prost(uint64, tag = "7")]
    pub timestamp: u64,
    #[prost(uint64, tag = "8")]
    pub maxts: u64,
    #[prost(enumeration = "TxnType", optional, tag = "9")]
    pub txn_type: ::core::option::Option<i32>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnType {
    TatpGetSubscriberData = 0,
    TatpGetNewDestination = 1,
    TatpGetAccessData = 2,
    TatpUpdateSubscriberData = 3,
    TatpUpdateLocation = 4,
    TatpInsertCallForwarding = 5,
    Ycsb = 6,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnOp {
    ReadOnly = 0,
    Prepare = 1,
    Accept = 2,
    Commit = 3,
    ReadOnlyRes = 4,
    PrepareRes = 5,
    AcceptRes = 6,
    CommitRes = 7,
    Abort = 8,
}
#[doc = r" Generated client implementations."]
pub mod client_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ClientServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClientServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClientServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn propose(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::Msg>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::Msg>>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/common.ClientService/Propose");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for ClientServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ClientServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ClientServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod client_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ClientServiceServer."]
    #[async_trait]
    pub trait ClientService: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Propose method."]
        type ProposeStream: futures_core::Stream<Item = Result<super::Msg, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn propose(
            &self,
            request: tonic::Request<tonic::Streaming<super::Msg>>,
        ) -> Result<tonic::Response<Self::ProposeStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ClientServiceServer<T: ClientService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: ClientService> ClientServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for ClientServiceServer<T>
    where
        T: ClientService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/common.ClientService/Propose" => {
                    #[allow(non_camel_case_types)]
                    struct ProposeSvc<T: ClientService>(pub Arc<T>);
                    impl<T: ClientService> tonic::server::StreamingService<super::Msg> for ProposeSvc<T> {
                        type Response = super::Msg;
                        type ResponseStream = T::ProposeStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::Msg>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).propose(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = ProposeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ClientService> Clone for ClientServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: ClientService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ClientService> tonic::transport::NamedService for ClientServiceServer<T> {
        const NAME: &'static str = "common.ClientService";
    }
}
