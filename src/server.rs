use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{
    thread_pool::ThreadPool, Decoder, Encoder, Error, KvsEngine, Request, Response, Result,
};

/// A server, listening client's command
pub struct KvsServer<E, P> {
    engine: E,
    pool: P,
    shutdown: Arc<AtomicBool>,
}
/// shutdown the server listening on `addr`, using signal `shutdown`
pub fn shutdown(addr: SocketAddr, shutdown: Arc<AtomicBool>) {
    shutdown.store(true, Ordering::SeqCst);
    TcpStream::connect(addr).unwrap();
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// create a server
    pub fn new(engine: E, shutdown: Arc<AtomicBool>, n_threads: usize) -> Self {
        let pool = P::new(n_threads as u32).unwrap();
        Self {
            engine,
            pool,
            shutdown,
        }
    }
    fn handle_stream(engine: E, stream: TcpStream) -> Result<()> {
        let mut tcp_wrtier = stream;
        let mut tcp_reader = tcp_wrtier.try_clone()?;
        log::info!("connect to {}", tcp_wrtier.peer_addr()?);
        let mut decoder = Decoder::new(&mut tcp_reader);
        let request = decoder.decode_request()?;
        log::info!("request {:?}", request);
        let mut encoder = Encoder::new();
        match request {
            Request::Set(key, value) => {
                if let Err(e) = engine.set(key, value) {
                    log::error!("Internal error: {e}");
                    tcp_wrtier.write_all(encoder.encode_response(Response::Err))?;
                    return Err(e);
                } else {
                    tcp_wrtier.write_all(encoder.encode_response(Response::Ok))?;
                }
            }
            Request::Get(key) => match engine.get(&key) {
                Ok(Some(value)) => {
                    tcp_wrtier.write_all(encoder.encode_response(Response::Value(value)))?;
                }
                Ok(None) => {
                    tcp_wrtier.write_all(encoder.encode_response(Response::NoKey))?;
                }
                Err(e) => {
                    log::error!("Internal error: {e}");
                    tcp_wrtier.write_all(encoder.encode_response(Response::Err))?;
                    return Err(e);
                }
            },
            Request::Rm(key) => match engine.remove(key) {
                Ok(_) => tcp_wrtier.write_all(encoder.encode_response(Response::Ok))?,
                Err(Error::RemoveNonexistKey) => {
                    tcp_wrtier.write_all(encoder.encode_response(Response::NoKey))?;
                }
                Err(e) => {
                    log::error!("Internal error: {e}");
                    tcp_wrtier.write_all(encoder.encode_response(Response::Err))?;
                    return Err(e);
                }
            },
        }
        log::info!("Send response");
        Ok(())
    }
    /// listen on the sepecified addr
    pub fn listen_on(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }
            let stream = stream?;
            let engine = self.engine.clone();
            self.pool.spawn(move || {
                Self::handle_stream(engine, stream).unwrap();
            });
        }
        Ok(())
    }
}
