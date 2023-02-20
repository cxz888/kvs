use std::{
    io::Write,
    net::{SocketAddr, TcpListener},
};

use crate::{Decoder, Encoder, Error, KvsEngine, Request, Response, Result};

/// A server, listening client's command
pub struct KvsServer<E> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// create a server
    pub fn new(engine: E) -> Self {
        Self { engine }
    }
    /// listen on the sepecified addr
    pub fn listen_on(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        let mut encoder = Encoder::new();
        for stream in listener.incoming() {
            let mut tcp_wrtier = stream?;
            let tcp_reader = tcp_wrtier.try_clone()?;
            log::info!("connect to {}", tcp_wrtier.peer_addr()?);
            let mut decoder = Decoder::new(tcp_reader);
            let request = decoder.decode_request()?;
            log::info!("request {:?}", request);
            match request {
                Request::Set(key, value) => {
                    if let Err(e) = self.engine.set(key, value) {
                        log::error!("Internal error: {e}");
                        tcp_wrtier.write_all(encoder.encode_response(Response::Err))?;
                        return Err(e);
                    } else {
                        tcp_wrtier.write_all(encoder.encode_response(Response::Ok))?;
                    }
                }
                Request::Get(key) => match self.engine.get(key) {
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
                Request::Rm(key) => match self.engine.remove(key) {
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
        }

        Ok(())
    }
}
