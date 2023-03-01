use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use crate::{Decoder, Encoder, Request, Response};

use crate::Result;

///
pub struct KvsClient {
    conn: TcpStream,
    encoder: Encoder,
}

impl KvsClient {
    ///
    pub fn new(addr: SocketAddr) -> Self {
        let conn = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
        log::debug!("{:?}", conn.local_addr());
        let encoder = Encoder::new();
        Self { conn, encoder }
    }
    /// Timeout 2s
    pub fn request(&mut self, request: Request) -> Result<Response> {
        let buf = self.encoder.encode_request(request);
        self.conn.write_all(buf)?;

        let mut decoder = Decoder::new(&mut self.conn);
        decoder.decode_response()
    }
}
