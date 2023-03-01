//! An in-memory k-v database
#![feature(seek_stream_len)]
#![deny(missing_docs)]
#![feature(concat_bytes)]

mod error;
mod kvstore;
mod server;
mod sled;

mod buf_file;
mod client;
/// Thread pool impl
pub mod thread_pool;

use std::{
    io::{self, Read},
    net::TcpStream,
};

const IS_TEST: bool = true;

pub use crate::{
    client::KvsClient,
    error::{Error, Result},
    kvstore::{rwlock, KvStore},
    server::{shutdown, KvsServer},
    sled::SledKvsEngine,
};

/// A key-value engine
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value corresponding to key to `value`,
    fn set(&self, key: String, value: String) -> Result<()>;
    /// get the value the `key` corresponding to
    fn get(&self, key: &str) -> Result<Option<String>>;
    /// Remove the key
    fn remove(&self, key: String) -> Result<()>;
}

///
pub struct Encoder {
    bytes: Vec<u8>,
}

///
#[repr(u8)]
#[derive(Debug)]
pub enum Request {
    ///
    Set(String, String) = 0,
    ///
    Get(String) = 1,
    ///
    Rm(String) = 2,
}

///
#[derive(PartialEq, Debug)]
pub enum Response {
    ///
    Value(String),
    ///
    Ok,
    ///
    NoKey,
    ///
    Err,
}

impl Encoder {
    ///
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }
    ///
    pub fn encode_request(&mut self, request: Request) -> &[u8] {
        self.bytes.clear();
        match request {
            Request::Set(key, value) => {
                self.encode_type(0)
                    .encode_string(&key)
                    .encode_string(&value);
            }
            Request::Get(key) => {
                self.encode_type(1).encode_string(&key);
            }
            Request::Rm(key) => {
                self.encode_type(2).encode_string(&key);
            }
        }
        &self.bytes
    }
    /// encode response to:
    ///
    /// - `Value(value)` -> 0ssssss, s are the bytes of string
    /// - `None` -> 0
    /// - `Err` -> 0xff
    pub fn encode_response(&mut self, response: Response) -> &[u8] {
        self.bytes.clear();
        match response {
            Response::Value(value) => {
                self.bytes.push(0);
                self.encode_string(&value);
            }
            // why `Response::Ok as u8` doesn't compile?
            Response::Ok => self.bytes.push(1),
            Response::NoKey => self.bytes.push(2),
            Response::Err => self.bytes.push(0xff),
        }
        &self.bytes
    }
    fn encode_len(&mut self, len: u32) -> &mut Self {
        let len = u32::to_be_bytes(len);
        self.bytes.extend_from_slice(&len);
        self
    }
    fn encode_type(&mut self, type_: u8) -> &mut Self {
        self.bytes.push(type_);
        self
    }
    fn encode_string(&mut self, s: &str) -> &mut Self {
        self.encode_len(s.len() as u32);
        self.bytes.extend_from_slice(s.as_bytes());
        self
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

///
pub struct Decoder<'a> {
    buf: Vec<u8>,
    reader: io::BufReader<&'a mut TcpStream>,
}

impl<'a> Decoder<'a> {
    ///
    pub fn new(stream: &'a mut TcpStream) -> Self {
        Self {
            buf: Vec::new(),
            reader: io::BufReader::new(stream),
        }
    }
    fn decode_len(&mut self) -> Result<usize> {
        let mut buf = [0; 4];
        if self.reader.read_exact(&mut buf).is_err() {
            return Err(Error::DecodeError("Can't get len".to_string()));
        };
        Ok(u32::from_be_bytes(buf) as usize)
    }
    fn decode_string(&mut self) -> Result<String> {
        let len = self.decode_len()?;
        self.buf.resize(len, 0);
        if self.reader.read_exact(&mut self.buf[0..len]).is_err() {
            return Err(Error::DecodeError("Can't get key".to_string()));
        };
        Ok(std::str::from_utf8(&self.buf[0..len])?.to_owned())
    }
    ///
    pub fn decode_request(&mut self) -> Result<Request> {
        let mut type_ = [0];
        if self.reader.read_exact(&mut type_).is_err() {
            return Err(Error::DecodeError("Type byte nonexists".to_string()));
        };
        match type_[0] {
            // set
            0 => {
                let key = self.decode_string()?;
                let value = self.decode_string()?;
                Ok(Request::Set(key, value))
            }
            // get
            1 => {
                let key = self.decode_string()?;
                Ok(Request::Get(key))
            }
            // remove
            2 => {
                let key = self.decode_string()?;
                Ok(Request::Rm(key))
            }
            t => Err(Error::DecodeError(format!("Wrong type byte: {t}"))),
        }
    }
    ///
    pub fn decode_response(&mut self) -> Result<Response> {
        let mut type_ = [0];
        if let Err(e) = self.reader.read_exact(&mut type_) {
            log::error!("Type byte error: {e}");
            return Err(Error::DecodeError("Type byte nonexists".to_string()));
        };
        match type_[0] {
            0 => {
                let value = self.decode_string()?;
                Ok(Response::Value(value))
            }
            1 => Ok(Response::Ok),
            2 => Ok(Response::NoKey),
            0xff => Ok(Response::Err),
            t => Err(Error::DecodeError(format!("Wrong type byte: {t}"))),
        }
    }
}
