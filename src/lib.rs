//! An in-memory k-v database
#![feature(seek_stream_len)]
#![deny(missing_docs)]
#![feature(concat_bytes)]

mod error;
mod kvstore;
mod server;
mod sled;

use std::{
    io::{BufReader, Read},
    net::TcpStream,
};

pub use crate::{
    error::{Error, Result},
    kvstore::KvStore,
    server::KvsServer,
    sled::SledKvsEngine,
};

/// A key-value engine
pub trait KvsEngine {
    /// Set the value corresponding to key to `value`,
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// get the value the `key` corresponding to
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove the key
    fn remove(&mut self, key: String) -> Result<()>;
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
#[repr(u8)]
pub enum Response {
    ///
    Value(String) = 0,
    ///
    Ok = 1,
    ///
    NoKey = 2,
    ///
    Err = 0xff,
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

///
pub struct Decoder {
    buf: Vec<u8>,
    reader: BufReader<TcpStream>,
}

impl Decoder {
    ///
    pub fn new(stream: TcpStream) -> Self {
        Self {
            buf: Vec::new(),
            reader: BufReader::new(stream),
        }
    }
    fn decode_len(&mut self) -> Result<usize> {
        let mut buf = [0; 4];
        if let Err(_) = self.reader.read_exact(&mut buf) {
            return Err(Error::DecodeError(format!("Can't get len")));
        };
        Ok(u32::from_be_bytes(buf) as usize)
    }
    fn decode_string(&mut self) -> Result<String> {
        let len = self.decode_len()?;
        self.buf.resize(len, 0);
        if let Err(_) = self.reader.read_exact(&mut self.buf[0..len]) {
            return Err(Error::DecodeError(format!("Can't get key")));
        };
        Ok(std::str::from_utf8(&self.buf[0..len])?.to_owned())
    }
    ///
    pub fn decode_request(&mut self) -> Result<Request> {
        let mut type_ = [0];
        if let Err(_) = self.reader.read_exact(&mut type_) {
            return Err(Error::DecodeError(format!("Type byte nonexists")));
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
        if let Err(_) = self.reader.read_exact(&mut type_) {
            return Err(Error::DecodeError(format!("Type byte nonexists")));
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
