//! error module

use std::str::Utf8Error;

use thiserror::Error;

/// crate-level error
#[derive(Debug, Error)]
pub enum Error {
    /// IO error
    #[error("Unexpected io error")]
    IoError(#[from] std::io::Error),
    /// Call remove on a nonexist key
    #[error("Remove a nonexist key")]
    RemoveNonexistKey,
    /// Error for serializing and deserializing
    #[error("serde error: {0}")]
    SeredError(#[from] serde_json::Error),
    /// Error for sled
    #[error("sled error: {0}")]
    SledError(#[from] sled::Error),
    /// Error for utf-8, when decoded from sled
    #[error("Invalid utf-8 from sled")]
    NonUtf8(#[from] Utf8Error),
    ///
    #[error("Decode error: {0}")]
    DecodeError(String),
    /// Thread pool cannot be zero size
    #[error("Thread pool cannot be zero size")]
    ZeroSizedPool,
    /// Error when build rayon thread pool
    #[error("Rayon error: {0}")]
    RayonError(#[from] rayon::ThreadPoolBuildError),
}

/// crate-level Result type
pub type Result<T, E = Error> = core::result::Result<T, E>;
