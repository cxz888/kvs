//! error module

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
}

/// crate-level Result type
pub type Result<T, E = Error> = core::result::Result<T, E>;
