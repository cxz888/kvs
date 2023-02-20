use std::path::Path;

use sled::Db;

use crate::{Error, KvsEngine, Result};

/// A sled wrapper to impl `KvsEngine` trait
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Open a directory as db store
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            db: sled::open(path)?,
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let Some(value) = self.db.get(key)? else {
            return Ok(None);
        };
        Ok(Some(std::str::from_utf8(&value)?.to_owned()))
    }
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        // self.db.flush()?;
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(Error::RemoveNonexistKey)?;
        // self.db.flush()?;
        Ok(())
    }
}
