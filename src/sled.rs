use std::path::Path;

use sled::Db;

use crate::{Error, KvsEngine, Result, IS_TEST};

/// A sled wrapper to impl `KvsEngine` trait
#[derive(Clone)]
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
    ///
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let Some(value) = self.db.get(key)? else {
            return Ok(None);
        };
        Ok(Some(std::str::from_utf8(&value)?.to_owned()))
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        if IS_TEST {
            self.db.flush()?;
        }
        Ok(())
    }
    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(Error::RemoveNonexistKey)?;
        if IS_TEST {
            self.db.flush()?;
        }
        Ok(())
    }
}
