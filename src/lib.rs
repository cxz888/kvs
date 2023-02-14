//! An in-memory k-v database
#![deny(missing_docs)]

use std::collections::HashMap;

/// a k-v database, map key to value
pub struct KvStore {
    /// backbone for the database, currently is a hashmap
    data: HashMap<String, String>,
}

impl KvStore {
    /// construct a database based on hashmap
    #[inline]
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
    /// set the value corresponding to key to `value`
    #[inline]
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }
    /// get the value the `key` corresponding to
    #[inline]
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }
    /// # Panics
    ///
    /// Panics if there was no such key.
    #[inline]
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key).unwrap();
    }
}

impl Default for KvStore {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
