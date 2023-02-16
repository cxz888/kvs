//! An in-memory k-v database
#![feature(seek_stream_len)]
#![deny(missing_docs)]

mod error;

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

pub use crate::error::{Error, Result};

const MAX_DATA_FILE_SIZE: u32 = 0x1000;
const COMPACT_THRESHOLD: u32 = 0x2000;

/// a k-v database, map key to value
pub struct KvStore {
    useless_size: u32,
    curr_dir: PathBuf,
    curr_file_id: u32,
    /// map from key to file id and file offset
    key_dir: HashMap<String, CommandMeta>,
    readers: BTreeMap<u32, BufReader<File>>,
    writer: BufWriter<File>,
}

struct CommandMeta {
    file_id: u32,
    file_offset: u32,
    len: u32,
}

#[derive(Deserialize, Serialize)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

impl KvStore {
    /// open log file and replay it
    #[inline]
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let curr_dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let dir = fs::read_dir(path)?;
        let mut useless_size = 0;
        let mut readers = BTreeMap::new();
        // count the size of value, to get useless_size
        let mut curr_file_id = 0;
        for entry in dir {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            let file_id = file_name.split('.').next().unwrap().parse().unwrap();
            curr_file_id = curr_file_id.max(file_id);
            let read_file = File::open(entry.path())?;

            let reader = BufReader::new(read_file);
            readers.insert(file_id, reader);
        }
        let mut key_dir = HashMap::new();
        for (&file_id, reader) in readers.iter_mut() {
            let mut de = Deserializer::from_reader(reader).into_iter::<Command>();
            let mut file_offset = 0;
            while let Some(command) = de.next() {
                let command = command?;
                let new_offset = de.byte_offset() as u32;
                match command {
                    Command::Set { key, .. } => {
                        let meta = CommandMeta {
                            file_id,
                            file_offset,
                            len: new_offset - file_offset,
                        };
                        if let Some(CommandMeta { len, .. }) = key_dir.insert(key, meta) {
                            useless_size += len;
                        }
                    }
                    Command::Rm { key } => {
                        if let Some(CommandMeta { len, .. }) = key_dir.remove(&key) {
                            useless_size += len;
                        }
                    }
                };
                file_offset = new_offset;
            }
        }

        let curr_file_path = curr_dir.join(format!("{}.dat", curr_file_id));
        let mut write_file = File::options()
            .append(true)
            .create(true)
            .open(&curr_file_path)?;
        write_file.seek(io::SeekFrom::End(0))?;
        if readers.get(&curr_file_id).is_none() {
            let read_file = File::open(curr_file_path)?;
            readers.insert(curr_file_id, BufReader::new(read_file));
        }

        Ok(Self {
            useless_size,
            curr_dir,
            curr_file_id,
            key_dir,
            readers,
            writer: BufWriter::new(write_file),
        })
    }
    /// create or open a data file, return a reader and a writer
    fn create_new_data_file(&mut self) -> Result<()> {
        self.curr_file_id += 1;
        let curr_file_path = self.curr_dir.join(format!("{}.dat", self.curr_file_id));
        let mut write_file = File::options()
            .append(true)
            .create(true)
            .open(&curr_file_path)?;
        write_file.seek(io::SeekFrom::End(0))?;
        self.writer = BufWriter::new(write_file);

        let read_file = BufReader::new(File::open(curr_file_path)?);
        self.readers.insert(self.curr_file_id, read_file);
        Ok(())
    }
    // set value in the disk
    fn set_impl(&mut self, key: String, value: String) -> Result<()> {
        let mut file_offset = self.writer.stream_position()? as u32;

        let command = Command::Set {
            key: key.clone(),
            value,
        };
        let log = serde_json::to_vec(&command)?;
        assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
        if log.len() as u32 + file_offset > MAX_DATA_FILE_SIZE {
            self.curr_file_id += 1;
            self.create_new_data_file()?;
            file_offset = 0;
        }
        self.writer.write(&log)?;
        let meta = CommandMeta {
            file_id: self.curr_file_id,
            file_offset,
            len: log.len() as u32,
        };
        if let Some(CommandMeta { len, .. }) = self.key_dir.insert(key.clone(), meta) {
            self.useless_size += len;
        }
        Ok(())
    }
    /// Set the value corresponding to key to `value`,
    /// record its offset and write to disk
    #[inline]
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set_impl(key, value)?;
        if self.useless_size > COMPACT_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    // find value in the disk
    fn get_impl(
        readers: &mut BTreeMap<u32, BufReader<File>>,
        file_id: u32,
        file_offset: u32,
    ) -> Result<String> {
        // in normal condition, the file must have been opened
        let reader = readers.get_mut(&file_id).unwrap();
        reader.seek(io::SeekFrom::Start(file_offset as u64))?;
        let mut de = Deserializer::from_reader(reader).into_iter::<Command>();

        let command = match de.next() {
            Some(command) => command?,
            None => unreachable!(),
        };
        match command {
            Command::Set { value, .. } => Ok(value),
            Command::Rm { .. } => unreachable!(),
        }
    }
    /// get the value the `key` corresponding to
    #[inline]
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let Some(&CommandMeta {
            file_id,
            file_offset,
            ..
        }) = self.key_dir.get(&key) else
        {
            return Ok(None);
        };
        self.writer.flush()?;
        Self::get_impl(&mut self.readers, file_id, file_offset).map(|value| Some(value))
    }
    /// Remove the key, write to log
    #[inline]
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(CommandMeta { len, .. }) = self.key_dir.remove(&key) {
            self.useless_size += len;
        } else {
            return Err(Error::RemoveNonexistKey);
        }
        let command = Command::Rm { key };
        let log = serde_json::to_vec(&command)?;
        assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
        if log.len() as u32 + self.writer.stream_position()? as u32 > MAX_DATA_FILE_SIZE {
            self.curr_file_id += 1;
            self.create_new_data_file()?;
        }
        self.writer.write(&log)?;
        if self.useless_size > COMPACT_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    #[inline]
    fn data_file_path(&self, file_id: u32) -> PathBuf {
        self.curr_dir.join(format!("{}.dat", file_id))
    }
    /// try to begin compacting
    ///
    /// 目前采取最朴素的做法，即：
    ///
    /// 顺序扫描所有键，找到对应的值，追加到末尾
    fn compact(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.curr_file_id += 1;
        self.useless_size = 0;
        let mut readers = std::mem::take(&mut self.readers);
        self.create_new_data_file()?;

        let key_dir = std::mem::take(&mut self.key_dir);
        for (
            key,
            CommandMeta {
                file_id,
                file_offset,
                ..
            },
        ) in key_dir
        {
            let value = Self::get_impl(&mut readers, file_id, file_offset)?;
            self.set_impl(key, value)?;
        }

        for (file_id, _) in readers {
            std::fs::remove_file(self.data_file_path(file_id))?;
        }

        // get enough threshold
        Ok(())
    }
}
