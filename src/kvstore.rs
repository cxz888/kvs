use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    fs::{self, File},
    io::{self, Read, Seek, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{
    buf_file::{BufReader, BufWriter},
    Error, KvsEngine, Result, IS_TEST,
};

const MAX_DATA_FILE_SIZE: u32 = if IS_TEST { 0x1000 } else { 0x1000000 };
const COMPACT_THRESHOLD: u32 = if IS_TEST { 0x2000 } else { 0x200000 };
/// a k-v database, map key to value
pub struct KvStore {
    readers: Readers,
    /// pack together to use one `Arc`
    shared: Arc<SharedState>,
}

struct SharedState {
    /// For one store, this is immutable
    curr_dir: PathBuf,
    writer: Mutex<Writer>,
    /// Map from key to file id and file offset.
    ///
    /// When writing new log, key dir will be mutated
    key_dir: ArcSwap<DashMap<String, CommandMeta>>,
    /// Increment after compacting, to notify readers to update files
    global_version: AtomicU32,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            readers: Readers {
                local_version: AtomicU32::new(self.readers.local_version.load(Ordering::SeqCst)),
                files: RefCell::new(HashMap::new()),
            },
            shared: Arc::clone(&self.shared),
        }
    }
}

#[derive(Clone, Copy)]
struct CommandMeta {
    file_id: u32,
    /// For this project, offset can't overflow u32.
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let curr_dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;
        File::create(path.as_ref().join("kvs"))?;

        let dir = fs::read_dir(path)?;
        let mut useless_size = 0;

        // Traverse the directory to read all data files and generate key dir.
        let key_dir = DashMap::new();
        let mut curr_file_id = 0;
        let mut data_file_cnt = 0;
        for entry in dir {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            let Ok(file_id) = file_name.split('.').next().unwrap().parse() else {
                continue;
            };
            data_file_cnt += 1;
            curr_file_id = curr_file_id.max(file_id);
            let mut reader = io::BufReader::new(File::open(entry.path())?);
            let mut file_content = String::new();
            reader.read_to_string(&mut file_content).unwrap();
            reader.seek(io::SeekFrom::Start(0)).unwrap();
            let mut de = Deserializer::from_reader(reader).into_iter();
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
                        if let Some((_, CommandMeta { len, .. })) = key_dir.remove(&key) {
                            useless_size += len;
                        }
                    }
                };
                file_offset = new_offset;
            }
        }

        let readers = Readers {
            local_version: AtomicU32::new(0),
            files: RefCell::new(HashMap::with_capacity(data_file_cnt)),
        };

        // Get a writer of the newest data file in the end of the file
        let curr_file_path = curr_dir.join(format!("{curr_file_id}.dat"));
        let mut write_file = BufWriter::create(curr_file_path)?;
        write_file.seek(io::SeekFrom::End(0))?;
        let writer = Mutex::new(Writer {
            curr_file_id,
            useless_size,
            file: write_file,
        });

        Ok(Self {
            readers,
            shared: Arc::new(SharedState {
                curr_dir,
                writer,
                key_dir: ArcSwap::new(Arc::new(key_dir)),
                global_version: AtomicU32::new(0),
            }),
        })
    }

    /// try to begin compacting
    ///
    /// 目前采取最朴素的做法，即：
    ///
    /// 顺序扫描所有键，找到对应的值，追加到末尾
    fn compact(&self, writer: &mut Writer) -> Result<()> {
        writer.file.flush()?;
        writer.create_new_data_file(&self.shared.curr_dir)?;

        let new_key_dir = (**self.shared.key_dir.load()).clone();
        for mut kv_pair in new_key_dir.iter_mut() {
            let value =
                self.readers
                    .read_value(kv_pair.file_id, kv_pair.file_offset, &self.shared)?;
            let command = Command::Set {
                key: kv_pair.key().to_owned(),
                value,
            };
            let meta = writer.append_log(command, &self.shared.curr_dir)?;
            *kv_pair.value_mut() = meta;
        }

        // It's best to follow this order for consistency

        // First delete old file.
        for (file_id, _) in self.readers.files.borrow().deref() {
            fs::remove_file(self.shared.curr_dir.join(format!("{file_id}.dat")))?;
        }
        // If the deleted file was accessed before global_version is upgraded, it will return error.

        // Second upgrade the global_version
        self.shared.global_version.fetch_add(1, Ordering::SeqCst);
        // Seems ok to swap these two steps?

        // Third update the key_dir
        self.shared.key_dir.store(Arc::new(new_key_dir));

        writer.useless_size = 0;

        Ok(())
    }

    ///
    pub fn flush(&self) -> Result<()> {
        self.shared.writer.lock().unwrap().file.flush()?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// get the value the `key` corresponding to
    fn get(&self, key: &str) -> Result<Option<String>> {
        let file_id;
        let file_offset;
        if let Some(meta) = self.shared.key_dir.load().get(key) {
            file_id = meta.file_id;
            file_offset = meta.file_offset;
        } else {
            return Ok(None);
        }
        self.readers
            .read_value(file_id, file_offset, &self.shared)
            .map(Some)
    }
    /// Set the value corresponding to key to `value`
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.shared.writer.lock().unwrap();
        let command = Command::Set {
            key: key.clone(),
            value,
        };
        let meta = writer.append_log(command, &self.shared.curr_dir)?;

        // NOTE: If we removed this key and insert it again, the remove log should also be useless.
        // We need some kind of mechnism to record the remove, such as another dashmap.
        // For now the useless_size is just estimation.
        if let Some(CommandMeta { len, .. }) = self.shared.key_dir.load().insert(key, meta) {
            writer.useless_size += len;
        }
        // Use this to pass test

        if IS_TEST {
            writer.file.flush()?;
        }
        if writer.useless_size > COMPACT_THRESHOLD {
            self.compact(writer.deref_mut())?;
        }
        Ok(())
    }

    /// Remove the key, write to log
    fn remove(&self, key: String) -> Result<()> {
        let mut writer = self.shared.writer.lock().unwrap();
        if let Some((_, CommandMeta { len, .. })) = self.shared.key_dir.load().remove(&key) {
            writer.useless_size += len;
        } else {
            return Err(Error::RemoveNonexistKey);
        }
        let command = Command::Rm { key };
        writer.append_log(command, &self.shared.curr_dir)?;

        // Use this to pass test
        if IS_TEST {
            writer.file.flush()?;
        }
        if writer.useless_size > COMPACT_THRESHOLD {
            self.compact(writer.deref_mut())?;
        }
        Ok(())
    }
}

/// When compacting, readers can still read.
///
/// And after new compacted data files generated,
/// readers should update its files.
struct Readers {
    /// If `global_version` is greater than reader's `local_version`,
    /// clear the files to load new files, and update `local_verion`
    local_version: AtomicU32,
    files: RefCell<HashMap<u32, BufReader>>,
}

impl Readers {
    /// Find value in the disk.
    fn read_value(&self, file_id: u32, file_offset: u32, shared: &SharedState) -> Result<String> {
        let mut files = self.files.borrow_mut();
        {
            let global_version = shared.global_version.load(Ordering::SeqCst);
            if global_version > self.local_version.load(Ordering::SeqCst) {
                files.clear();
                self.local_version.store(global_version, Ordering::SeqCst);
            }
        }
        let reader = match files.entry(file_id) {
            Entry::Vacant(entry) => {
                let file = File::open(shared.curr_dir.join(format!("{file_id}.dat")))?;
                let reader = BufReader::new(file);
                entry.insert(reader)
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

        reader.seek(file_offset as u64)?;
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
}

struct Writer {
    /// When creating new data file, will be mutated
    curr_file_id: u32,
    /// When writing, may be mutated
    useless_size: u32,
    file: BufWriter,
}

impl Writer {
    /// Append write log in the disk, return the log's meta.
    ///
    /// If the data file is full, create new one and increment `curr_file_id`
    fn append_log(&mut self, command: Command, dir: &Path) -> Result<CommandMeta> {
        let mut file_offset = self.file.file_offset() as u32;

        let log = serde_json::to_vec(&command)?;
        assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
        if log.len() as u32 + file_offset > MAX_DATA_FILE_SIZE {
            self.curr_file_id += 1;
            self.create_new_data_file(dir)?;
            file_offset = 0;
        }
        self.file.write_all(&log)?;
        let meta = CommandMeta {
            file_id: self.curr_file_id,
            file_offset,
            len: log.len() as u32,
        };
        Ok(meta)
    }

    /// create or open a data file, return a reader and a writer
    fn create_new_data_file(&mut self, dir: &Path) -> Result<()> {
        self.curr_file_id += 1;
        let curr_file_path = dir.join(format!("{}.dat", self.curr_file_id));
        self.file = BufWriter::create_new(curr_file_path)?;
        self.file.seek(io::SeekFrom::End(0))?;
        self.file.set_file_offset(0);

        Ok(())
    }
}

mod alternative {
    // use std::{
    //     cell::RefCell,
    //     collections::{hash_map::Entry, HashMap},
    //     fs::{self, File},
    //     io::{self, Read, Seek, Write},
    //     ops::DerefMut,
    //     path::{Path, PathBuf},
    //     sync::{
    //         atomic::{AtomicU32, Ordering},
    //         Arc, Mutex,
    //     },
    // };

    // use dashmap::DashMap;
    // use serde::{Deserialize, Serialize};
    // use serde_json::Deserializer;

    // use crate::{
    //     buf_file::{BufReader, BufWriter},
    //     KvsEngine, Result,
    // };

    // const MAX_DATA_FILE_SIZE: u32 = 0x10000;
    // const COMPACT_THRESHOLD: u32 = 0x200000;

    // /// a k-v database, map key to value
    // pub struct KvStore {
    //     readers: Readers,
    //     /// pack context and writer together to use one `Arc`
    //     shared: Arc<SharedState>,
    // }

    // impl KvStore {
    //     fn writer(&self) -> &Mutex<Writer> {
    //         &self.shared.writer
    //     }
    //     fn context(&self) -> &Context {
    //         &self.shared.context
    //     }
    //     fn key_dir(&self) -> &DashMap<String, CommandMeta> {
    //         &self.context().key_dir
    //     }
    // }

    // struct SharedState {
    //     writer: Mutex<Writer>,
    //     /// Some global state
    //     context: Context,
    // }

    // struct Context {
    //     /// For one store, this is immutable
    //     curr_dir: PathBuf,
    //     /// Map from key to file id and file offset.
    //     ///
    //     /// When writing new log, key dir will be mutated
    //     key_dir: DashMap<String, CommandMeta>,
    //     /// Increment after compacting, to notify readers to update files
    //     global_version: AtomicU32,
    //     ///
    //     first_data_file_id:AtomicU32,
    // }

    // impl Clone for KvStore {
    //     fn clone(&self) -> Self {
    //         Self {
    //             readers: Readers {
    //                 local_version: AtomicU32::new(self.readers.local_version.load(Ordering::SeqCst)),
    //                 files: RefCell::new(HashMap::new()),
    //             },
    //             shared: Arc::clone(&self.shared),
    //         }
    //     }
    // }

    // struct CommandMeta {
    //     file_id: u32,
    //     /// For this project, offset can't overflow u32.
    //     file_offset: u32,
    //     len: u32,
    // }

    // #[derive(Deserialize, Serialize)]
    // enum Command {
    //     Set { key: String, value: String },
    //     Rm { key: String },
    // }

    // impl KvStore {
    //     /// open log file and replay it
    //     pub fn open(path: impl AsRef<Path>) -> Result<Self> {
    //         let curr_dir = path.as_ref().to_path_buf();
    //         fs::create_dir_all(&path)?;
    //         File::create(path.as_ref().join("kvs"))?;

    //         let dir = fs::read_dir(path)?;
    //         let mut useless_size = 0;

    //         // Traverse the directory to read all data files and generate key dir.
    //         let key_dir = DashMap::new();
    //         let mut curr_file_id = 0;
    //         let mut data_file_cnt = 0;
    //         for entry in dir {
    //             let entry = entry?;
    //             let file_name = entry.file_name();
    //             let file_name = file_name.to_string_lossy();
    //             let Ok(file_id) = file_name.split('.').next().unwrap().parse() else {
    //                 continue;
    //             };
    //             data_file_cnt += 1;
    //             curr_file_id = curr_file_id.max(file_id);
    //             let reader = io::BufReader::new(File::open(entry.path())?);
    //             let mut de = Deserializer::from_reader(reader).into_iter();
    //             let mut file_offset = 0;
    //             while let Some(command) = de.next() {
    //                 let command = command?;
    //                 let new_offset = de.byte_offset() as u32;
    //                 match command {
    //                     Command::Set { key, .. } => {
    //                         let meta = CommandMeta {
    //                             file_id,
    //                             file_offset,
    //                             len: new_offset - file_offset,
    //                         };
    //                         if let Some(CommandMeta { len, .. }) = key_dir.insert(key, meta) {
    //                             useless_size += len;
    //                         }
    //                     }
    //                     Command::Rm { key } => {
    //                         if let Some((_, CommandMeta { len, .. })) = key_dir.remove(&key) {
    //                             useless_size += len;
    //                         }
    //                     }
    //                 };
    //                 file_offset = new_offset;
    //             }
    //         }

    //         let readers = Readers {
    //             local_version: AtomicU32::new(0),
    //             files: RefCell::new(HashMap::with_capacity(data_file_cnt)),
    //         };

    //         // Get a writer of the newest data file in the end of the file
    //         let curr_file_path = curr_dir.join(format!("{curr_file_id}.dat"));
    //         let mut write_file = BufWriter::create(curr_file_path)?;
    //         write_file.seek(io::SeekFrom::End(0))?;
    //         let writer = Mutex::new(Writer {
    //             curr_file_id,
    //             useless_size,
    //             file: write_file,
    //         });

    //         let context = Context {
    //             curr_dir,
    //             key_dir,
    //             global_version: AtomicU32::new(0),
    //         };

    //         Ok(Self {
    //             readers,
    //             shared: Arc::new(SharedState { writer, context }),
    //         })
    //     }

    //     /// try to begin compacting
    //     ///
    //     /// 目前采取最朴素的做法，即：
    //     ///
    //     /// 顺序扫描所有键，找到对应的值，追加到末尾
    //     fn compact(&self, writer: &mut Writer) -> Result<()> {
    //         let old_ctx=self.context();

    //         let new_ctx=Context {
    //             curr_dir:old_ctx.curr_dir.clone(),
    //             global_version:
    //         };
    //         writer.file.flush()?;
    //         writer.useless_size = 0;
    //         writer.create_new_data_file(ctx)?;

    //         let new_key_dir = DashMap::with_capacity(ctx.key_dir.len());

    //         for kv_pair in &ctx.key_dir {
    //             let value = self
    //                 .readers
    //                 .read_value(kv_pair.file_id, kv_pair.file_offset, ctx)?;
    //             writer.append_log(kv_pair.key().to_owned(), value, ctx);
    //         }

    //         // get enough threshold
    //         Ok(())
    //     }
    // }

    // impl KvsEngine for KvStore {
    //     /// get the value the `key` corresponding to
    //     fn get(&self, key: &str) -> Result<Option<String>> {
    //         let file_id;
    //         let file_offset;
    //         if let Some(meta) = self.key_dir().get(key) {
    //             file_id = meta.file_id;
    //             file_offset = meta.file_offset;
    //         } else {
    //             return Ok(None);
    //         }
    //         self.readers
    //             .read_value(file_id, file_offset, &self.context())
    //             .map(Some)
    //     }
    //     /// Set the value corresponding to key to `value`
    //     fn set(&self, key: String, value: String) -> Result<()> {
    //         let mut writer = self.writer().lock().unwrap();
    //         writer.append_log(key, value, self.context())?;
    //         // Use this to pass test
    //         writer.file.flush()?;
    //         if writer.useless_size > COMPACT_THRESHOLD {
    //             self.compact(writer.deref_mut(), self.context());
    //         }
    //         Ok(())
    //     }
    //     /// Remove the key, write to log
    //     fn remove(&self, key: String) -> Result<()> {
    //         // let mut inner = self.inner.write().unwrap();
    //         // if let Some(CommandMeta { len, .. }) = inner.key_dir.remove(&key) {
    //         //     inner.useless_size += len;
    //         // } else {
    //         //     return Err(Error::RemoveNonexistKey);
    //         // }
    //         // let command = Command::Rm { key };
    //         // let log = serde_json::to_vec(&command)?;
    //         // assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
    //         // if log.len() as u32 + inner.writer.stream_position()? as u32 > MAX_DATA_FILE_SIZE {
    //         //     inner.curr_file_id += 1;
    //         //     inner.create_new_data_file()?;
    //         // }
    //         // inner.writer.write_all(&log)?;
    //         // inner.writer.flush()?;
    //         // if inner.useless_size > COMPACT_THRESHOLD {
    //         //     inner.compact()?;
    //         // }
    //         // Ok(())
    //         todo!()
    //     }
    // }

    // // impl Inner {

    // //     /// set value in the disk
    // //     fn set_impl(&mut self, key: String, value: String) -> Result<()> {
    // //         let mut file_offset = self.writer.stream_position()? as u32;

    // //         let command = Command::Set {
    // //             key: key.clone(),
    // //             value,
    // //         };
    // //         let log = serde_json::to_vec(&command)?;
    // //         assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
    // //         if log.len() as u32 + file_offset > MAX_DATA_FILE_SIZE {
    // //             self.curr_file_id += 1;
    // //             self.create_new_data_file()?;
    // //             file_offset = 0;
    // //         }
    // //         self.writer.write_all(&log)?;
    // //         let meta = CommandMeta {
    // //             file_id: self.curr_file_id,
    // //             file_offset,
    // //             len: log.len() as u32,
    // //         };
    // //         if let Some(CommandMeta { len, .. }) = self.key_dir.insert(key, meta) {
    // //             self.useless_size += len;
    // //         }
    // //         Ok(())
    // //     }
    // //     // find value in the disk
    // //     fn get_impl(
    // //         readers: &BTreeMap<u32, Mutex<BufReader<File>>>,
    // //         file_id: u32,
    // //         file_offset: u32,
    // //     ) -> Result<String> {
    // //         // in normal condition, the file must have been opened
    // //         let reader = readers.get(&file_id).unwrap();
    // //         let mut reader = reader.lock().unwrap();
    // //         reader.seek(io::SeekFrom::Start(file_offset as u64))?;
    // //         let mut de = Deserializer::from_reader(reader.deref_mut()).into_iter::<Command>();

    // //         let command = match de.next() {
    // //             Some(command) => command?,
    // //             None => unreachable!(),
    // //         };
    // //         match command {
    // //             Command::Set { value, .. } => Ok(value),
    // //             Command::Rm { .. } => unreachable!(),
    // //         }
    // //     }
    // //     /// try to begin compacting
    // //     ///
    // //     /// 目前采取最朴素的做法，即：
    // //     ///
    // //     /// 顺序扫描所有键，找到对应的值，追加到末尾
    // //     fn compact(&mut self) -> Result<()> {
    // //         self.writer.flush()?;
    // //         self.curr_file_id += 1;
    // //         self.useless_size = 0;
    // //         let mut readers = std::mem::take(&mut self.readers);
    // //         self.create_new_data_file()?;

    // //         let key_dir = std::mem::take(&mut self.key_dir);
    // //         for (
    // //             key,
    // //             CommandMeta {
    // //                 file_id,
    // //                 file_offset,
    // //                 ..
    // //             },
    // //         ) in key_dir
    // //         {
    // //             let value = Self::get_impl(&mut readers, file_id, file_offset)?;
    // //             self.set_impl(key, value)?;
    // //         }

    // //         for (file_id, _) in readers {
    // //             std::fs::remove_file(self.curr_dir.join(format!("{file_id}.dat")))?;
    // //         }

    // //         // get enough threshold
    // //         Ok(())
    // //     }
    // // }

    // /// When compacting, readers can still read.
    // ///
    // /// And after new compacted data files generated,
    // /// readers should update its files.
    // struct Readers {
    //     /// If `global_version` is greater than reader's `local_version`,
    //     /// clear the files to load new files, and update `local_verion`
    //     local_version: AtomicU32,
    //     files: RefCell<HashMap<u32, BufReader>>,
    // }

    // impl Readers {
    //     /// Find value in the disk.
    //     fn read_value(&self, file_id: u32, file_offset: u32, ctx: &Context) -> Result<String> {
    //         let mut files = self.files.borrow_mut();
    //         {
    //             let global_version = ctx.global_version.load(Ordering::SeqCst);
    //             if global_version > self.local_version.load(Ordering::SeqCst) {
    //                 files.clear();
    //             }
    //         }
    //         let reader = match files.entry(file_id) {
    //             Entry::Vacant(entry) => {
    //                 let file = File::open(ctx.curr_dir.join(format!("{file_id}.dat")))?;
    //                 let reader = BufReader::new(file);
    //                 entry.insert(reader)
    //             }
    //             Entry::Occupied(entry) => entry.into_mut(),
    //         };

    //         reader.seek(io::SeekFrom::Start(file_offset as u64))?;
    //         let mut de = Deserializer::from_reader(reader).into_iter::<Command>();

    //         let command = match de.next() {
    //             Some(command) => command?,
    //             None => unreachable!(),
    //         };
    //         match command {
    //             Command::Set { value, .. } => Ok(value),
    //             Command::Rm { .. } => unreachable!(),
    //         }
    //     }
    // }

    // struct Writer {
    //     /// When creating new data file, will be mutated
    //     curr_file_id: u32,
    //     /// When writing, may be mutated
    //     useless_size: u32,
    //     file: BufWriter,
    // }

    // impl Writer {
    //     /// set value in the disk
    //     fn append_log(&mut self, key: String, value: String, ctx: &Context) -> Result<()> {
    //         let mut file_offset = self.file.file_offset() as u32;

    //         let command = Command::Set {
    //             key: key.clone(),
    //             value,
    //         };
    //         let log = serde_json::to_vec(&command)?;
    //         assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
    //         if log.len() as u32 + file_offset > MAX_DATA_FILE_SIZE {
    //             self.curr_file_id += 1;
    //             self.create_new_data_file(ctx)?;
    //             file_offset = 0;
    //         }
    //         self.file.write_all(&log)?;
    //         let meta = CommandMeta {
    //             file_id: self.curr_file_id,
    //             file_offset,
    //             len: log.len() as u32,
    //         };
    //         if let Some(CommandMeta { len, .. }) = ctx.key_dir.insert(key, meta) {
    //             self.useless_size += len;
    //         }
    //         Ok(())
    //     }

    //     /// create or open a data file, return a reader and a writer
    //     fn create_new_data_file(&mut self, ctx: &Context) -> Result<()> {
    //         self.curr_file_id += 1;
    //         let curr_file_path = ctx.curr_dir.join(format!("{}.dat", self.curr_file_id));
    //         self.file = BufWriter::create_new(curr_file_path)?;
    //         self.file.seek(io::SeekFrom::End(0))?;

    //         Ok(())
    //     }
    // }
}

///
pub mod rwlock {
    use std::{
        collections::{BTreeMap, HashMap},
        fs::{self, File},
        io::{self, BufReader, BufWriter, Seek, Write},
        ops::DerefMut,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, RwLock},
    };

    use serde::{Deserialize, Serialize};
    use serde_json::Deserializer;

    use crate::{Error, KvsEngine, Result, IS_TEST};

    const MAX_DATA_FILE_SIZE: u32 = if IS_TEST { 0x1000 } else { 0x10000 };
    const COMPACT_THRESHOLD: u32 = if IS_TEST { 0x2000 } else { 0x200000 };

    /// a k-v database, map key to value
    #[derive(Clone)]
    pub struct KvStore {
        inner: Arc<RwLock<Inner>>,
    }

    struct Inner {
        useless_size: u32,
        curr_dir: PathBuf,
        curr_file_id: u32,
        /// map from key to file id and file offset
        key_dir: HashMap<String, CommandMeta>,
        readers: BTreeMap<u32, Mutex<BufReader<File>>>,
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
        pub fn open(path: impl AsRef<Path>) -> Result<Self> {
            let curr_dir = path.as_ref().to_path_buf();
            fs::create_dir_all(&path)?;
            File::create(path.as_ref().join("kvs"))?;

            let dir = fs::read_dir(path)?;
            let mut useless_size = 0;
            let mut readers = BTreeMap::new();
            // count the size of value, to get useless_size
            let mut curr_file_id = 0;
            for entry in dir {
                let entry = entry?;
                let file_name = entry.file_name();
                let file_name = file_name.to_string_lossy();
                let Ok(file_id) = file_name.split('.').next().unwrap().parse() else {
                    continue;
                };
                curr_file_id = curr_file_id.max(file_id);
                let read_file = File::open(entry.path())?;

                let reader = BufReader::new(read_file);
                readers.insert(file_id, Mutex::new(reader));
            }
            let mut key_dir = HashMap::new();
            for (&file_id, reader) in readers.iter_mut() {
                let mut reader = reader.lock().unwrap();
                let mut de = Deserializer::from_reader(reader.deref_mut()).into_iter();
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

            let curr_file_path = curr_dir.join(format!("{curr_file_id}.dat"));
            let mut write_file = File::options()
                .append(true)
                .create(true)
                .open(&curr_file_path)?;
            write_file.seek(io::SeekFrom::End(0))?;
            if readers.get(&curr_file_id).is_none() {
                let read_file = File::open(curr_file_path)?;
                readers.insert(curr_file_id, Mutex::new(BufReader::new(read_file)));
            }

            let inner = Inner {
                useless_size,
                curr_dir,
                curr_file_id,
                key_dir,
                readers,
                writer: BufWriter::new(write_file),
            };

            Ok(Self {
                inner: Arc::new(RwLock::new(inner)),
            })
        }
        ///
        pub fn flush(&self) -> Result<()> {
            self.inner.write().unwrap().writer.flush()?;
            Ok(())
        }
    }

    impl Inner {
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
            self.readers
                .insert(self.curr_file_id, Mutex::new(read_file));
            Ok(())
        }
        /// set value in the disk
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
            self.writer.write_all(&log)?;
            let meta = CommandMeta {
                file_id: self.curr_file_id,
                file_offset,
                len: log.len() as u32,
            };
            if let Some(CommandMeta { len, .. }) = self.key_dir.insert(key, meta) {
                self.useless_size += len;
            }
            Ok(())
        }
        // find value in the disk
        fn get_impl(
            readers: &BTreeMap<u32, Mutex<BufReader<File>>>,
            file_id: u32,
            file_offset: u32,
        ) -> Result<String> {
            // in normal condition, the file must have been opened
            let mut reader = readers[&file_id].lock().unwrap();
            reader.seek(io::SeekFrom::Start(file_offset as u64))?;
            let mut de = Deserializer::from_reader(reader.deref_mut()).into_iter::<Command>();

            let command = match de.next() {
                Some(command) => command?,
                None => unreachable!(),
            };
            match command {
                Command::Set { value, .. } => Ok(value),
                Command::Rm { .. } => unreachable!(),
            }
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
                std::fs::remove_file(self.curr_dir.join(format!("{file_id}.dat")))?;
            }

            // get enough threshold
            Ok(())
        }
    }

    impl KvsEngine for KvStore {
        /// get the value the `key` corresponding to
        fn get(&self, key: &str) -> Result<Option<String>> {
            let inner = self.inner.read().unwrap();
            let Some(&CommandMeta {
                file_id,
                file_offset,
                ..
            }) = inner.key_dir.get(key) else
            {
                return Ok(None);
            };
            Inner::get_impl(&inner.readers, file_id, file_offset).map(Some)
        }
        /// Set the value corresponding to key to `value`
        fn set(&self, key: String, value: String) -> Result<()> {
            let mut inner = self.inner.write().unwrap();
            inner.set_impl(key, value)?;
            if IS_TEST {
                inner.writer.flush()?;
            }
            if inner.useless_size > COMPACT_THRESHOLD {
                inner.compact()?;
            }
            Ok(())
        }
        /// Remove the key, write to log
        fn remove(&self, key: String) -> Result<()> {
            let mut inner = self.inner.write().unwrap();
            if let Some(CommandMeta { len, .. }) = inner.key_dir.remove(&key) {
                inner.useless_size += len;
            } else {
                return Err(Error::RemoveNonexistKey);
            }
            let command = Command::Rm { key };
            let log = serde_json::to_vec(&command)?;
            assert!(log.len() as u32 <= MAX_DATA_FILE_SIZE);
            if log.len() as u32 + inner.writer.stream_position()? as u32 > MAX_DATA_FILE_SIZE {
                inner.curr_file_id += 1;
                inner.create_new_data_file()?;
            }
            inner.writer.write_all(&log)?;
            if IS_TEST {
                inner.writer.flush()?;
            }
            if inner.useless_size > COMPACT_THRESHOLD {
                inner.compact()?;
            }
            Ok(())
        }
    }
}
