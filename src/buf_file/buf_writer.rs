use std::{
    fs::File,
    io::{self, Seek, Write},
    path::Path,
};

use crate::error::Result;

pub struct BufWriter {
    inner: io::BufWriter<File>,
    pos: u64,
}

impl BufWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options().create(true).write(true).open(path)?;
        Ok(Self {
            inner: io::BufWriter::new(file),
            pos: 0,
        })
    }
    pub fn create_new(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Self {
            inner: io::BufWriter::new(file),
            pos: 0,
        })
    }
    pub fn set_file_offset(&mut self, offset: u64) {
        self.pos = offset;
    }
    pub fn file_offset(&self) -> u64 {
        self.pos
    }
}

impl Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n_write = self.inner.write(buf)?;
        self.pos += n_write as u64;
        Ok(n_write)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for BufWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}
