use std::{
    fs::File,
    io::{self, Read},
};

pub struct BufReader {
    inner: io::BufReader<File>,
    pos: u64,
}

impl BufReader {
    pub fn new(file: File) -> Self {
        Self {
            inner: io::BufReader::new(file),
            pos: 0,
        }
    }
    pub fn seek(&mut self, pos: u64) -> io::Result<()> {
        let offset = pos.wrapping_sub(self.pos) as i64;
        self.inner.seek_relative(offset)?;
        self.pos = pos;
        Ok(())
    }
}

impl Read for BufReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n_read = self.inner.read(buf)?;
        self.pos += n_read as u64;
        Ok(n_read)
    }
}
