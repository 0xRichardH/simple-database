mod sstable_index;

pub use self::sstable_index::*;

use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use crate::prelude::*;

/// Sorted String Table
pub struct SSTable {
    index: SSTableIndex,
    path: PathBuf,
    writer: BufWriter<File>,
}

impl SSTable {
    pub fn new(path: PathBuf) -> Result<Self> {
        let index_path = path.join("idx");
        let index = SSTableIndexBuilder::new(index_path).indexs()?.build();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            index,
            path,
            writer,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if let Some(offset) = self.index.get(key) {
            let file = OpenOptions::new().read(true).open(&self.path).ok()?;
            let mut reader = BufReader::new(file);
            reader.seek(io::SeekFrom::Start(*offset)).ok()?;
            return Entry::read_from(&mut reader);
        }

        None
    }

    pub fn set(&mut self, entry: &Entry) -> io::Result<()> {
        let offset = self.writer.stream_position()?;
        entry.write_to(&mut self.writer)?;
        self.index.insert(entry.key.as_slice(), offset);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.index.persist()?;
        self.writer.flush()?;
        Ok(())
    }
}
