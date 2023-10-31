use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Seek, Write},
    path::PathBuf,
};

use crate::prelude::*;

use super::{get_index_path, SSTableIndex, SSTableIndexBuilder};

/// Sorted String Table
pub struct SSTableWriter {
    index: SSTableIndex,
    writer: BufWriter<File>,
    offset: u64,
}

impl SSTableWriter {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let index_path = get_index_path(path)?;
        let index = SSTableIndexBuilder::new(index_path).indexs()?.build();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let offset = file.metadata()?.len();
        let writer = BufWriter::new(file);

        Ok(Self {
            index,
            writer,
            offset,
        })
    }

    /// Set Entry to SSTable
    pub fn set(&mut self, entry: &Entry) -> io::Result<&mut Self> {
        entry.write_to(&mut self.writer)?;
        self.index.insert(entry.key.as_slice(), self.offset);
        self.offset += self.writer.stream_position()?;
        Ok(self)
    }

    /// Flush SSTable to the file
    pub fn flush(&mut self) -> Result<&mut Self> {
        self.index.persist()?;
        self.writer.flush()?;
        Ok(self)
    }
}
