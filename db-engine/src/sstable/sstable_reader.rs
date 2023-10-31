use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Seek},
    path::PathBuf,
};

use crate::prelude::*;

use super::{get_index_path, SSTableIndex, SSTableIndexBuilder};

/// Sorted String Table
pub struct SSTableReader {
    index: SSTableIndex,
    reader: BufReader<File>,
}

impl SSTableReader {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let index_path = get_index_path(path)?;
        let index = SSTableIndexBuilder::new(index_path).indexs()?.build();

        let file = OpenOptions::new().write(true).read(true).open(path)?;
        let reader = BufReader::new(file);

        Ok(Self { index, reader })
    }

    /// Get Entry from SSTable file
    pub fn get(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(offset) = self.index.get(key) {
            self.reader.seek(io::SeekFrom::Start(*offset)).ok()?;
            return Entry::read_from(&mut self.reader);
        }

        None
    }
}
