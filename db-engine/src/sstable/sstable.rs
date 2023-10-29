use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
};

use crate::prelude::*;

use super::sstable_index::SSTableIndex;

/// Sorted String Table
pub struct SSTable {
    index: SSTableIndex,
    path: PathBuf,
    writer: BufWriter<File>,
}

impl SSTable {
    pub fn new(path: PathBuf) -> Self {
        todo!()
    }

    // TODO
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        todo!()
    }

    pub fn set(&self, key: &[u8], value: Option<&[u8]>, timestamp: u128) -> io::Result<()> {
        todo!()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
