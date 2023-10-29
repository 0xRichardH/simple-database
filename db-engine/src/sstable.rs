use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufWriter, Write},
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

/// Sorted String Table Index
struct SSTableIndex {
    indexs: BTreeMap<Vec<u8>, u64>,
    path: PathBuf,
    writer: BufWriter<File>,
}

impl SSTableIndex {
    /// Load SSTable Index from file
    /// create new file if not exist
    fn new(path: PathBuf) -> Self {
        todo!()
    }

    /// Insert the Entry Key and SSTable Offset to the SSTable Index
    fn insert(&mut self, key: Vec<u8>, offset: u64) {
        todo!()
    }

    /// Flush the Index to file
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
