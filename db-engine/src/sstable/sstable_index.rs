use anyhow::Result;
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{Read, Write},
    path::PathBuf,
};

/// Sorted String Table Index
pub struct SSTableIndex {
    indexs: BTreeMap<Vec<u8>, u64>,
    path: PathBuf,
}

pub struct SSTableIndexBuilder(SSTableIndex);

impl SSTableIndexBuilder {
    /// Load SSTable Index from file
    /// create new file if not exist
    pub fn new(path: PathBuf) -> Self {
        let indexs = BTreeMap::new();
        let index = SSTableIndex { indexs, path };
        Self(index)
    }

    pub fn indexs(mut self) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .open(&self.0.path)?;
        let mut buf = Vec::new();
        let read_size = file.read_to_end(&mut buf)?;
        if read_size > 0 {
            self.0.indexs = bincode::deserialize(&buf)?;
        }

        Ok(self)
    }

    pub fn build(self) -> SSTableIndex {
        self.0
    }
}

impl SSTableIndex {
    /// Insert the Entry Key and SSTable Offset to the SSTable Index
    pub fn insert(&mut self, key: Vec<u8>, offset: u64) {
        self.indexs.insert(key, offset);
    }

    /// Persist the indexs to file
    pub fn persist(&mut self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.path)?;
        let bytes = bincode::serialize(&self.indexs)?;
        file.write_all(&bytes)?;
        Ok(())
    }

    pub fn indexs(&self) -> &BTreeMap<Vec<u8>, u64> {
        &self.indexs
    }
}
