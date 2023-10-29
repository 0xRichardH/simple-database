use anyhow::{Context, Result};
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
        dbg!(&self.0.path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.0.path)
            .context("open idx file to read")?;
        let mut buf = Vec::new();
        let read_size = file
            .read_to_end(&mut buf)
            .context("read content from idx")?;
        if read_size > 0 {
            self.0.indexs = bincode::deserialize(&buf).context("deserialize idx to BTreeMap")?;
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
            .open(&self.path)
            .context("open idx file to write")?;
        let bytes = bincode::serialize(&self.indexs).context("serialize idx to bytes")?;
        file.write_all(&bytes).context("write idx bytes to file")?;
        Ok(())
    }

    pub fn indexs(&self) -> &BTreeMap<Vec<u8>, u64> {
        &self.indexs
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn it_works() -> Result<()> {
        let temp_dir = TempDir::new("sstable_index")?;
        let path = temp_dir.path().join("sstable_index.idx");

        // create SSTableIndex
        let mut idx = SSTableIndexBuilder::new(path.clone()).indexs()?.build();
        assert_eq!(idx.indexs().len(), 0);
        idx.insert(b"hello".to_vec(), 1);
        assert_eq!(idx.indexs().len(), 1);
        assert_eq!(idx.indexs().get(&b"hello".to_vec()), Some(&1));

        // persist to file
        idx.persist()?;

        // load from file
        let mut idx_2 = SSTableIndexBuilder::new(path.clone()).indexs()?.build();
        assert_eq!(idx_2.indexs().len(), 1);
        assert_eq!(idx_2.indexs().get(&b"hello".to_vec()), Some(&1));

        // inesert new key and value
        idx_2.insert(b"world".to_vec(), 2);
        assert_eq!(idx.indexs().len(), 1);
        assert_eq!(idx_2.indexs().len(), 2);
        assert_eq!(idx_2.indexs().get(&b"world".to_vec()), Some(&2));

        // persist to file
        idx_2.persist()?;
        let idx_3 = SSTableIndexBuilder::new(path).indexs()?.build();
        assert_eq!(idx_3.indexs().len(), 2);

        temp_dir.close().unwrap();
        Ok(())
    }
}
