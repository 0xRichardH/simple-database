use anyhow::{Context, Result};
use std::{collections::BTreeMap, path::PathBuf};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
};

type SSTableIndexType = BTreeMap<Vec<u8>, u64>;

/// Sorted String Table Index
#[derive(Debug)]
pub struct SSTableIndex {
    indexes: SSTableIndexType,
    path: PathBuf,
}

pub struct SSTableIndexBuilder(SSTableIndex);

impl SSTableIndexBuilder {
    /// Load SSTable Index from file
    /// create new file if not exist
    pub fn new(path: PathBuf) -> Self {
        let indexes = BTreeMap::new();
        let index = SSTableIndex { indexes, path };
        Self(index)
    }

    pub async fn indexes(mut self) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.0.path)
            .await
            .context("open idx file to read")?;
        let mut buf = Vec::new();
        let read_size = file
            .read_to_end(&mut buf)
            .await
            .context("read content from idx")?;
        if read_size > 0 {
            self.0.indexes = bincode::deserialize(&buf).context("deserialize idx to BTreeMap")?;
        }

        Ok(self)
    }

    pub fn build(self) -> SSTableIndex {
        self.0
    }
}

impl SSTableIndex {
    /// Insert the Entry Key and SSTable Offset to the SSTable Index
    pub fn insert(&mut self, key: &[u8], offset: u64) {
        self.indexes.insert(key.to_vec(), offset);
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.indexes.contains_key(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<&u64> {
        self.indexes.get(key)
    }

    /// Get the SSTable Indexes
    pub fn indexes(&self) -> &SSTableIndexType {
        &self.indexes
    }

    /// Persist the indexes to file
    pub async fn persist(&mut self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.path)
            .await
            .context("open idx file to write")?;
        let bytes = bincode::serialize(&self.indexes).context("serialize idx to bytes")?;
        file.write_all(&bytes)
            .await
            .context("write idx bytes to file")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let temp_dir = TempDir::new("sstable_index")?;
        let path = temp_dir.path().join("sstable_index.idx");

        // create SSTableIndex
        let mut idx = SSTableIndexBuilder::new(path.clone())
            .indexes()
            .await?
            .build();
        assert_eq!(idx.indexes.len(), 0);
        idx.insert(b"hello", 1);
        assert_eq!(idx.indexes.len(), 1);
        assert_eq!(idx.get(b"hello"), Some(&1));

        // persist to file
        idx.persist().await?;

        // load from file
        let mut idx_2 = SSTableIndexBuilder::new(path.clone())
            .indexes()
            .await?
            .build();
        assert_eq!(idx_2.indexes.len(), 1);
        assert_eq!(idx_2.get(b"hello"), Some(&1));

        // inesert new key and value
        idx_2.insert(b"world", 2);
        assert_eq!(idx.indexes.len(), 1);
        assert_eq!(idx_2.indexes.len(), 2);
        assert_eq!(idx_2.get(b"world"), Some(&2));

        // persist to file
        idx_2.persist().await?;
        let idx_3 = SSTableIndexBuilder::new(path).indexes().await?.build();
        assert_eq!(idx_3.indexes.len(), 2);

        temp_dir.close().unwrap();
        Ok(())
    }
}
