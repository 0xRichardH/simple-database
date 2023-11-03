use anyhow::Result;
use std::path::PathBuf;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncSeekExt, BufReader},
};

use crate::prelude::*;

use super::{
    get_index_path,
    sstable_index::{SSTableIndex, SSTableIndexBuilder},
};

/// Sorted String Table
pub struct SSTableReader {
    index: SSTableIndex,
    reader: BufReader<File>,
}

impl SSTableReader {
    pub async fn new(path: &PathBuf) -> Result<Self> {
        let index_path = get_index_path(path)?;
        let index = SSTableIndexBuilder::new(index_path).indexs().await?.build();

        let file = OpenOptions::new().write(true).read(true).open(path).await?;
        let reader = BufReader::new(file);

        Ok(Self { index, reader })
    }

    /// Get Entry from SSTable file
    pub async fn get(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(offset) = self.index.get(key) {
            self.reader.seek(io::SeekFrom::Start(*offset)).await.ok()?;
            return Entry::read_from(&mut self.reader).await;
        }

        None
    }
}
