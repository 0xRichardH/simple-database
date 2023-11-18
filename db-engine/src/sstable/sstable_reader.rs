use anyhow::Result;
use async_trait::async_trait;
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

/// This function will be called for each Entry when calling SSTableReader#scan
#[async_trait]
pub trait SSTableReaderScanHandler {
    async fn handle(&mut self, entry: Entry) -> Result<()>;
}

/// Sorted String Table
pub struct SSTableReader {
    index: SSTableIndex,
    reader: BufReader<File>,
}

impl SSTableReader {
    pub async fn new(path: &PathBuf) -> Result<Self> {
        let index_path = get_index_path(path)?;
        let index = SSTableIndexBuilder::new(index_path)
            .indexes()
            .await?
            .build();

        let file = OpenOptions::new().write(true).read(true).open(path).await?;
        let reader = BufReader::new(file);

        Ok(Self { index, reader })
    }

    /// Get Entry from SSTable file
    pub async fn get(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(&offset) = self.index.get(key) {
            return self.read(offset).await;
        }

        None
    }

    /// Read Entry from SSTable file by offset
    pub async fn read(&mut self, offset: u64) -> Option<Entry> {
        self.reader.seek(io::SeekFrom::Start(offset)).await.ok()?;
        Entry::read_from(&mut self.reader).await
    }

    /// Scan Entries from SSTable file
    pub async fn scan(&mut self, mut handler: impl SSTableReaderScanHandler) -> Result<()> {
        for (_, offset) in self.index.indexes().clone() {
            if let Some(entry) = self.read(offset).await {
                handler.handle(entry).await?
            }
        }
        Ok(())
    }
}
