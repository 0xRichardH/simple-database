use anyhow::Result;
use std::path::PathBuf;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::prelude::*;

use super::{
    get_index_path,
    sstable_index::{SSTableIndex, SSTableIndexBuilder},
};

/// Sorted String Table
pub struct SSTableWriter {
    index: SSTableIndex,
    writer: BufWriter<File>,
    offset: u64,
}

impl SSTableWriter {
    pub async fn new(path: &PathBuf) -> Result<Self> {
        let index_path = get_index_path(path)?;
        let index = SSTableIndexBuilder::new(index_path)
            .indexes()
            .await?
            .build();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let offset = file.metadata().await?.len();
        let writer = BufWriter::new(file);

        Ok(Self {
            index,
            writer,
            offset,
        })
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    /// Set Entry to SSTable
    pub async fn set(&mut self, entry: &Entry) -> io::Result<&mut Self> {
        entry.write_to(&mut self.writer).await?;
        self.index.insert(entry.key.as_slice(), self.offset);
        self.offset += self.writer.stream_position().await?;
        Ok(self)
    }

    /// Flush SSTable to the file
    pub async fn flush(&mut self) -> Result<&mut Self> {
        let persist_index = self.index.persist();
        let flush_db = self.writer.flush();

        let (persist_result, flush_result) = tokio::join!(persist_index, flush_db);
        persist_result?;
        flush_result?;

        Ok(self)
    }
}
