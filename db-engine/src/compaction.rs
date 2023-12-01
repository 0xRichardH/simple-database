use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::{fs::remove_file, task::JoinSet};

use crate::{
    prelude::Entry,
    sstable::{SSTableIndexBuilder, SSTableReader, SSTableReaderScanHandler, SSTableWriter},
    utils::{get_files_with_ext, get_files_with_ext_and_size, micros_now},
};

pub struct Compaction {
    dir: PathBuf,
    size: u64,
    ext: String,
}

impl Compaction {
    pub fn new(dir: PathBuf, size: u64, ext: &str) -> Self {
        Self {
            dir,
            size,
            ext: ext.into(),
        }
    }

    pub async fn compact(&self) -> Result<()> {
        let mut files = get_files_with_ext_and_size(&self.dir, self.ext.as_str(), self.size)?;
        if files.is_empty() {
            tracing::info!("Skip compacting because no sstable files found");
            return Ok(());
        }
        files.sort_by(|a, b| b.cmp(a));

        let new_sstable_path = self.dir.join(format!("{}.db", micros_now()?));
        let mut writer = SSTableWriter::new(&new_sstable_path).await?;
        let mut to_be_deleted_keys: Vec<Vec<u8>> = Vec::new();
        for file in files.iter() {
            let mut reader = SSTableReader::new(file).await?;
            reader
                .scan(SSTableScanHandler::new(
                    &mut writer,
                    &mut to_be_deleted_keys,
                ))
                .await?;
        }

        // persist to disk
        writer.flush().await.context("flush new sstable to disk")?;
        // delete the old files
        let mut remove_file_fn_set = files.into_iter().fold(JoinSet::new(), |mut fn_set, file| {
            fn_set.spawn(remove_file(file));
            fn_set
        });
        while let Some(res) = remove_file_fn_set.join_next().await {
            if let Err(e) = res {
                tracing::error!("Failed to remove old sstable file: {}", e);
            }
        }

        // handle to be deleted keys
        self.remove_deleted_keys(to_be_deleted_keys)
            .await
            .context("remove deleted keys")?;

        Ok(())
    }

    async fn remove_deleted_keys(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        let idx_files = get_files_with_ext(self.dir.as_ref(), "idx")?;
        for file in idx_files {
            let mut idx = SSTableIndexBuilder::new(file).indexes().await?.build();

            for key in keys.iter() {
                if idx.contains_key(key) {
                    idx.remove(key);
                }
            }

            idx.persist().await.context("update the idx file")?;
        }

        Ok(())
    }
}

struct SSTableScanHandler<'a, 'b> {
    writer: &'a mut SSTableWriter,
    to_be_deleted_keys: &'b mut Vec<Vec<u8>>,
}

impl<'a, 'b> SSTableScanHandler<'a, 'b> {
    fn new(writer: &'a mut SSTableWriter, to_be_deleted_keys: &'b mut Vec<Vec<u8>>) -> Self {
        Self {
            writer,
            to_be_deleted_keys,
        }
    }
}

#[async_trait]
impl<'a, 'b> SSTableReaderScanHandler for SSTableScanHandler<'a, 'b> {
    async fn handle(&mut self, entry: Entry) -> Result<()> {
        if entry.is_deleted() {
            self.to_be_deleted_keys.push(entry.key);
            // delete it in all of the files
            return Ok(());
        }

        if self.writer.contains_key(&entry.key) {
            // Skip handling the duplcate entry
            return Ok(());
        }

        self.writer
            .set(&entry)
            .await
            .context("write entry to new sstable")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;
    use tempdir::TempDir;

    use super::*;

    // Helper function to create a dummy SSTable file for testing
    async fn create_dummy_sstable_file(dir: &Path, filename: &str, entry: &Entry) -> Result<()> {
        let file_path = dir.join(filename);
        let mut writer = SSTableWriter::new(&file_path)
            .await
            .context("init sstable writer")?;
        writer
            .set(entry)
            .await
            .context("insert record")?
            .flush()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_compact() -> Result<()> {
        // Setup test directory and files
        let tmpdir = TempDir::new("test_compact")?;
        let test_dir = tmpdir.path();
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"hello").map(|i| i.to_vec()), 2);
        create_dummy_sstable_file(test_dir, "test1.db", &entry_1).await?;
        create_dummy_sstable_file(test_dir, "test2.db", &entry_2).await?;

        // Initialize Compaction
        let compaction = Compaction::new(test_dir.to_path_buf(), 100, "db");

        // Perform compaction
        compaction.compact().await.context("Failed to compact")?;

        // Check results
        // 1. check if the old files are deleted
        assert!(!test_dir.join("test1.db").exists());
        assert!(!test_dir.join("test2.db").exists());
        // 2. check if the new file is created
        let files = get_files_with_ext(test_dir, "db")?;
        assert_eq!(files.len(), 1);

        // 3. check if the data in the new file are correct
        let new_file = files.first().unwrap();
        let mut sstable_reader = SSTableReader::new(new_file).await?;
        assert!(sstable_reader.get(entry_1.key.as_slice()).await.is_some());
        assert!(sstable_reader.get(entry_2.key.as_slice()).await.is_some());

        // Cleanup
        tmpdir.close().context("remove the test folders")?;

        Ok(())
    }
}
