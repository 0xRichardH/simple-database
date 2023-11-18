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
                eprintln!("Failed to remove old sstable file: {}", e);
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
