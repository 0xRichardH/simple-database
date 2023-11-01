use anyhow::{Context, Result};
use std::{fs::remove_file, path::PathBuf};

use crate::{
    mem_table::MemTable,
    prelude::*,
    sstable::{SSTableQuerier, SSTableWriter},
    utils::*,
    wal::WriteAheadLog,
};

const MAX_MEM_TABLE_SIZE: usize = 10 * 1024 * 1024;

pub struct Database {
    dir: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable,
}

impl Database {
    pub fn new(dir: PathBuf) -> Result<Self> {
        let (wal, mem_table) = WriteAheadLog::restore_from_dir(&dir)?;

        let db = Self {
            dir,
            wal,
            mem_table,
        };

        Ok(db)
    }

    pub fn get(self, key: &[u8]) -> Option<DbEntry> {
        let mut entry_opt = self.mem_table.get(key).cloned();
        if entry_opt.is_none() {
            match SSTableQuerier::new(&self.dir) {
                Ok(querier) => {
                    entry_opt = querier.query(key);
                }
                Err(e) => {
                    eprintln!("SSTable querier error: {}", e);
                }
            }
        }

        let entry = entry_opt?;
        if entry.is_deleted() {
            return None;
        }

        let db_entry = DbEntry {
            key: entry.key,
            value: entry.value.unwrap(),
            timestamp: entry.timestamp,
        };
        Some(db_entry)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<usize> {
        let timestamp = micros_now()?;

        // wal
        self.wal
            .set(key, value, timestamp)
            .context("write data to wal")?;
        self.wal.flush().context("flash wal to file")?;

        // mem_table
        self.mem_table.set(key, value, timestamp);

        // persist to SSTable
        self.persist_to_sstable()?;

        Ok(1)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<usize> {
        let timestamp = micros_now()?;

        // wal
        self.wal.delete(key, timestamp)?;
        self.wal.flush()?;

        // mem_table
        self.mem_table.delete(key, timestamp);

        // persist to SSTable
        self.persist_to_sstable()?;

        Ok(1)
    }

    fn persist_to_sstable(&mut self) -> Result<()> {
        if self.mem_table.size() >= MAX_MEM_TABLE_SIZE {
            // flush the data to sstable
            let sstable_path = self.dir.join(format!("{}.db", micros_now()?));
            let mut writer = SSTableWriter::new(&sstable_path)?;
            for entry in self.mem_table.entries().iter() {
                writer.set(entry).context("add entry to sstable")?;
            }
            writer.flush().context("flash sstable buffer to file")?;

            // delete correspond wal file
            remove_file(self.wal.path()).context("remove wal file")?;
            // clear mem_table
            *self = Self::new(self.dir.clone())?;
        }
        Ok(())
    }
}
