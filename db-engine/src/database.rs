use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{mem_table::MemTable, prelude::*, wal::WriteAheadLog};

pub struct Database {
    dir: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable,
}

impl Database {
    pub fn new(dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        let (wal, mem_table) = WriteAheadLog::restore_from_dir(&dir)?;

        let db = Self {
            dir,
            wal,
            mem_table,
        };

        Ok(db)
    }

    pub fn get(self, key: &[u8]) -> Option<DbEntry> {
        // TODO : load from SSTable

        let entry = self.mem_table.get(key)?;
        if entry.is_deleted() {
            return None;
        }

        let db_entry = DbEntry {
            key: entry.key.clone(),
            value: entry.value.as_ref().unwrap().clone(),
            timestamp: entry.timestamp,
        };
        Some(db_entry)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<usize> {
        let timestamp = self.new_timestamp()?;

        // wal
        self.wal
            .set(key, value, timestamp)
            .context("write data to wal")?;
        self.wal.flush().context("flash wal to file")?;

        // mem_table
        self.mem_table.set(key, value, timestamp);

        // TODO -> persist data to SSTable

        Ok(1)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<usize> {
        let timestamp = self.new_timestamp()?;

        // wal
        self.wal.delete(key, timestamp)?;
        self.wal.flush()?;

        // mem_table
        self.mem_table.delete(key, timestamp);

        Ok(1)
    }

    // TODO scan

    fn new_timestamp(&self) -> Result<u128> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("generate new timestamp")?
            .as_micros();
        Ok(timestamp)
    }
}
