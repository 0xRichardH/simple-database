use anyhow::{Context, Result};
use std::{fs::remove_file, path::PathBuf};

use crate::{
    mem_table::MemTable,
    prelude::*,
    sstable::{SSTableQuerier, SSTableWriter},
    utils::*,
    wal::WriteAheadLog,
};

const DEFAULT_MAX_MEM_TABLE_SIZE: usize = 10 * 1024 * 1024;

pub struct Database {
    dir: PathBuf,
    wal: WriteAheadLog,
    mem_table: MemTable,
    max_mem_table_size: usize,
}

pub struct DatabaseBuilder(Database);

impl DatabaseBuilder {
    pub fn new(dir: PathBuf) -> Result<Self> {
        let (wal, mem_table) = WriteAheadLog::restore_from_dir(&dir)?;

        let db = Database {
            dir,
            wal,
            mem_table,
            max_mem_table_size: DEFAULT_MAX_MEM_TABLE_SIZE,
        };
        Ok(Self(db))
    }

    pub fn max_mem_table_size(mut self, max_mem_table_size: usize) -> Self {
        self.0.max_mem_table_size = max_mem_table_size;
        self
    }

    pub fn build(self) -> Database {
        self.0
    }
}

impl Database {
    pub fn get(&self, key: &[u8]) -> Option<DbEntry> {
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
        if self.mem_table.size() >= self.max_mem_table_size {
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
            *self = DatabaseBuilder::new(self.dir.clone())?
                .max_mem_table_size(self.max_mem_table_size)
                .build();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn it_works_with_mem_table() -> Result<()> {
        let tmpdir = TempDir::new("mem_table_test")?;
        let mut db = DatabaseBuilder::new(tmpdir.path().to_path_buf())?.build();

        assert!(db.get(b"test").is_none());
        assert_eq!(db.mem_table.size(), 0);
        assert_eq!(db.mem_table.len(), 0);

        let result = db.set(b"test", b"hello")?;
        assert_eq!(result, 1);
        assert_ne!(db.mem_table.size(), 0);
        assert_eq!(db.mem_table.len(), 1);

        let entry = db.get(b"test").unwrap();
        assert_eq!(entry.key, b"test");
        assert_eq!(entry.value, b"hello");

        db.delete(b"test")?;
        assert!(db.get(b"test").is_none());

        tmpdir.close()?;
        Ok(())
    }

    #[test]
    fn it_works_with_wal_files() -> Result<()> {
        let tmpdir = TempDir::new("wal_test")?;
        let dir = tmpdir.path().to_path_buf();

        // seed
        DatabaseBuilder::new(dir.clone())?
            .build()
            .set(b"hello", b"world")?;

        // load data in existing wal file
        let db = DatabaseBuilder::new(dir)?.build();
        assert!(db.get(b"test").is_none());
        assert!(db.get(b"hello").is_some());

        tmpdir.close()?;
        Ok(())
    }

    #[test]
    fn it_works_with_sstable() -> Result<()> {
        let tmpdir = TempDir::new("sstable_test")?;
        let dir = tmpdir.path().to_path_buf();

        // seed
        let seed_entry = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        SSTableWriter::new(&dir.join("test.db"))?
            .set(&seed_entry)?
            .flush()?;

        // test
        let db = DatabaseBuilder::new(dir)?.build();
        let result = db.get(b"test1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, b"hello");
        assert!(db.get(b"test").is_none());

        tmpdir.close()?;
        Ok(())
    }

    #[test]
    fn it_persists_data_to_sstable_when_reached_the_max_limitation() -> Result<()> {
        let tmpdir = TempDir::new("persist_to_sstable").unwrap();

        let mut db = DatabaseBuilder::new(tmpdir.path().to_path_buf())?
            .max_mem_table_size(64)
            .build();
        db.set(b"test", b"helloworld")?;
        db.set(b"test1", b"helloworld1")?;
        assert_eq!(db.mem_table.size(), 0);
        assert_eq!(db.mem_table.len(), 0);

        let entry = db.get(b"test");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, b"helloworld");

        tmpdir.close()?;
        Ok(())
    }
}
