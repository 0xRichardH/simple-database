mod sstable_index;

pub use self::sstable_index::*;

use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use crate::prelude::*;

/// Sorted String Table
pub struct SSTable {
    index: SSTableIndex,
    path: PathBuf,
    writer: BufWriter<File>,
    offset: u64,
}

impl SSTable {
    pub fn new(path: PathBuf) -> Result<Self> {
        let index_path = Self::get_index_path(&path)?;
        let index = SSTableIndexBuilder::new(index_path).indexs()?.build();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        let offset = file.metadata()?.len();
        let writer = BufWriter::new(file);

        Ok(Self {
            index,
            path,
            writer,
            offset,
        })
    }

    fn get_index_path(path: &PathBuf) -> Result<PathBuf> {
        let base_path = path.parent().ok_or(Error::InvalidPath(path.clone()))?;
        let db_file_name = path.file_name().ok_or(Error::InvalidPath(path.clone()))?;
        let index_path = base_path.join(format!("{}.idx", db_file_name.to_string_lossy()));
        Ok(index_path)
    }

    /// Get Entry from SSTable file
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if let Some(offset) = self.index.get(key) {
            let file = OpenOptions::new().read(true).open(&self.path).ok()?;
            let mut reader = BufReader::new(file);
            reader.seek(io::SeekFrom::Start(*offset)).ok()?;
            return Entry::read_from(&mut reader);
        }

        None
    }

    /// Set Entry to SSTable
    pub fn set(&mut self, entry: &Entry) -> io::Result<()> {
        entry.write_to(&mut self.writer)?;
        self.index.insert(entry.key.as_slice(), self.offset);
        self.offset += self.writer.stream_position()?;
        Ok(())
    }

    /// Flush SSTable to the file
    pub fn flush(&mut self) -> Result<()> {
        self.index.persist()?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn it_creates_new_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file")?;
        let path = temp_dir.path().join("test.db");

        let mut sst = SSTable::new(path)?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);

        // seed the data
        sst.set(&entry_1)?;
        sst.set(&entry_2)?;

        // persist to file
        sst.flush()?;
        assert_entry(&sst.get(b"test1").unwrap(), &entry_1);
        assert_entry(&sst.get(b"test2").unwrap(), &entry_2);

        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn it_updates_the_existing_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file_update")?;
        let path = temp_dir.path().join("test.db");

        // seed
        let mut sst = SSTable::new(path.clone())?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        sst.set(&entry_1)?;
        sst.flush()?;
        assert_entry(&sst.get(b"test1").unwrap(), &entry_1);

        // load from existing file
        let mut new_sst = SSTable::new(path)?;
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);
        new_sst.set(&entry_2)?;
        new_sst.flush()?;
        assert_entry(&new_sst.get(b"test1").unwrap(), &entry_1);
        assert_entry(&new_sst.get(b"test2").unwrap(), &entry_2);

        Ok(())
    }

    fn assert_entry(entry_1: &Entry, entry_2: &Entry) {
        assert_eq!(entry_1.key, entry_2.key);
        assert_eq!(entry_1.value, entry_2.value);
        assert_eq!(entry_1.timestamp, entry_2.timestamp);
        assert_eq!(entry_1.is_deleted(), entry_2.is_deleted());
    }
}
