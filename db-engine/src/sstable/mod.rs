mod sstable_index;
mod sstable_querier;
mod sstable_reader;
mod sstable_writer;

pub use self::sstable_index::*;

use crate::prelude::*;
use std::path::PathBuf;

fn get_index_path(db_path: &PathBuf) -> anyhow::Result<PathBuf> {
    let base_path = db_path
        .parent()
        .ok_or(Error::InvalidPath(db_path.clone()))?;
    let db_file_name = db_path
        .file_name()
        .ok_or(Error::InvalidPath(db_path.clone()))?;
    let index_path = base_path.join(format!("{}.idx", db_file_name.to_string_lossy()));
    Ok(index_path)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::{sstable_reader::SSTableReader, sstable_writer::SSTableWriter, *};
    use anyhow::Result;

    #[test]
    fn it_creates_new_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file")?;
        let path = temp_dir.path().join("test.db");

        let mut sst_writer = SSTableWriter::new(&path)?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);

        // seed the data
        sst_writer.set(&entry_1)?;
        sst_writer.set(&entry_2)?;

        // persist to file
        sst_writer.flush()?;
        let mut sst_reader = SSTableReader::new(&path)?;
        assert_entry(&sst_reader.get(b"test1").unwrap(), &entry_1);
        assert_entry(&sst_reader.get(b"test2").unwrap(), &entry_2);

        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn it_updates_the_existing_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file_update")?;
        let path = temp_dir.path().join("test.db");

        // seed
        let mut sst_writer = SSTableWriter::new(&path)?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        sst_writer.set(&entry_1)?;
        sst_writer.flush()?;
        let mut sst_reader = SSTableReader::new(&path)?;
        assert_entry(&sst_reader.get(b"test1").unwrap(), &entry_1);

        // load from existing file
        let mut new_sst_writer = SSTableWriter::new(&path)?;
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);
        new_sst_writer.set(&entry_2)?;
        new_sst_writer.flush()?;
        let mut new_sst_reader = SSTableReader::new(&path)?;
        assert_entry(&new_sst_reader.get(b"test1").unwrap(), &entry_1);
        assert_entry(&new_sst_reader.get(b"test2").unwrap(), &entry_2);

        Ok(())
    }

    fn assert_entry(entry_1: &Entry, entry_2: &Entry) {
        assert_eq!(entry_1.key, entry_2.key);
        assert_eq!(entry_1.value, entry_2.value);
        assert_eq!(entry_1.timestamp, entry_2.timestamp);
        assert_eq!(entry_1.is_deleted(), entry_2.is_deleted());
    }
}