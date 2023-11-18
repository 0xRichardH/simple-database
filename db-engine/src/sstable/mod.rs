mod sstable_index;
mod sstable_querier;
mod sstable_reader;
mod sstable_writer;

pub use self::sstable_querier::*;
pub use self::sstable_reader::*;
pub use self::sstable_writer::*;

use crate::prelude::*;
use std::path::Path;
use std::path::PathBuf;

fn get_index_path(db_path: &Path) -> anyhow::Result<PathBuf> {
    let base_path = db_path
        .parent()
        .ok_or(Error::InvalidPath(db_path.to_path_buf()))?;
    let db_file_name = db_path
        .file_name()
        .ok_or(Error::InvalidPath(db_path.to_path_buf()))?;
    let index_path = base_path.join(format!("{}.idx", db_file_name.to_string_lossy()));
    Ok(index_path)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::{sstable_reader::SSTableReader, sstable_writer::SSTableWriter, *};
    use anyhow::Result;

    #[tokio::test]
    async fn it_creates_new_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file")?;
        let path = temp_dir.path().join("test.db");

        let mut sst_writer = SSTableWriter::new(&path).await?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);

        // seed the data
        sst_writer
            .set(&entry_1)
            .await?
            .set(&entry_2)
            .await?
            .flush()
            .await?;

        // persist to file
        let mut sst_reader = SSTableReader::new(&path).await?;
        assert_entry(&sst_reader.get(b"test1").await.unwrap(), &entry_1);
        assert_entry(&sst_reader.get(b"test2").await.unwrap(), &entry_2);
        assert!(sst_reader.get(b"test3").await.is_none());

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn it_updates_the_existing_sstable_file() -> Result<()> {
        let temp_dir = TempDir::new("sstable_file_update")?;
        let path = temp_dir.path().join("test.db");

        // seed
        let mut sst_writer = SSTableWriter::new(&path).await?;
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        sst_writer.set(&entry_1).await?.flush().await?;
        let mut sst_reader = SSTableReader::new(&path).await?;
        assert_entry(&sst_reader.get(b"test1").await.unwrap(), &entry_1);

        // load from existing file
        let mut new_sst_writer = SSTableWriter::new(&path).await?;
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"world").map(|i| i.to_vec()), 2);
        new_sst_writer.set(&entry_2).await?.flush().await?;
        let mut new_sst_reader = SSTableReader::new(&path).await?;
        assert_entry(&new_sst_reader.get(b"test1").await.unwrap(), &entry_1);
        assert_entry(&new_sst_reader.get(b"test2").await.unwrap(), &entry_2);
        assert!(new_sst_reader.get(b"test3").await.is_none());

        Ok(())
    }

    fn assert_entry(entry_1: &Entry, entry_2: &Entry) {
        assert_eq!(entry_1.key, entry_2.key);
        assert_eq!(entry_1.value, entry_2.value);
        assert_eq!(entry_1.timestamp, entry_2.timestamp);
        assert_eq!(entry_1.is_deleted(), entry_2.is_deleted());
    }
}
