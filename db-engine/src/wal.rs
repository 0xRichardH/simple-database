use anyhow::Result;
use std::{
    future::Future,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{remove_file, File, OpenOptions},
    io::{self, AsyncWriteExt, BufReader, BufWriter},
};
use tokio_stream::{Stream, StreamExt};

use crate::{
    mem_table::MemTable,
    prelude::*,
    utils::{self, micros_now},
};

/// Write Ahead Log
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    /// Creates a new WAL in a given directory.
    pub async fn new(dir: &Path) -> Result<Self> {
        let timestamp = micros_now()?;
        let path = Path::new(dir).join(format!("{}.wal", timestamp));
        Self::from_path(&path).await
    }

    /// Creates a WAL from an existing file path.
    pub async fn from_path(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            path: path.to_owned(),
        })
    }

    /// Restore our MemTable and WAL from a directory.
    /// We need to replay all of the operations.
    pub async fn restore_from_dir(dir: &Path) -> Result<(WriteAheadLog, MemTable)> {
        let mut wal_files = utils::get_files_with_ext(dir, "wal")?;
        wal_files.sort();

        let mut new_memtable = MemTable::new();
        let mut new_wal = WriteAheadLog::new(dir).await?;
        for file in wal_files.iter() {
            let wal = WriteAheadLog::from_path(file).await?;
            let mut wal_iter = WALIterator::new(wal.path).await?;
            while let Some(entry) = wal_iter.next().await {
                let key = entry.key.as_slice();
                let timestamp = entry.timestamp;
                if entry.is_deleted() {
                    new_wal.delete(key, timestamp).await?;
                    new_memtable.delete(key, timestamp);
                } else {
                    let value = entry.value.unwrap();
                    new_wal.set(key, value.as_slice(), timestamp).await?;
                    new_memtable.set(key, value.as_slice(), timestamp);
                }
            }
        }
        new_wal.flush().await?;

        // clean up the old WAL files
        for file in wal_files.into_iter() {
            // FIXME concurrent
            remove_file(file).await?;
        }

        Ok((new_wal, new_memtable))
    }

    /// Sets a Key-Value pair and the operation is appended to the WAL.
    pub async fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        let entry = Entry::new(key.to_vec(), Some(value.to_vec()), timestamp);
        entry.write_to(&mut self.writer).await
    }

    /// Deletes a Key-Value pair and the operation is appended to the WAL.
    pub async fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        let entry = Entry::new(key.to_vec(), None, timestamp);
        entry.write_to(&mut self.writer).await
    }

    /// Flushes the WAL to disk.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

/// WAL Iterator will iterate over the items in the WAL file.
pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub async fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path).await?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Stream for WALIterator {
    type Item = Entry;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut reader = self.get_mut().reader;
        let entry_future = Entry::read_from(&mut reader);
        Box::pin(entry_future).as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use tokio::{
        fs::{metadata, File, OpenOptions},
        io::BufReader,
    };

    use crate::prelude::Entry;
    use crate::wal::WriteAheadLog;
    use std::time::{SystemTime, UNIX_EPOCH};

    async fn check_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
        deleted: bool,
    ) {
        let entry = Entry::read_from(reader).await.unwrap();
        assert_eq!(entry.key, key);
        assert_eq!(entry.value.as_deref(), value);
        assert_eq!(entry.timestamp, timestamp);
        assert_eq!(entry.is_deleted(), deleted);
    }

    #[tokio::test]
    async fn test_write_one() {
        let temp_dir = TempDir::new("test_write_one").unwrap();
        let dir = temp_dir.path();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WriteAheadLog::new(dir).await.unwrap();
        wal.set(b"Lime", b"Lime Smoothie", timestamp).await.unwrap();
        wal.flush().await.unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).await.unwrap();
        let mut reader = BufReader::new(file);

        check_entry(
            &mut reader,
            b"Lime",
            Some(b"Lime Smoothie"),
            timestamp,
            false,
        );

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_write_many() {
        let temp_dir = TempDir::new("test_write_many").unwrap();
        let dir = temp_dir.path();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = WriteAheadLog::new(dir).await.unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).await.unwrap();
        }
        wal.flush().await.unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).await.unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_write_delete() {
        let temp_dir = TempDir::new("test_write_delete").unwrap();
        let dir = temp_dir.path();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = WriteAheadLog::new(dir).await.unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).await.unwrap();
        }
        for e in entries.iter() {
            wal.delete(e.0, timestamp).await.unwrap();
        }

        wal.flush().await.unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).await.unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }
        for e in entries.iter() {
            check_entry(&mut reader, e.0, None, timestamp, true);
        }

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_read_wal_none() {
        let temp_dir = TempDir::new("test_read_wal_none").unwrap();
        let dir = temp_dir.path();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).await.unwrap();
        assert_eq!(new_mem_table.len(), 0);

        let m = metadata(new_wal.path).await.unwrap();
        assert_eq!(m.len(), 0);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_read_wal_one() {
        let temp_dir = TempDir::new("test_read_wal_one").unwrap();
        let dir = temp_dir.path();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = WriteAheadLog::new(dir).await.unwrap();

        for (i, e) in entries.iter().enumerate() {
            wal.set(e.0, e.1.unwrap(), i as u128).await.unwrap();
        }
        wal.flush().await.unwrap();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).await.unwrap();

        let file = OpenOptions::new()
            .read(true)
            .open(&new_wal.path)
            .await
            .unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, i as u128);
        }

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_read_wal_multiple() {
        let temp_dir = TempDir::new("test_read_wal_multiple").unwrap();
        let dir = temp_dir.path();

        let entries_1: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];
        let mut wal_1 = WriteAheadLog::new(dir).await.unwrap();
        for (i, e) in entries_1.iter().enumerate() {
            wal_1.set(e.0, e.1.unwrap(), i as u128).await.unwrap();
        }
        wal_1.flush().await.unwrap();

        let entries_2: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Strawberry", Some(b"Strawberry Smoothie")),
            (b"Blueberry", Some(b"Blueberry Smoothie")),
            (b"Orange", Some(b"Orange Milkshake")),
        ];
        let mut wal_2 = WriteAheadLog::new(dir).await.unwrap();
        for (i, e) in entries_2.iter().enumerate() {
            wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).await.unwrap();
        }
        wal_2.flush().await.unwrap();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).await.unwrap();

        let file = OpenOptions::new()
            .read(true)
            .open(&new_wal.path)
            .await
            .unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries_1.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            if i != 2 {
                assert_eq!(mem_e.key, e.0);
                assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_eq!(mem_e.timestamp, i as u128);
            } else {
                assert_eq!(mem_e.key, e.0);
                assert_ne!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_ne!(mem_e.timestamp, i as u128);
            }
        }
        for (i, e) in entries_2.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, (i + 3) as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, (i + 3) as u128);
        }

        temp_dir.close().unwrap();
    }
}
