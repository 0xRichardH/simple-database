use anyhow::Result;
use std::{
    fs::{remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

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
    pub fn new(dir: &Path) -> Result<Self> {
        let timestamp = micros_now()?;
        let path = Path::new(dir).join(format!("{}.wal", timestamp));
        Self::from_path(&path)
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            path: path.to_owned(),
        })
    }

    /// Restore our MemTable and WAL from a directory.
    /// We need to replay all of the operations.
    pub fn restore_from_dir(dir: &Path) -> Result<(WriteAheadLog, MemTable)> {
        let mut wal_files = utils::get_files_with_ext(dir, "wal")?;
        wal_files.sort();

        let mut new_memtable = MemTable::new();
        let mut new_wal = WriteAheadLog::new(dir)?;
        for file in wal_files.iter() {
            let wal = WriteAheadLog::from_path(file)?;
            let wal_iter: WALIterator = wal.try_into()?;
            for entry in wal_iter {
                let key = entry.key.as_slice();
                let timestamp = entry.timestamp;
                if entry.is_deleted() {
                    new_wal.delete(key, timestamp)?;
                    new_memtable.delete(key, timestamp);
                } else {
                    let value = entry.value.unwrap();
                    new_wal.set(key, value.as_slice(), timestamp)?;
                    new_memtable.set(key, value.as_slice(), timestamp);
                }
            }
        }
        new_wal.flush()?;

        // clean up the old WAL files
        for file in wal_files.into_iter() {
            remove_file(file)?;
        }

        Ok((new_wal, new_memtable))
    }

    /// Sets a Key-Value pair and the operation is appended to the WAL.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        let entry = Entry::new(key.to_vec(), Some(value.to_vec()), timestamp);
        entry.write_to(&mut self.writer)
    }

    /// Deletes a Key-Value pair and the operation is appended to the WAL.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        let entry = Entry::new(key.to_vec(), None, timestamp);
        entry.write_to(&mut self.writer)
    }

    /// Flushes the WAL to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// WAL Iterator will iterate over the items in the WAL file.
pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WALIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        Entry::read_from(&mut self.reader)
    }
}

impl TryFrom<WriteAheadLog> for WALIterator {
    type Error = io::Error;

    fn try_from(value: WriteAheadLog) -> std::result::Result<Self, Self::Error> {
        WALIterator::new(value.path)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::prelude::Entry;
    use crate::wal::WriteAheadLog;
    use std::fs::{metadata, File, OpenOptions};
    use std::io::BufReader;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn check_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
        deleted: bool,
    ) {
        let entry = Entry::read_from(reader).unwrap();
        assert_eq!(entry.key, key);
        assert_eq!(entry.value.as_deref(), value);
        assert_eq!(entry.timestamp, timestamp);
        assert_eq!(entry.is_deleted(), deleted);
    }

    #[test]
    fn test_write_one() {
        let temp_dir = TempDir::new("test_write_one").unwrap();
        let dir = temp_dir.path();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WriteAheadLog::new(dir).unwrap();
        wal.set(b"Lime", b"Lime Smoothie", timestamp).unwrap();
        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
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

    #[test]
    fn test_write_many() {
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

        let mut wal = WriteAheadLog::new(dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_write_delete() {
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

        let mut wal = WriteAheadLog::new(dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        for e in entries.iter() {
            wal.delete(e.0, timestamp).unwrap();
        }

        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }
        for e in entries.iter() {
            check_entry(&mut reader, e.0, None, timestamp, true);
        }

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_read_wal_none() {
        let temp_dir = TempDir::new("test_read_wal_none").unwrap();
        let dir = temp_dir.path();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).unwrap();
        assert_eq!(new_mem_table.len(), 0);

        let m = metadata(new_wal.path).unwrap();
        assert_eq!(m.len(), 0);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_read_wal_one() {
        let temp_dir = TempDir::new("test_read_wal_one").unwrap();
        let dir = temp_dir.path();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = WriteAheadLog::new(dir).unwrap();

        for (i, e) in entries.iter().enumerate() {
            wal.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal.flush().unwrap();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
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

    #[test]
    fn test_read_wal_multiple() {
        let temp_dir = TempDir::new("test_read_wal_multiple").unwrap();
        let dir = temp_dir.path();

        let entries_1: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];
        let mut wal_1 = WriteAheadLog::new(dir).unwrap();
        for (i, e) in entries_1.iter().enumerate() {
            wal_1.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal_1.flush().unwrap();

        let entries_2: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Strawberry", Some(b"Strawberry Smoothie")),
            (b"Blueberry", Some(b"Blueberry Smoothie")),
            (b"Orange", Some(b"Orange Milkshake")),
        ];
        let mut wal_2 = WriteAheadLog::new(dir).unwrap();
        for (i, e) in entries_2.iter().enumerate() {
            wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).unwrap();
        }
        wal_2.flush().unwrap();

        let (new_wal, new_mem_table) = WriteAheadLog::restore_from_dir(dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
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
