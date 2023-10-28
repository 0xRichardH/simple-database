use anyhow::Result;
use std::{
    fs::{remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{mem_table::MemTable, prelude::*, utils};

/// Write Ahead Log
pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    /// Creates a new WAL in a given directory.
    pub fn new(dir: &Path) -> Result<Self> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let path = Path::new(dir).join(format!("{}.wal", timestamp));
        Self::from_path(&path)
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            path: path.to_owned(),
        })
    }

    /// Restore our MemTable and WAL from a directory.
    /// We need to replay all of the operations.
    pub fn restore_from_dir(dir: &Path) -> Result<(WAL, MemTable)> {
        let mut wal_files = utils::get_files_with_ext(dir, "wal")?;
        wal_files.sort();

        let mut new_memtable = MemTable::new();
        let mut new_wal = WAL::new(dir)?;
        for file in wal_files.iter() {
            let wal = WAL::from_path(file)?;
            let wal_iter: WALIterator = wal.try_into()?;
            for entry in wal_iter {
                let key = entry.key.as_slice();
                let timestamp = entry.timestamp;
                if entry.is_deleted() {
                    new_wal.delete(key, timestamp)?;
                    new_memtable.delete(key, timestamp);
                } else {
                    let value = entry.value.unwrap().as_slice();
                    new_wal.set(key, value, timestamp)?;
                    new_memtable.set(key, value, timestamp);
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

impl TryFrom<WAL> for WALIterator {
    type Error = io::Error;

    fn try_from(value: WAL) -> std::result::Result<Self, Self::Error> {
        WALIterator::new(value.path)
    }
}
