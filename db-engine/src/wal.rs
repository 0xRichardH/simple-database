use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::prelude::*;

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
        Self::from_path(path)
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self { writer, path })
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
