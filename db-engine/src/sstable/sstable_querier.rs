use anyhow::Result;
use std::path::PathBuf;

use crate::prelude::*;
use crate::utils;

use super::sstable_reader::SSTableReader;

pub struct SSTableQuerier {
    path_collection: Vec<PathBuf>,
}

impl SSTableQuerier {
    pub fn new(dir: &PathBuf) -> Result<Self> {
        let mut path_collection = utils::get_files_with_ext(dir, "db")?;
        path_collection.sort_by(|a, b| b.cmp(a));
        Ok(Self { path_collection })
    }

    pub fn query(&self, key: &[u8]) -> Option<Entry> {
        for p in self.path_collection.iter() {
            match SSTableReader::new(p) {
                Ok(mut reader) => {
                    let entry_opt = reader.get(key);
                    if entry_opt.is_some() {
                        return entry_opt;
                    }
                }
                Err(e) => {
                    eprintln!("{e:?}");
                    return None;
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::sstable::sstable_writer::SSTableWriter;

    use super::*;
    use anyhow::Result;
    use tempdir::TempDir;

    #[test]
    fn it_works() -> Result<()> {
        let temp_dir = TempDir::new("sstable_querier")?;
        let dir = temp_dir.path();
        let db_path_1 = dir.join("test.db");
        let db_path_2 = dir.join("test.db");

        // seed
        let entry_1 = Entry::new(b"test1".to_vec(), Some(b"hello").map(|i| i.to_vec()), 1);
        let entry_2 = Entry::new(b"test2".to_vec(), Some(b"hello").map(|i| i.to_vec()), 2);
        let mut sst_writer_1 = SSTableWriter::new(&db_path_1)?;
        sst_writer_1.set(&entry_1)?.flush()?;
        let mut sst_writer_2 = SSTableWriter::new(&db_path_2)?;
        sst_writer_2.set(&entry_2)?.flush()?;

        // test SSTableQuerier
        let querier = SSTableQuerier::new(&dir.to_path_buf())?;
        assert!(querier.query(b"test1").is_some());
        assert!(querier.query(b"test2").is_some());
        assert!(querier.query(b"test3").is_none());

        Ok(())
    }
}
