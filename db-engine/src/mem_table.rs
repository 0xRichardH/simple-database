use crate::prelude::*;

/// Timestamp size (16 bytes)
const TIMESTAMP_SIZE: usize = 16;

/// Tombstone size (1 byte)
const TOMBSTONE_SIZE: usize = 1;

/// MemTable holds a sorted list of the latest writes.
pub struct MemTable {
    entries: Vec<Entry>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: 0,
        }
    }

    /// Get Key-Value pair record in MemTable.
    ///
    /// Return None if no record is being found.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }

        None
    }

    /// Set Key-Value pair in MemTable.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = Entry::new(key.to_vec(), Some(value.to_vec()), timestamp);
        let key_size = key.len();
        let value_size = value.len();

        match self.get_index(key) {
            Ok(idx) => {
                // update exists entry
                if let Some(v) = self.entries[idx].value.as_ref() {
                    self.size -= v.len();
                }
                self.size += value_size;
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // create new entry
                self.entries.insert(idx, entry);
                self.size += key_size + value_size + TIMESTAMP_SIZE + TOMBSTONE_SIZE;
            }
        }
    }

    /// Delete Key-Value pair in MemTable.
    /// The deletion is done by Tombstone.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = Entry {
            key: key.to_vec(),
            value: None,
            timestamp,
        };
        let key_size = key.len();

        match self.get_index(key) {
            Ok(idx) => {
                // update exists entry
                if let Some(v) = self.entries[idx].value.as_ref() {
                    self.size -= v.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // create new entry
                self.entries.insert(idx, entry);
                self.size += key_size + TIMESTAMP_SIZE + TOMBSTONE_SIZE;
            }
        }
    }

    /// Perform the binary search to find the index of the key
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |entry| entry.key.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_table_put_start() {
        let mut table = MemTable::new();
        table.set(b"Lime", b"Lime Smoothie", 0); // 17 + 16 + 1
        table.set(b"Orange", b"Orange Smoothie", 10); // 21 + 16 + 1

        table.set(b"Apple", b"Apple Smoothie", 20); // 19 + 16 + 1

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 20);
        assert!(!table.entries[0].is_deleted());
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 0);
        assert!(!table.entries[1].is_deleted());
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert!(!table.entries[2].is_deleted());

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_middle() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 10);

        table.set(b"Lime", b"Lime Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].is_deleted());
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 20);
        assert!(!table.entries[1].is_deleted());
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert!(!table.entries[2].is_deleted());

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_end() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);

        table.set(b"Orange", b"Orange Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].is_deleted());
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 10);
        assert!(!table.entries[1].is_deleted());
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert!(!table.entries[2].is_deleted());

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_overwrite() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        table.set(b"Lime", b"A sour fruit", 30);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].is_deleted());
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
        assert_eq!(table.entries[1].timestamp, 30);
        assert!(!table.entries[1].is_deleted());
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert!(!table.entries[2].is_deleted());

        assert_eq!(table.size, 107);
    }

    #[test]
    fn test_mem_table_get_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        let entry = table.get(b"Orange").unwrap();

        assert_eq!(entry.key, b"Orange");
        assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(entry.timestamp, 20);
    }

    #[test]
    fn test_mem_table_get_not_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 0);

        let res = table.get(b"Potato");
        assert!(res.is_none());
    }

    #[test]
    fn test_mem_table_delete_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert!(res.is_deleted());

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(table.entries[0].is_deleted());

        assert_eq!(table.size, 22);
    }

    #[test]
    fn test_mem_table_delete_empty() {
        let mut table = MemTable::new();

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert!(res.is_deleted());

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(table.entries[0].is_deleted());

        assert_eq!(table.size, 22);
    }
}
