use std::io::{BufReader, BufWriter, Read, Write};

/// Data Entry
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // the vaule will be None when the entry is deleted
    pub timestamp: u128,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u128) -> Self {
        Self {
            key,
            value,
            timestamp,
        }
    }

    /// Get the Entry object from BufReader.
    pub fn read_from<R: Read>(reader: &mut BufReader<R>) -> Option<Self> {
        // key
        let mut key_len_buffers = [0; 8];
        if reader.read_exact(&mut key_len_buffers).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(key_len_buffers);
        let mut key = vec![0; key_len];
        if reader.read_exact(&mut key).is_err() {
            return None;
        }

        // is_deleted
        let mut bool_buffers = [0; 1];
        if reader.read_exact(&mut bool_buffers).is_err() {
            return None;
        }
        let is_deleted = bool_buffers[0] != 0;

        // value
        let mut value = None;
        if !is_deleted {
            let mut value_len_buffers = [0; 8];
            if reader.read_exact(&mut value_len_buffers).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(value_len_buffers);
            let mut value_buf = vec![0; value_len];
            if reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }

        // timestamp
        let mut timestamp_buffers = [0; 16];
        if reader.read_exact(&mut timestamp_buffers).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffers);

        Some(Self {
            key,
            value,
            timestamp,
        })
    }

    /// To check if the entry is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    /// Write the Entry object to BufWriter.
    pub fn write_to<W: Write>(&self, writer: &mut BufWriter<W>) -> std::io::Result<()> {
        // key
        writer.write_all(&self.key.len().to_le_bytes())?;
        writer.write_all(&self.key)?;

        // is_deleted
        let is_deleted: u8 = self.is_deleted().into();
        writer.write_all(&is_deleted.to_le_bytes())?;

        // value
        if let Some(val) = &self.value {
            writer.write_all(&val.len().to_le_bytes())?;
            writer.write_all(val)?;
        }

        // timestamp
        writer.write_all(&self.timestamp.to_le_bytes())?;

        Ok(())
    }
}
