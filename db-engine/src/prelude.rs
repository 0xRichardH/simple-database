/// Data Entry
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // the vaule will be None when the entry is deleted
    pub timestamp: u128,
}

impl Entry {
    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }
}
