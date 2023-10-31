use std::path::PathBuf;

pub struct SSTableQuerier {
    dir: PathBuf,
}

impl SSTableQuerier {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
}
