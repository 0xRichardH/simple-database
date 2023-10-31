use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid Path: {0}")]
    InvalidPath(PathBuf),
}

