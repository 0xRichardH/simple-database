use anyhow::Result;
use std::{
    fs::read_dir,
    path::{Path, PathBuf},
};

/// Gets the set of files with an extension for a given directory.
pub fn get_files_with_ext(dir: &Path, ext: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for file in read_dir(dir)? {
        let path = file?.path();
        if let Some(e) = path.extension() {
            if e == ext {
                files.push(path);
            }
        }
    }

    Ok(files)
}
