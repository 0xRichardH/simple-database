use anyhow::Result;
use std::{
    fs::read_dir,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
};

/// Gets the set of files with an extension for a given directory.
pub fn get_files_with_ext(dir: &Path, ext: &str) -> Result<Vec<PathBuf>> {
    let files = read_dir(dir)?
        .filter_map(|file| file.ok())
        .map(|file| file.path())
        .filter(|path| path.extension().map_or(false, |e| e == ext))
        .collect::<Vec<_>>();
    Ok(files)
}

/// Get the set of files with and extension and size for a given directory.
pub fn get_files_with_ext_and_size(dir: &Path, ext: &str, size: u64) -> Result<Vec<PathBuf>> {
    let files = read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|file| file.extension().map_or(false, |e| e == ext))
        .filter(|file| file.metadata().map_or(false, |m| m.size() < size))
        .collect::<Vec<_>>();

    Ok(files)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use std::{fs::File, io::Write};

    #[test]
    fn test_get_files_with_ext() -> Result<()> {
        let dir = TempDir::new("utils")?;
        let dir_path = dir.path();

        // Create sample files
        File::create(dir_path.join("file1.txt"))?;
        File::create(dir_path.join("file2.txt"))?;
        File::create(dir_path.join("image.png"))?;
        File::create(dir_path.join("document.pdf"))?;

        // Call your function
        let txt_files = get_files_with_ext(dir_path, "txt")?;

        // Assertions
        assert_eq!(txt_files.len(), 2);
        assert!(txt_files
            .iter()
            .all(|path| path.extension().unwrap() == "txt"));

        Ok(())
    }

    #[test]
    fn test_get_files_with_ext_with_empty_directory() -> Result<()> {
        let dir = TempDir::new("utils")?;
        let files = get_files_with_ext(dir.path(), "txt")?;
        assert!(files.is_empty());
        Ok(())
    }

    #[test]
    fn test_get_files_with_ext_with_no_matching_files() -> Result<()> {
        let dir = TempDir::new("utils")?;
        File::create(dir.path().join("image.png"))?;
        File::create(dir.path().join("document.pdf"))?;

        let files = get_files_with_ext(dir.path(), "txt")?;
        assert!(files.is_empty());
        Ok(())
    }

    #[test]
    fn test_get_files_with_ext_with_nonexistent_directory() {
        let dir = Path::new("/path/that/does/not/exist");
        let result = get_files_with_ext(dir, "txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_unusual_extension() -> Result<()> {
        let dir = TempDir::new("utils")?;
        File::create(dir.path().join("file.weirdextension"))?;

        let files = get_files_with_ext(dir.path(), "weirdextension")?;
        assert_eq!(files.len(), 1);
        assert!(files[0].extension().unwrap() == "weirdextension");
        Ok(())
    }

    #[test]
    fn test_get_files_with_ext_and_size() {
        let dir = TempDir::new("utils").unwrap();
        let file_path1 = dir.path().join("test1.txt");
        let file_path2 = dir.path().join("test2.txt");
        let file_path3 = dir.path().join("test3.jpg");

        // Create test files
        let mut file1 = File::create(&file_path1).unwrap();
        file1.write_all(b"Hello").unwrap(); // 5 bytes

        let mut file2 = File::create(file_path2).unwrap();
        file2.write_all(b"HelloWorld").unwrap(); // 10 bytes

        File::create(file_path3).unwrap();

        // Test with specific extension and size
        let result = get_files_with_ext_and_size(dir.path(), "txt", 6).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&file_path1));
    }
}
