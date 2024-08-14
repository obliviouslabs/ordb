use crate::params::PAGE_SIZE;
use crate::storage::BlockStorage;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::os::unix::prelude::FileExt;
use std::path::Path;

pub struct PageFile {
    file: std::fs::File,
    path: std::path::PathBuf,
}

impl BlockStorage for PageFile {
    fn open<P: AsRef<Path>>(path: P, total_pages: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Reserve space by setting the file length
        file.set_len((total_pages * PAGE_SIZE) as u64)?;

        Ok(PageFile {
            file,
            path: path.as_ref().to_path_buf(),
        })
    }

    // Write a single page
    fn write(&mut self, block_idx: usize, buf: &[u8]) -> io::Result<()> {
        let offset = block_idx * PAGE_SIZE;
        self.file.write_at(buf, offset as u64)?;
        Ok(())
    }

    // Read a single page
    fn read(&self, block_idx: usize, buf: &mut [u8]) -> io::Result<()> {
        let offset = block_idx * PAGE_SIZE;
        self.file.read_at(buf, offset as u64)?;
        Ok(())
    }
}

impl Drop for PageFile {
    fn drop(&mut self) {
        // delete the file when the PageFile instance goes out of scope
        let _ = std::fs::remove_file(&self.path);
    }
}
