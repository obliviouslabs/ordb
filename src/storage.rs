use std::io;
use std::path::Path;
pub trait BlockStorage {
    fn open<P: AsRef<Path>>(path: P, total_pages: usize) -> io::Result<Self>
    where
        Self: Sized;
    fn read(&self, block_idx: usize, buf: &mut [u8]) -> io::Result<()>;
    fn write(&mut self, block_idx: usize, buf: &[u8]) -> io::Result<()>;
}
