use crate::params::PAGE_SIZE;
use crate::storage::storage::BlockStorage;
use std::io;
use std::path::Path;
use std::sync::RwLock;
pub struct MemStore {
    data: RwLock<Vec<u8>>,
}

impl BlockStorage for MemStore {
    fn open<P: AsRef<Path>>(_path: P, total_pages: usize) -> io::Result<Self> {
        Ok(MemStore {
            data: RwLock::new(vec![0; total_pages * PAGE_SIZE]),
        })
    }

    fn read(&self, block_idx: usize, buf: &mut [u8]) -> io::Result<()> {
        let start = block_idx * PAGE_SIZE;
        let end = start + PAGE_SIZE;
        buf.copy_from_slice(&self.data.read().unwrap()[start..end]);
        Ok(())
    }

    fn write(&self, block_idx: usize, buf: &[u8]) -> io::Result<()> {
        let start = block_idx * PAGE_SIZE;
        let end = start + PAGE_SIZE;
        self.data.write().unwrap()[start..end].copy_from_slice(buf);
        Ok(())
    }
}
