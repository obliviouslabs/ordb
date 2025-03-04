use crate::params::{ENCRYPT_FLAG, KEY_SIZE, PAGE_SIZE};
use crate::storage::storage::BlockStorage;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use bytemuck::{Pod, Zeroable};
use rand::Rng;
use std::convert::TryFrom;
#[derive(Clone, Copy)]
struct EncPage {
    data: [u8; PAGE_SIZE],
}
pub struct EncVec<T: Clone + Pod + Zeroable, StoreT: BlockStorage> {
    file_pages: StoreT,
    size: usize,
    cipher: Aes256Gcm,
    phantom: std::marker::PhantomData<T>,
}

impl EncPage {
    pub fn new() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }
}

impl<T: Clone + Pod + Zeroable, StoreT: BlockStorage> EncVec<T, StoreT> {
    pub fn new(size: usize, raw_key: &[u8; KEY_SIZE]) -> Self {
        let key = (*raw_key).into();
        let filename_uid = rand::thread_rng().gen::<u64>();
        let filename = format!("encvec_{}.dat", filename_uid);
        Self {
            file_pages: StoreT::open(filename, size).unwrap(),
            size,
            cipher: Aes256Gcm::new(&key),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, index: usize, nonce: u32) -> Option<T> {
        if index < self.size {
            // perform an AES-NI decryption
            let mut page = EncPage::new();
            let err = self.file_pages.read(index, &mut page.data);
            if err.is_err() {
                panic!("read error: {:?}", err);
            }
            let mut nonce_bytes = [0u8; 12];
            nonce_bytes[0..4].copy_from_slice(&nonce.to_ne_bytes());

            let nonce = Nonce::from_slice(&nonce_bytes);
            let len_bytes = [page.data[0], page.data[1]];
            let len = u16::from_ne_bytes(len_bytes);
            if len == 0 {
                return Some(unsafe { std::mem::zeroed() });
            }
            if ENCRYPT_FLAG {
                let decrypted_plaintext = self
                    .cipher
                    .decrypt(&nonce, page.data[2..2 + len as usize].as_ref())
                    .expect("decryption failure!");

                let decrypted_data: &T = bytemuck::from_bytes(&decrypted_plaintext);
                Some(*decrypted_data)
            } else {
                let decrypted_data: &T = bytemuck::from_bytes(&page.data[8..8 + len as usize]);
                Some(*decrypted_data)
            }
        } else {
            None
        }
    }

    pub fn put(&self, index: usize, value: &T, nonce: u32) {
        if index < self.size {
            // perform an AES-NI encryption
            let mut page = EncPage::new();
            let mut nonce_bytes = [0u8; 12];
            nonce_bytes[0..4].copy_from_slice(&nonce.to_ne_bytes());
            let nonce = Nonce::from_slice(&nonce_bytes);

            if ENCRYPT_FLAG {
                let encrypted_data = self
                    .cipher
                    .encrypt(&nonce, bytemuck::cast_slice(&[*value]))
                    .expect("encryption failure!");
                page.data[0..2].copy_from_slice(&(encrypted_data.len() as u16).to_ne_bytes());
                page.data[2..encrypted_data.len() + 2].copy_from_slice(encrypted_data.as_ref());
            } else {
                let len = std::mem::size_of::<T>() as u16;
                page.data[0..2].copy_from_slice(&len.to_ne_bytes());
                page.data[8..len as usize + 8].copy_from_slice(bytemuck::cast_slice(&[*value]));
            }
            let err = self.file_pages.write(index, &page.data);
            if err.is_err() {
                panic!("write error: {:?}", err);
            }
        }
    }

    pub fn raw_get(&self, index: usize) -> Option<[u8; PAGE_SIZE]> {
        if index < self.size {
            let mut page = [0; PAGE_SIZE];
            let err = self.file_pages.read(index, &mut page);
            if err.is_err() {
                panic!("read error: {:?}", err);
            }
            Some(page)
        } else {
            None
        }
    }

    pub fn raw_put(&self, index: usize, value: &[u8; PAGE_SIZE]) {
        if index < self.size {
            let err = self.file_pages.write(index, value);
            if err.is_err() {
                panic!("write error: {:?}", err);
            }
        }
    }
}

mod tests {
    use crate::params::PAGE_SIZE;
    use crate::storage::pagefile::PageFile;
    use crate::tree::encvec::EncVec;

    #[test]
    fn it_works() {
        let vec = EncVec::<u128, PageFile>::new(1024, &[0u8; 32]);
        vec.put(0, &42, 123);
        assert_eq!(vec.get(0, 123), Some(42));
    }

    #[derive(Clone, Copy)]
    struct TestBuffer {
        data: [u8; PAGE_SIZE - 64],
    }

    impl Default for TestBuffer {
        fn default() -> Self {
            Self {
                data: [0; PAGE_SIZE - 64],
            }
        }
    }

    unsafe impl bytemuck::Pod for TestBuffer {}
    unsafe impl bytemuck::Zeroable for TestBuffer {}

    #[test]
    fn enc_perf_test() {
        let num_pages = 1e6 as usize;
        const BUFFER_SIZE: usize = PAGE_SIZE - 64;
        let mut vec = EncVec::<TestBuffer, PageFile>::new(PAGE_SIZE, &[0u8; 32]);
        for round in 0..num_pages {
            let mut buffer = TestBuffer::default();
            for i in 0..8 {
                buffer.data[i] = (round >> (i * 8)) as u8;
            }
            vec.put(round % BUFFER_SIZE, &buffer, 1);
        }
    }
}
