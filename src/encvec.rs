use std::vec;

use crate::params::{KEY_SIZE, PAGE_SIZE};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use bytemuck::{Pod, Zeroable};
#[derive(Clone, Copy)]
struct EncPage {
    data: [u8; PAGE_SIZE],
}
pub struct EncVec<T: Clone + Pod + Zeroable> {
    pages: Vec<EncPage>,
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

impl<T: Clone + Pod + Zeroable> EncVec<T> {
    pub fn new(size: usize, raw_key: &[u8; KEY_SIZE]) -> Self {
        let key = (*raw_key).into();
        Self {
            pages: vec![EncPage::new(); size],
            cipher: Aes256Gcm::new(&key),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if index < self.pages.len() {
            // perform an AES-NI decryption
            let page = &self.pages[index];
            let nonce_bytes = [0u8; 12];
            let nonce = Nonce::from_slice(&nonce_bytes);
            let len_bytes = [page.data[0], page.data[1]];
            let len = u16::from_ne_bytes(len_bytes);
            if len == 0 {
                return Some(unsafe { std::mem::zeroed() });
            }
            let decrypted_plaintext = self
                .cipher
                .decrypt(nonce, page.data[2..2 + len as usize].as_ref())
                .expect("decryption failure!");

            let decrypted_data: &T = bytemuck::from_bytes(&decrypted_plaintext);
            Some(*decrypted_data)
        } else {
            None
        }
    }

    pub fn put(&mut self, index: usize, value: &T) {
        if index < self.pages.len() {
            // perform an AES-NI encryption
            let page = &mut self.pages[index];
            let nonce_bytes = [0u8; 12];
            let nonce = Nonce::from_slice(&nonce_bytes);
            let encrypted_data = self
                .cipher
                .encrypt(nonce, bytemuck::cast_slice(&[*value]))
                .expect("encryption failure!");
            page.data[0..2].copy_from_slice(&(encrypted_data.len() as u16).to_ne_bytes());
            page.data[2..encrypted_data.len() + 2].copy_from_slice(encrypted_data.as_ref());
        }
    }
}

mod tests {
    use crate::encvec::EncVec;
    use crate::params::PAGE_SIZE;

    #[test]
    fn it_works() {
        let mut vec = EncVec::<u128>::new(1024, &[0u8; 32]);
        vec.put(0, &42);
        assert_eq!(vec.get(0), Some(42));
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
        let mut vec = EncVec::<TestBuffer>::new(PAGE_SIZE, &[0u8; 32]);
        for round in 0..num_pages {
            let mut buffer = TestBuffer::default();
            for i in 0..8 {
                buffer.data[i] = (round >> (i * 8)) as u8;
            }
            vec.put(round % BUFFER_SIZE, &buffer);
        }
    }
}
