use std::vec;

use crate::params::{KEY_SIZE, PAGE_SIZE};
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use bytemuck::{Pod, Zeroable};
use rand::RngCore;
#[derive(Clone, Copy)]
struct EncPage {
    data: [u8; PAGE_SIZE],
}
pub struct EncVec<T: Clone + Pod + Zeroable> {
    pages: Vec<EncPage>,
    key: Key<Aes256Gcm>,
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
            key,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if index < self.pages.len() {
            // perform an AES-NI decryption
            let page = &self.pages[index];
            let nonce_bytes = [0u8; 12];
            let nonce = Nonce::from_slice(&nonce_bytes);
            let cipher = Aes256Gcm::new(&self.key);
            let len_bytes = [page.data[0], page.data[1]];
            let len = u16::from_ne_bytes(len_bytes);
            if len == 0 {
                return Some(unsafe { std::mem::zeroed() });
            }
            let decrypted_plaintext = cipher
                .decrypt(nonce, page.data[2..2 + len as usize].as_ref())
                .expect("decryption failure!");
            // let unaligned_decrypted_plaintext = &page.data[2..2 + len as usize];
            // let mut decrypted_plaintext = vec![0u8; len as usize];
            // decrypted_plaintext.copy_from_slice(unaligned_decrypted_plaintext);

            let decrypted_data: &T = bytemuck::from_bytes(&decrypted_plaintext);
            Some(*decrypted_data)
        } else {
            None
        }
    }

    pub fn put(&mut self, index: usize, value: T) {
        if index < self.pages.len() {
            // perform an AES-NI encryption
            let page = &mut self.pages[index];
            let nonce_bytes = [0u8; 12];
            let nonce = Nonce::from_slice(&nonce_bytes);
            let cipher = Aes256Gcm::new(&self.key);
            let encrypted_data = cipher
                .encrypt(nonce, bytemuck::cast_slice(&[value]))
                .expect("encryption failure!");
            // let val_slice = &[value];
            // let encrypted_data = bytemuck::cast_slice(val_slice);
            page.data[0..2].copy_from_slice(&(encrypted_data.len() as u16).to_ne_bytes());
            page.data[2..encrypted_data.len() + 2].copy_from_slice(encrypted_data.as_ref());
        }
    }
}

mod tests {
    use crate::encvec::EncVec;

    #[test]
    fn it_works() {
        let mut vec = EncVec::<u128>::new(1024, &[0u8; 32]);
        vec.put(0, 42);
        assert_eq!(vec.get(0), Some(42));
    }
}
