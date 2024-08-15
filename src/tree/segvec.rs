use std::usize::MIN;

use super::encvec::EncVec;
use crate::params::MIN_SEGMENT_SIZE;
use crate::storage::memstore::MemStore;
use crate::storage::pagefile::PageFile;
use bytemuck::{Pod, Zeroable};

pub struct SegmentedVec<T: Clone + Pod + Zeroable> {
    segments: Vec<EncVec<T, MemStore>>,
    pub versions: Vec<u8>,
    nonce: Vec<u32>,
    size: usize,
    log_size: u8,
}

impl<T: Clone + Pod + Zeroable> SegmentedVec<T> {
    pub fn new() -> Self {
        println!("Creating new SegmentedVec");
        let initial_segment = EncVec::new(MIN_SEGMENT_SIZE, &[0; 32]);
        let init_version = MIN_SEGMENT_SIZE.trailing_zeros() as u8;
        let mut nonce = vec![0; MIN_SEGMENT_SIZE];
        for i in 0..MIN_SEGMENT_SIZE {
            nonce[i] = rand::random();
        }
        Self {
            segments: vec![initial_segment],
            size: MIN_SEGMENT_SIZE,
            log_size: init_version,
            versions: vec![init_version; MIN_SEGMENT_SIZE],
            nonce,
        }
    }

    fn double_size(&mut self) {
        let new_segment = EncVec::new(self.size, &[0; 32]);
        self.segments.push(new_segment);
        self.size *= 2;
        self.log_size += 1;
    }

    // pub fn double_size_and_fork_self(&mut self) {
    //     let mut new_segment = vec![unsafe { mem::zeroed() }; self.size];
    //     let mut offset: isize = 0;
    //     for seg in self.segments.iter_mut() {
    //         // memcpy the old segment to the new segment starting from offset
    //         let len = seg.len();
    //         let src = seg.as_ptr();
    //         unsafe {
    //             let dst = (new_segment.as_mut_ptr() as *mut T).offset(offset);
    //             std::ptr::copy_nonoverlapping(src, dst, len);
    //         }
    //         offset += len as isize;
    //     }
    //     self.segments.push(new_segment);
    //     self.size *= 2;
    // }
    pub fn double_size_and_fork_self(&mut self) {
        let original_size = self.size;
        self.double_size();
        self.versions.extend_from_within(0..original_size);
        // self.versions.resize(self.size, 0);
        self.nonce.resize(self.size, 0);
    }

    fn inner_indices(&self, index: usize) -> (usize, usize) {
        let segment_index_power_two = (index / MIN_SEGMENT_SIZE) as u64;
        let segment_index = (u64::BITS - segment_index_power_two.leading_zeros()) as usize;
        let within_segment_index = index - ((1 << segment_index) / 2) * MIN_SEGMENT_SIZE;
        (segment_index, within_segment_index)
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if index >= self.size {
            return None;
        }
        let version = self.versions[index];
        let actual_index = index & ((1 << version) - 1);
        let (segment_index, within_segment_index) = self.inner_indices(actual_index);
        self.segments[segment_index].get(within_segment_index, self.nonce[actual_index])
    }

    pub fn set(&mut self, index: usize, value: &T) {
        if index >= self.size {
            return;
        }
        let version = self.versions[index];
        let version_size = 1 << version;
        if version_size != self.size {
            // fork the original version to other indices
            let original_index = index & (version_size - 1);
            // TODO: avoid decrypt and re-encrypt
            let (from_segment_index, from_within_segment_index) =
                self.inner_indices(original_index);
            let original_value = self.segments[from_segment_index]
                .raw_get(from_within_segment_index)
                .unwrap();
            self.versions[original_index] = self.log_size;
            let mut to_idx = original_index + version_size;
            while to_idx < self.size {
                if to_idx != index {
                    let (to_segment_index, to_within_segment_index) = self.inner_indices(to_idx);
                    self.segments[to_segment_index]
                        .raw_put(to_within_segment_index, &original_value);
                }
                self.versions[to_idx] = self.log_size;
                self.nonce[to_idx] = self.nonce[original_index];
                to_idx += version_size;
            }
        }
        let (segment_index, within_segment_index) = self.inner_indices(index);
        self.nonce[index] = rand::random();
        self.segments[segment_index].put(within_segment_index, value, self.nonce[index]);
    }

    pub fn capacity(&self) -> usize {
        self.size
    }
}

mod tests {
    use crate::params::MIN_SEGMENT_SIZE;
    use crate::tree::segvec::SegmentedVec;
    #[test]
    fn it_works() {
        let mut vec = SegmentedVec::<u128>::new();
        vec.double_size_and_fork_self();
        vec.double_size_and_fork_self();
        vec.set(0, &42);
        assert_eq!(vec.get(0), Some(42));
        vec.set(MIN_SEGMENT_SIZE - 1, &43);
        assert_eq!(vec.get(MIN_SEGMENT_SIZE - 1), Some(43));
        vec.set(MIN_SEGMENT_SIZE, &44);
        assert_eq!(vec.get(MIN_SEGMENT_SIZE), Some(44));
        vec.set(MIN_SEGMENT_SIZE * 2 - 1, &45);
        assert_eq!(vec.get(MIN_SEGMENT_SIZE * 2 - 1), Some(45));
        vec.set(MIN_SEGMENT_SIZE * 2, &46);
        assert_eq!(vec.get(MIN_SEGMENT_SIZE * 2), Some(46));
        vec.set(MIN_SEGMENT_SIZE * 4 - 1, &47);
        assert_eq!(vec.get(MIN_SEGMENT_SIZE * 4 - 1), Some(47));
        assert_eq!(vec.get(MIN_SEGMENT_SIZE * 4), None);
    }
}
