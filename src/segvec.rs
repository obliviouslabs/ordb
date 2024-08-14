use crate::encvec::EncVec;
use crate::pagefile::PageFile;
use crate::params::MIN_SEGMENT_SIZE;
use bytemuck::{Pod, Zeroable};
use std::sync::RwLock;

pub struct SegmentedVec<T: Clone + Pod + Zeroable> {
    segments: Vec<EncVec<T, PageFile>>,
    pub versions: RwLock<Vec<u8>>,
    size: usize,
    log_size: u8,
}

impl<T: Clone + Pod + Zeroable> SegmentedVec<T> {
    pub fn new() -> Self {
        println!("Creating new SegmentedVec");
        let initial_segment = EncVec::new(MIN_SEGMENT_SIZE, &[0; 32]);
        let init_version = MIN_SEGMENT_SIZE.trailing_zeros() as u8;
        Self {
            segments: vec![initial_segment],
            size: MIN_SEGMENT_SIZE,
            log_size: init_version,
            versions: RwLock::new(vec![init_version; MIN_SEGMENT_SIZE]),
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
        self.double_size();
        self.versions
            .write()
            .unwrap()
            .extend_from_within(0..self.size);
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
        let version_raii = self.versions.read().unwrap();
        let version = version_raii[index];
        let actual_index = index & ((1 << version) - 1);
        let (segment_index, within_segment_index) = self.inner_indices(actual_index);
        self.segments[segment_index].get(within_segment_index)
    }

    pub fn set(&self, index: usize, value: &T) {
        if index >= self.size {
            return;
        }
        let mut versions = self.versions.write().unwrap();
        let version = versions[index];
        let version_size = 1 << version;
        if version_size != self.size {
            // fork the original version to other indices
            let original_index = index & (version_size - 1);
            // TODO: avoid decrypt and re-encrypt
            let original_value = self.get(original_index).unwrap();
            versions[original_index] = self.log_size;
            let mut to_idx = original_index + version_size;
            while to_idx < self.size {
                if to_idx != index {
                    let (to_segment_index, to_within_segment_index) = self.inner_indices(to_idx);
                    self.segments[to_segment_index].put(to_within_segment_index, &original_value);
                }
                versions[to_idx] = self.log_size;
                to_idx += version_size;
            }
        }
        let (segment_index, within_segment_index) = self.inner_indices(index);
        self.segments[segment_index].put(within_segment_index, value);
    }

    pub fn capacity(&self) -> usize {
        self.size
    }
}

mod tests {
    use crate::segvec::SegmentedVec;

    #[test]
    fn it_works() {
        let mut vec = SegmentedVec::<u128>::new();
        vec.double_size_and_fork_self();
        vec.double_size_and_fork_self();
        vec.set(0, &42);
        assert_eq!(vec.get(0), Some(42));
        vec.set(1023, &43);
        assert_eq!(vec.get(1023), Some(43));
        vec.set(1024, &44);
        assert_eq!(vec.get(1024), Some(44));
        vec.set(2047, &45);
        assert_eq!(vec.get(2047), Some(45));
        vec.set(2048, &46);
        assert_eq!(vec.get(2048), Some(46));
        vec.set(4095, &47);
        assert_eq!(vec.get(4095), Some(47));
        assert_eq!(vec.get(4096), None);
    }
}
