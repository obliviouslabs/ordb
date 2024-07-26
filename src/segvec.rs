use std::mem;

pub const MIN_SEGMENT_SIZE: usize = 1024; // Example segment size

pub struct SegmentedVector<T: Clone> {
    segments: Vec<Vec<T>>,
    versions: Vec<u8>,
    size: usize,
    log_size: u8,
}

impl<T: Clone> SegmentedVector<T> {
    pub fn new() -> Self {
        println!("Creating new SegmentedVector");
        let initial_segment = vec![unsafe { mem::zeroed() }; MIN_SEGMENT_SIZE];
        let init_version = MIN_SEGMENT_SIZE.trailing_zeros() as u8;
        Self {
            segments: vec![initial_segment],
            size: MIN_SEGMENT_SIZE,
            log_size: init_version,
            versions: vec![init_version; MIN_SEGMENT_SIZE],
        }
    }

    fn double_size(&mut self) {
        let new_segment = vec![unsafe { mem::zeroed() }; self.size];
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
        self.versions.resize(self.size, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.versions.as_ptr(),
                self.versions.as_mut_ptr().offset(original_size as isize),
                original_size,
            );
        }
    }

    fn inner_indices(&self, index: usize) -> (usize, usize) {
        let segment_index_power_two = (index / MIN_SEGMENT_SIZE) as u64;
        let segment_index = (u64::BITS - segment_index_power_two.leading_zeros()) as usize;
        let within_segment_index = index - ((1 << segment_index) / 2) * MIN_SEGMENT_SIZE;
        (segment_index, within_segment_index)
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.size {
            return None;
        }
        let version = self.versions[index];
        let actual_index = index & ((1 << version) - 1);
        let (segment_index, within_segment_index) = self.inner_indices(actual_index);
        Some(&self.segments[segment_index][within_segment_index])
    }

    pub fn set(&mut self, index: usize, value: &T) {
        self.set_move(index, value.clone());
    }

    pub fn set_move(&mut self, index: usize, value: T) {
        if index >= self.size {
            return;
        }
        let version = self.versions[index];
        let version_size = 1 << version;
        if version_size != self.size {
            // fork the original version to other indices
            let original_index = index & (version_size - 1);
            let (segment_index, within_segment_index) = self.inner_indices(original_index);
            let original_value_ptr = unsafe {
                self.segments[segment_index]
                    .as_mut_ptr()
                    .offset(within_segment_index as isize)
            };
            self.versions[original_index] = self.log_size;
            let mut to_idx = original_index + version_size;
            while to_idx < self.size {
                if to_idx != index {
                    let (to_segment_index, to_within_segment_index) = self.inner_indices(to_idx);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            original_value_ptr,
                            self.segments[to_segment_index]
                                .as_mut_ptr()
                                .offset(to_within_segment_index as isize),
                            1,
                        );
                    }
                }
                self.versions[to_idx] = self.log_size;
                to_idx += version_size;
            }
        }
        // calculate log of index
        let (segment_index, within_segment_index) = self.inner_indices(index);
        self.segments[segment_index][within_segment_index] = value;
    }

    pub fn capacity(&self) -> usize {
        self.size
    }
}

mod tests {
    use crate::segvec::SegmentedVector;

    #[test]
    fn it_works() {
        let mut vec = SegmentedVector::<u128>::new();
        vec.double_size_and_fork_self();
        vec.double_size_and_fork_self();
        vec.set(0, &42);
        assert_eq!(vec.get(0), Some(&42));
        vec.set(1023, &43);
        assert_eq!(vec.get(1023), Some(&43));
        vec.set(1024, &44);
        assert_eq!(vec.get(1024), Some(&44));
        vec.set(2047, &45);
        assert_eq!(vec.get(2047), Some(&45));
        vec.set(2048, &46);
        assert_eq!(vec.get(2048), Some(&46));
        vec.set(4095, &47);
        assert_eq!(vec.get(4095), Some(&47));
        assert_eq!(vec.get(4096), None);
    }
}
