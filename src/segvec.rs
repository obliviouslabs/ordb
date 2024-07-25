use std::mem;

pub const MIN_SEGMENT_SIZE: usize = 1024; // Example segment size

pub struct SegmentedVector<T: Clone> {
    segments: Vec<Vec<T>>,
    size: usize,
}

impl<T: Clone> SegmentedVector<T> {
    pub fn new() -> Self {
        println!("Creating new SegmentedVector");
        let initial_segment = vec![unsafe { mem::zeroed() }; MIN_SEGMENT_SIZE];
        Self {
            segments: vec![initial_segment],
            size: MIN_SEGMENT_SIZE,
        }
    }

    pub fn double_size(&mut self) {
        let new_segment = vec![unsafe { mem::zeroed() }; self.size];
        self.segments.push(new_segment);
        self.size *= 2;
    }

    pub fn double_size_and_fork_self(&mut self) {
        let mut new_segment = vec![unsafe { mem::zeroed() }; self.size];
        let mut offset: isize = 0;
        for seg in self.segments.iter_mut() {
            // memcpy the old segment to the new segment starting from offset
            let len = seg.len();
            let src = seg.as_ptr();
            unsafe {
                let dst = (new_segment.as_mut_ptr() as *mut T).offset(offset);
                std::ptr::copy_nonoverlapping(src, dst, len);
            }
            offset += len as isize;
        }
        self.segments.push(new_segment);
        self.size *= 2;
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
        let (segment_index, within_segment_index) = self.inner_indices(index);
        Some(&self.segments[segment_index][within_segment_index])
    }

    pub fn set(&mut self, index: usize, value: &T) {
        if index >= self.size {
            return;
        }
        // calculate log of index
        let (segment_index, within_segment_index) = self.inner_indices(index);
        self.segments[segment_index][within_segment_index] = value.clone();
    }

    pub fn set_move(&mut self, index: usize, value: T) {
        if index >= self.size {
            return;
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
        vec.double_size();
        vec.double_size();
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
