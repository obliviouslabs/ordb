const MAX_ENTRY: usize = 32;
const PAGE_SIZE: usize = 4096;
const BUFFER_SIZE: usize = PAGE_SIZE
    - 2 * MAX_ENTRY * (std::mem::size_of::<HashEntry<usize>>() + std::mem::size_of::<u16>());
use std::hash::Hash;

use crate::cuckoo::HashEntry;
use crate::segvec::{SegmentedVector, MIN_SEGMENT_SIZE};
#[derive(Clone)]
struct Page {
    hashEntries: [HashEntry<usize>; MAX_ENTRY],
    offsets: [u16; MAX_ENTRY],
    buffer: [u8; BUFFER_SIZE],
}

impl Page {
    fn new() -> Self {
        Self {
            hashEntries: [HashEntry::new(); MAX_ENTRY],
            offsets: [0; MAX_ENTRY],
            buffer: [0; BUFFER_SIZE],
        }
    }

    pub fn read_and_remove(&mut self, entry: &HashEntry<usize>) -> Option<Vec<u8>> {
        for i in 0..MAX_ENTRY {
            if self.hashEntries[i] == *entry {
                let begin_offset = if i == 0 {
                    0 as usize
                } else {
                    self.offsets[i - 1] as usize
                };
                let end_offset = self.offsets[i] as usize;
                self.hashEntries[i] = HashEntry::new();
                return Some(self.buffer[begin_offset..end_offset].to_vec());
            }
        }
        None
    }

    fn is_empty_entry(entry: &HashEntry<usize>, page_idx: usize, page_num: usize) -> bool {
        if entry.is_empty() {
            return true;
        }
        if entry.get_val() % page_num != page_idx {
            // after rescaling, the entries with wrong page_num becomes empty
            return true;
        }
        false
    }

    // Compact the page by moving all the data to the beginning of the buffer
    // return the number of entries that are compacted
    pub fn compact(&mut self, page_idx: usize, page_num: usize) -> usize {
        let mut write_offset: u16 = 0;
        let mut read_offset: u16 = 0;
        let mut write_idx: usize = 0;
        for read_idx in 0..MAX_ENTRY {
            if self.offsets[read_idx] == 0 {
                // reaches end
                break;
            }
            let len = self.offsets[read_idx] - read_offset;
            if !Self::is_empty_entry(&self.hashEntries[read_idx], page_idx, page_num) {
                if len > 0 {
                    unsafe {
                        let src = self.buffer.as_ptr().offset(read_offset as isize);
                        let dst = self.buffer.as_mut_ptr().offset(write_offset as isize);
                        std::ptr::copy(src, dst, len as usize);
                    }
                }
                self.offsets[write_idx] = write_offset + len;
                self.hashEntries[write_idx] = self.hashEntries[read_idx];
                if read_idx != write_idx {
                    self.offsets[read_idx] = 0;
                    self.hashEntries[read_idx] = HashEntry::new();
                }
                write_offset += len;
                write_idx += 1;
            }
            read_offset += len;
        }
        write_idx as usize
    }

    pub fn insert(&mut self, write_idx: usize, entry: &HashEntry<usize>, value: &[u8]) -> bool {
        if write_idx >= MAX_ENTRY {
            return false;
        }
        let write_offset = if write_idx == 0 {
            0 as usize
        } else {
            self.offsets[write_idx - 1] as usize
        };
        let len = value.len();
        if write_offset + len > BUFFER_SIZE {
            return false;
        }
        self.offsets[write_idx] = (write_offset + len) as u16;
        self.hashEntries[write_idx] = *entry;
        unsafe {
            let src = value.as_ptr();
            let dst = self.buffer.as_mut_ptr().offset(write_offset as isize);
            std::ptr::copy_nonoverlapping(src, dst, len);
        }
        true
    }
}

pub struct PageOram {
    pages: SegmentedVector<Page>,
    stash: Vec<Vec<(HashEntry<usize>, Vec<u8>)>>,
}

impl PageOram {
    pub fn new() -> Self {
        let mut stash: Vec<Vec<(HashEntry<usize>, Vec<u8>)>> = Vec::with_capacity(MIN_SEGMENT_SIZE);
        for _ in 0..MIN_SEGMENT_SIZE {
            stash.push(Vec::new());
        }
        Self {
            pages: SegmentedVector::new(),
            stash,
        }
    }

    // read entry from the stash vec, update result, and evict the remaining entries to the page
    pub fn read_and_evict_stash(
        entry: &HashEntry<usize>,
        stash_vec_for_page: &mut Vec<(HashEntry<usize>, Vec<u8>)>,
        result: &mut Option<Vec<u8>>,
        page: &mut Page,
        page_idx: usize,
        page_num: usize,
    ) {
        // find the entry in the stash, don't remove it yet, just mark the index
        let mut read_idx = stash_vec_for_page.len();
        for i in 0..stash_vec_for_page.len() {
            let (e, value) = &stash_vec_for_page[i];
            if e == entry {
                read_idx = i;
                *result = Some(value.clone());
                break;
            }
        }

        let mut write_idx = page.compact(page_idx, page_num);
        // try to write stash vec to the page
        // loop backwards for efficient removal and consistent index
        for i in (0..stash_vec_for_page.len()).rev() {
            let (entry, value) = &stash_vec_for_page[i];
            // if it's the entry that we just read, just remove it
            if i != read_idx {
                if !page.insert(write_idx, entry, value) {
                    // page is full
                    if i > read_idx {
                        // now we need to remove the entry that we just read
                        stash_vec_for_page.remove(read_idx);
                    }
                    break;
                }
                write_idx += 1;
            }
            stash_vec_for_page.remove(i);
        }
    }

    pub fn read(&mut self, entry: &HashEntry<usize>, new_page_id: usize) -> Option<Vec<u8>> {
        let page_num = self.pages.capacity();
        let page_idx = entry.get_val() % page_num;
        let mut page = self.pages.get(page_idx).unwrap().clone();
        let mut result = page.read_and_remove(entry);
        // println!("result1: {:?}", result);

        let stash_vec_for_page = self.stash.get_mut(page_idx).unwrap();

        Self::read_and_evict_stash(
            entry,
            stash_vec_for_page,
            &mut result,
            &mut page,
            page_idx,
            page_num,
        );
        self.pages.set(page_idx, page);
        // println!("result2: {:?}", result);

        let mut new_entry = entry.clone();
        new_entry.set_val(new_page_id);
        let new_page_idx = new_page_id % page_num;
        if result.is_some() {
            let stash_vec_for_new_page = self.stash.get_mut(new_page_idx).unwrap();
            stash_vec_for_new_page.push((new_entry, result.clone().unwrap()));
        }
        result
    }

    pub fn write(
        &mut self,
        entry: &HashEntry<usize>,
        value: &Vec<u8>,
        new_page_id: usize,
    ) -> Option<Vec<u8>> {
        let page_num = self.pages.capacity();
        let page_idx = entry.get_val() % page_num;
        let mut page = self.pages.get(page_idx).unwrap().clone();
        let mut result = page.read_and_remove(entry);
        // println!("page_num: {}", page_num);
        // println!("page_idx: {}", page_idx);
        // println!("stash len: {}", self.stash.len());
        let stash_vec_for_page = self.stash.get_mut(page_idx).unwrap();
        // println!("result1: {:?}", result);
        Self::read_and_evict_stash(
            entry,
            stash_vec_for_page,
            &mut result,
            &mut page,
            page_idx,
            page_num,
        );
        self.pages.set(page_idx, page);
        // println!("result2: {:?}", result);
        let mut new_entry = entry.clone();
        new_entry.set_val(new_page_id);
        let new_page_idx = new_page_id % page_num;

        let stash_vec_for_new_page = self.stash.get_mut(new_page_idx).unwrap();
        stash_vec_for_new_page.push((new_entry, value.clone()));

        result
    }

    pub fn print_state(&self) {
        for i in 0..self.pages.capacity() {
            let page = self.pages.get(i).unwrap();
            println!("Page {}", i);
            for j in 0..MAX_ENTRY {
                let entry = &page.hashEntries[j];
                if !Page::is_empty_entry(entry, i, self.pages.capacity()) {
                    println!("Entry: {:?}", entry);
                }
            }
        }
        for i in 0..self.stash.len() {
            let stash_vec = &self.stash[i];
            if stash_vec.is_empty() {
                continue;
            }
            println!("Stash {}", i);
            for (entry, value) in stash_vec {
                println!("Entry: {:?}", entry);
            }
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_page_oram_simple() {
        let mut page_oram = PageOram::new();
        let mut entry = HashEntry::new();
        entry.set_idx([1, 2]);
        entry.set_val(1);
        let value = vec![1, 2, 3, 4];
        let new_page_id = 1;
        let result = page_oram.write(&entry, &value, new_page_id);
        assert_eq!(result, None);
        let result = page_oram.read(&entry, new_page_id);
        assert_eq!(result, Some(value));
    }
}
