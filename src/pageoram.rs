// const MAX_ENTRY: usize = 32;
const PAGE_SIZE: usize = 4096;
// const BUFFER_SIZE: usize = PAGE_SIZE
//     - 2 * MAX_ENTRY * (std::mem::size_of::<HashEntry<usize>>() + std::mem::size_of::<u16>());
const BUFFER_SIZE: usize = PAGE_SIZE - std::mem::size_of::<u16>();
use std::hash::Hash;
use std::vec;

use crate::cuckoo::HashEntry;
use crate::dynamictree::ORAMTree;
use crate::segvec::{SegmentedVector, MIN_SEGMENT_SIZE};
// #[derive(Clone)]
// struct Page {
//     hashEntries: [HashEntry<usize>; MAX_ENTRY],
//     offsets: [u16; MAX_ENTRY],
//     buffer: [u8; BUFFER_SIZE],
// }

// impl Page {
//     fn new() -> Self {
//         Self {
//             hashEntries: [HashEntry::new(); MAX_ENTRY],
//             offsets: [0; MAX_ENTRY],
//             buffer: [0; BUFFER_SIZE],
//         }
//     }

//     pub fn read_and_remove(&mut self, entry: &HashEntry<usize>) -> Option<Vec<u8>> {
//         for i in 0..MAX_ENTRY {
//             if self.hashEntries[i] == *entry {
//                 let begin_offset = if i == 0 {
//                     0 as usize
//                 } else {
//                     self.offsets[i - 1] as usize
//                 };
//                 let end_offset = self.offsets[i] as usize;
//                 self.hashEntries[i] = HashEntry::new();
//                 return Some(self.buffer[begin_offset..end_offset].to_vec());
//             }
//         }
//         None
//     }

//     fn is_empty_entry(entry: &HashEntry<usize>, page_idx: usize, page_num: usize) -> bool {
//         if entry.is_empty() {
//             return true;
//         }
//         if entry.get_val() % page_num != page_idx {
//             // after rescaling, the entries with wrong page_num becomes empty
//             return true;
//         }
//         false
//     }

//     // Compact the page by moving all the data to the beginning of the buffer
//     // return the number of entries that are compacted
//     pub fn compact(&mut self, page_idx: usize, page_num: usize) -> usize {
//         let mut write_offset: u16 = 0;
//         let mut read_offset: u16 = 0;
//         let mut write_idx: usize = 0;
//         for read_idx in 0..MAX_ENTRY {
//             if self.offsets[read_idx] == 0 {
//                 // reaches end
//                 break;
//             }
//             let len = self.offsets[read_idx] - read_offset;
//             if !Self::is_empty_entry(&self.hashEntries[read_idx], page_idx, page_num) {
//                 if len > 0 {
//                     unsafe {
//                         let src = self.buffer.as_ptr().offset(read_offset as isize);
//                         let dst = self.buffer.as_mut_ptr().offset(write_offset as isize);
//                         std::ptr::copy(src, dst, len as usize);
//                     }
//                 }
//                 self.offsets[write_idx] = write_offset + len;
//                 self.hashEntries[write_idx] = self.hashEntries[read_idx];
//                 if read_idx != write_idx {
//                     self.offsets[read_idx] = 0;
//                     self.hashEntries[read_idx] = HashEntry::new();
//                 }
//                 write_offset += len;
//                 write_idx += 1;
//             }
//             read_offset += len;
//         }
//         write_idx as usize
//     }

//     pub fn insert(&mut self, write_idx: usize, entry: &HashEntry<usize>, value: &[u8]) -> bool {
//         if write_idx >= MAX_ENTRY {
//             return false;
//         }
//         let write_offset = if write_idx == 0 {
//             0 as usize
//         } else {
//             self.offsets[write_idx - 1] as usize
//         };
//         let len = value.len();
//         if write_offset + len > BUFFER_SIZE {
//             return false;
//         }
//         self.offsets[write_idx] = (write_offset + len) as u16;
//         self.hashEntries[write_idx] = *entry;
//         unsafe {
//             let src = value.as_ptr();
//             let dst = self.buffer.as_mut_ptr().offset(write_offset as isize);
//             std::ptr::copy_nonoverlapping(src, dst, len);
//         }
//         true
//     }
// }

/*
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Serialize, Deserialize)]
struct Page {
    entries: VecDeque<(HashEntry<usize>, Vec<u8>)>,
    current_size: usize,
}

impl Page {
    fn new() -> Self {
        Page {
            entries: VecDeque::new(),
            current_size: std::mem::size_of::<Page>(),
        }
    }

    fn read_and_remove(&mut self, meta_data: &HashEntry<usize>) -> Option<Vec<u8>> {
        if let Some(pos) = self.entries.iter().position(|(md, _)| md == meta_data) {
            let (_, value) = self.entries.remove(pos).unwrap();
            return Some(value);
        }
        None
    }

    fn insert(&mut self, meta_data: HashEntry<usize>, entry: Vec<u8>) -> bool {
        let new_entry = (meta_data, entry);
        let serialized_size = bincode::serialized_size(&new_entry).unwrap() as usize;

        if self.current_size + serialized_size > PAGE_SIZE {
            return false;
        }
        self.current_size += serialized_size;
        self.entries.push_back(new_entry);
        true
    }
}

#[derive(Clone)]
struct RawPage {
    buffer: [u8; PAGE_SIZE],
}

impl RawPage {
    fn new() -> Self {
        RawPage {
            buffer: [0; PAGE_SIZE],
        }
    }

    fn from_page(page: &Page) -> Result<Self, bincode::Error> {
        let serialized_data = bincode::serialize(page)?;
        let mut buffer = [0u8; PAGE_SIZE];

        if serialized_data.len() > PAGE_SIZE {
            return Err(bincode::ErrorKind::SizeLimit.into());
        }

        buffer[..serialized_data.len()].copy_from_slice(&serialized_data);

        Ok(RawPage { buffer })
    }

    fn to_page(&self) -> Result<Page, bincode::Error> {
        bincode::deserialize(&self.buffer)
    }
}
*/

#[derive(Clone, Copy)]
#[repr(C)]
struct Page {
    page_num: usize, // the number of pages recorded, if it doesn't match the global page_num, perform a compaction
    filled_bytes: u16,
    buffer: [u8; BUFFER_SIZE],
}
impl Page {
    fn new() -> Self {
        Page {
            page_num: 0,
            filled_bytes: 0,
            buffer: [0; BUFFER_SIZE],
        }
    }

    fn read_and_remove(&mut self, meta_data: &HashEntry<usize>) -> Option<Vec<u8>> {
        let mut ptr = 0 as usize;
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        let meta_bytes = unsafe {
            std::slice::from_raw_parts(meta_data as *const HashEntry<usize> as *const u8, META_SIZE)
        };
        while ptr < self.filled_bytes as usize {
            // compare meta data with the bytes starting from page[ptr]
            let entry_size = u16::from_le_bytes([
                self.buffer[ptr + META_SIZE],
                self.buffer[ptr + META_SIZE + 1],
            ]) as usize;
            let full_entry_size = (META_SIZE + 2 + entry_size) as u16;
            let next_ptr = ptr + full_entry_size as usize;
            if &self.buffer[ptr..ptr + META_SIZE] == meta_bytes {
                // found the entry
                let value = self.buffer[ptr + META_SIZE + 2..next_ptr].to_vec();

                // remove the entry by shifting the remaining entries
                let remaining_bytes = self.filled_bytes - next_ptr as u16;
                unsafe {
                    std::ptr::copy(
                        self.buffer.as_ptr().offset(next_ptr as isize),
                        self.buffer.as_mut_ptr().offset(ptr as isize),
                        remaining_bytes as usize,
                    );
                }
                self.filled_bytes -= full_entry_size;
                return Some(value);
            }
            ptr = next_ptr;
        }
        None
    }

    /**
     * Compact the page if the oram has scaled up and the page_num doesn't match the global page_num
     */
    fn compact(&mut self, page_idx: usize, page_num: usize) {
        if self.page_num == page_num {
            return;
        }
        self.page_num = page_num;
        let mut read_ptr = 0 as usize;
        let mut write_ptr = 0 as usize;
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        while read_ptr < self.filled_bytes as usize {
            let entry_size = u16::from_le_bytes([
                self.buffer[read_ptr + META_SIZE],
                self.buffer[read_ptr + META_SIZE + 1],
            ]) as usize;
            let full_entry_size = (META_SIZE + 2 + entry_size) as u16;
            let next_read_ptr = read_ptr + full_entry_size as usize;
            let entry_meta_bytes = unsafe {
                std::slice::from_raw_parts(
                    self.buffer.as_ptr().offset(read_ptr as isize) as *const u8,
                    META_SIZE,
                )
            };
            let entry_meta = unsafe { &*(entry_meta_bytes.as_ptr() as *const HashEntry<usize>) };
            if entry_meta.get_val() % page_num == page_idx {
                // the entry is still valid
                if read_ptr != write_ptr {
                    // move the entry to the beginning of the buffer
                    let entry_bytes = &self.buffer[read_ptr..next_read_ptr];
                    unsafe {
                        std::ptr::copy(
                            entry_bytes.as_ptr(),
                            self.buffer.as_mut_ptr().offset(write_ptr as isize),
                            full_entry_size as usize,
                        );
                    }
                }
                write_ptr += full_entry_size as usize;
            }
            read_ptr = next_read_ptr;
        }
        self.filled_bytes = write_ptr as u16;
    }

    fn insert(&mut self, meta_data: &HashEntry<usize>, entry: &Vec<u8>) -> bool {
        let entry_size = entry.len();
        let serialized_size = std::mem::size_of::<HashEntry<usize>>() + 2 + entry_size;
        if self.filled_bytes as usize + serialized_size > BUFFER_SIZE {
            return false;
        }
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        let meta_bytes = unsafe {
            std::slice::from_raw_parts(meta_data as *const HashEntry<usize> as *const u8, META_SIZE)
        };
        let entry_size_bytes = (entry_size as u16).to_le_bytes();
        let entry_bytes = entry.as_slice();
        let ptr = self.filled_bytes as usize;
        unsafe {
            std::ptr::copy(
                meta_bytes.as_ptr(),
                self.buffer.as_mut_ptr().offset(ptr as isize),
                META_SIZE,
            );
            std::ptr::copy(
                entry_size_bytes.as_ptr(),
                self.buffer
                    .as_mut_ptr()
                    .offset(ptr as isize + META_SIZE as isize),
                2,
            );
            std::ptr::copy(
                entry_bytes.as_ptr(),
                self.buffer
                    .as_mut_ptr()
                    .offset(ptr as isize + META_SIZE as isize + 2),
                entry_size,
            );
        }
        self.filled_bytes += serialized_size as u16;

        true
    }

    fn insert_raw_bytes(&mut self, raw_bytes: *const u8, serialized_size: usize) -> bool {
        if self.filled_bytes as usize + serialized_size > BUFFER_SIZE {
            return false;
        }
        let ptr = self.filled_bytes as usize;
        unsafe {
            std::ptr::copy(
                raw_bytes,
                self.buffer.as_mut_ptr().offset(ptr as isize),
                serialized_size,
            );
        }
        self.filled_bytes += serialized_size as u16;
        true
    }

    fn read_entry_and_retrieve_rest(
        &self,
        meta_data: &HashEntry<usize>,
        rest: &mut Vec<SortEntry>,
        self_level: u8,
    ) -> Option<Vec<u8>> {
        let mut ptr = 0 as usize;
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        let meta_bytes = unsafe {
            std::slice::from_raw_parts(meta_data as *const HashEntry<usize> as *const u8, META_SIZE)
        };
        let mut ret: Option<Vec<u8>> = None;
        while ptr < self.filled_bytes as usize {
            // compare meta data with the bytes starting from page[ptr]
            let entry_size = u16::from_le_bytes([
                self.buffer[ptr + META_SIZE],
                self.buffer[ptr + META_SIZE + 1],
            ]) as usize;
            let full_entry_size = (META_SIZE + 2 + entry_size) as u16;
            let next_ptr = ptr + full_entry_size as usize;
            if ret == None && &self.buffer[ptr..ptr + META_SIZE] == meta_bytes {
                // found the entry
                let value = self.buffer[ptr + META_SIZE + 2..next_ptr].to_vec();
                ret = Some(value);
            } else {
                // let self_idx = self.buffer[ptr..ptr + META_SIZE]
                let entry_meta_bytes = unsafe {
                    std::slice::from_raw_parts(
                        self.buffer.as_ptr().offset(ptr as isize) as *const u8,
                        META_SIZE,
                    )
                };
                let entry_meta =
                    unsafe { &*(entry_meta_bytes.as_ptr() as *const HashEntry<usize>) };
                let self_idx = entry_meta.get_val();
                let deepest = (self_idx ^ meta_data.get_val()).trailing_zeros() as u8;
                rest.push(SortEntry {
                    deepest,
                    src: self_level,
                    len: full_entry_size,
                    offset: ptr as u16,
                });
            }
            ptr = next_ptr;
        }
        ret
    }
}

pub struct PageOram {
    tree: ORAMTree<Page>,
    stash: Vec<Vec<(HashEntry<usize>, Vec<u8>)>>,
    num_stash_entry: usize,
    num_bytes_stash: usize,
}
#[derive(Clone)]
struct SortEntry {
    deepest: u8,
    src: u8,
    len: u16,
    offset: u16,
}

impl PageOram {
    pub fn new() -> Self {
        let mut stash: Vec<Vec<(HashEntry<usize>, Vec<u8>)>> = Vec::with_capacity(MIN_SEGMENT_SIZE);
        for _ in 0..MIN_SEGMENT_SIZE {
            stash.push(Vec::new());
        }
        Self {
            tree: ORAMTree::new(1024),
            stash,
            num_stash_entry: 0,
            num_bytes_stash: 0,
        }
    }
    const STASH_ENTRY_META_SIZE: usize =
        std::mem::size_of::<HashEntry<usize>>() + std::mem::size_of::<Vec<u8>>();
    // read entry from the stash vec, update result, and evict the remaining entries to the page
    fn read_and_evict_stash(
        &mut self,
        entry: &HashEntry<usize>,
        result: &mut Option<Vec<u8>>,
        page: &mut Page,
        page_idx: usize,
        page_num: usize,
    ) {
        let stash_vec = self.stash.get_mut(page_idx).unwrap();
        // find the entry in the stash, don't remove it yet, just mark the index
        let mut read_idx = stash_vec.len();
        for i in 0..stash_vec.len() {
            let (e, value) = &stash_vec[i];
            if e == entry {
                read_idx = i;
                *result = Some(value.clone());
                break;
            }
        }
        page.compact(page_idx, page_num);

        // try to write stash vec to the page
        // loop backwards for efficient removal and consistent index
        for i in (0..stash_vec.len()).rev() {
            let (entry, value) = &stash_vec[i];
            // if it's the entry that we just read, just remove it
            if i != read_idx {
                if !page.insert(&entry, &value) {
                    // page is full
                    if i > read_idx {
                        // now we need to remove the entry that we just read
                        let (_, read_val) = stash_vec.remove(read_idx);
                        self.num_stash_entry -= 1;
                        self.num_bytes_stash -= read_val.len() + Self::STASH_ENTRY_META_SIZE;
                    }
                    break;
                }
            }
            self.num_stash_entry -= 1;
            self.num_bytes_stash -= value.len() + Self::STASH_ENTRY_META_SIZE;
            stash_vec.remove(i);
        }
    }

    pub fn read_or_write(
        &mut self,
        entry: &HashEntry<usize>,
        value_to_write: Option<&Vec<u8>>,
        new_page_id: usize,
    ) -> Option<Vec<u8>> {
        let page_idx = entry.get_val();
        let (path, layer_sizes) = self.tree.read_path(page_idx);
        let num_layer = layer_sizes.len();
        let mut result: Option<Vec<u8>> = None;
        let mut rest: Vec<SortEntry> = Vec::new();
        for (i, page) in path.iter().enumerate() {
            let page_result = page.read_entry_and_retrieve_rest(entry, &mut rest, i as u8);
            if page_result.is_some() {
                result = page_result;
            }
        }
        let page_idx_in_stash = page_idx % self.stash.len();
        let stash_vec = self.stash.get(page_idx_in_stash).unwrap();
        if stash_vec.len() >= 65536 {
            // TODO
            println!("stash is catastrophically full");
            return None;
        }
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        for (i, (stash_entry, value)) in stash_vec.iter().enumerate() {
            if stash_entry == entry {
                result = Some(value.clone());
            } else {
                let deepest = (stash_entry.get_val() ^ page_idx).trailing_zeros() as u8;
                rest.push(SortEntry {
                    deepest,
                    src: num_layer as u8,
                    len: (META_SIZE + 2 + value.len()) as u16,
                    offset: i as u16,
                });
            }
            self.num_bytes_stash -= value.len() + Self::STASH_ENTRY_META_SIZE;
        }
        self.num_stash_entry -= stash_vec.len();

        let layer_log_sizes: Vec<u8> = layer_sizes
            .iter()
            .map(|x| x.trailing_zeros() as u8)
            .collect();
        for entry in rest.iter_mut() {
            // change the deepest from the number of common trailing bits to the actual level number
            let mut success = false;
            for (i, log_layer_size) in layer_log_sizes.iter().enumerate() {
                if entry.deepest >= *log_layer_size {
                    entry.deepest = i as u8;
                    success = true;
                    break;
                }
            }
            if !success {
                entry.deepest = num_layer as u8;
            }
        }

        // now sort the rest of the entries by the deepest level
        rest.sort_by(|a, b| a.deepest.cmp(&b.deepest));

        // entries allowed to be placed in the currently enumerated page
        let mut allowed_set: Vec<SortEntry> = Vec::new();
        let mut rest_idx = 0;
        let mut new_path: Vec<Page> = vec![Page::new(); path.len()];
        for dest in 0..num_layer {
            while rest_idx < rest.len() && rest[rest_idx].deepest <= dest as u8 {
                allowed_set.push(rest[rest_idx].clone());
                rest_idx += 1;
            }
            allowed_set.sort_by(|a, b| b.len.cmp(&a.len));
            // apply a greedy strategy to place the entries from large to small
            let mut new_allowed_set: Vec<SortEntry> = Vec::new(); // put entries that doesn't fit
            let page = &mut new_path[dest];
            let mut write_count = 0;
            for entry in allowed_set.iter() {
                if write_count + entry.len as usize > BUFFER_SIZE {
                    new_allowed_set.push(entry.clone());
                    continue;
                }
                if entry.src == num_layer as u8 {
                    // copy from stash
                    let (stash_entry, value) = &stash_vec[entry.offset as usize];
                    page.insert(stash_entry, value);
                } else {
                    // copy from page
                    let src_page = path[entry.src as usize];
                    let raw_bytes =
                        unsafe { src_page.buffer.as_ptr().offset(entry.offset as isize) };
                    page.insert_raw_bytes(raw_bytes, entry.len as usize);
                }
                write_count += entry.len as usize;
            }
            allowed_set = new_allowed_set;
        }

        let mut new_stash_vec = Vec::new();
        new_stash_vec.reserve(allowed_set.len());
        for entry in allowed_set.iter() {
            if entry.src == num_layer as u8 {
                let (stash_entry, value) = &stash_vec[entry.offset as usize];
                self.num_bytes_stash += value.len() + Self::STASH_ENTRY_META_SIZE;
                new_stash_vec.push((stash_entry.clone(), value.clone()));
            } else {
                let src_page = path[entry.src as usize];
                let meta_bytes = unsafe { src_page.buffer.as_ptr().offset(entry.offset as isize) };
                let meta_data = unsafe { &*(meta_bytes as *const HashEntry<usize>) };
                let value = src_page.buffer
                    [entry.offset as usize + META_SIZE + 2..(entry.offset + entry.len) as usize]
                    .to_vec();
                self.num_bytes_stash += value.len() + Self::STASH_ENTRY_META_SIZE;
                new_stash_vec.push((meta_data.clone(), value));
            }
        }
        self.num_stash_entry += new_stash_vec.len();
        self.stash[page_idx_in_stash] = new_stash_vec;

        // write back path
        self.tree.write_path(page_idx, &new_path);

        if value_to_write.is_some() || result.is_some() {
            let mut new_entry = entry.clone();
            new_entry.set_val(new_page_id);
            let new_page_idx_in_stash = new_page_id % self.stash.len();
            let value_to_write_clone = if value_to_write.is_some() {
                value_to_write.unwrap().clone()
            } else {
                result.clone().unwrap()
            };
            self.num_stash_entry += 1;
            self.num_bytes_stash += value_to_write_clone.len() + Self::STASH_ENTRY_META_SIZE;
            let stash_vec_for_new_page = self.stash.get_mut(new_page_idx_in_stash).unwrap();
            stash_vec_for_new_page.push((new_entry, value_to_write_clone));
        }
        result
    }

    fn scale(&mut self) {}

    pub fn read(&mut self, entry: &HashEntry<usize>, new_page_id: usize) -> Option<Vec<u8>> {
        self.read_or_write(entry, None, new_page_id)
    }

    pub fn write(
        &mut self,
        entry: &HashEntry<usize>,
        value: &Vec<u8>,
        new_page_id: usize,
    ) -> Option<Vec<u8>> {
        self.read_or_write(entry, Some(value), new_page_id)
    }

    pub fn print_meta_state(&self) {
        println!("PageOram meta state:");
        // println!("PageOram pages count: {}", self.pages.capacity());
        println!("PageOram stash entry count: {}", self.num_stash_entry);
        println!(
            "PageOram stash size: {} MB",
            self.num_bytes_stash as f64 / (1024 * 1024) as f64
        );
    }
    pub fn print_state(&self) {
        // for i in 0..self.pages.capacity() {
        //     let page = self.pages.get(i).unwrap();
        //     println!(
        //         "Page {} num entries: {} total bytes {}",
        //         i,
        //         page.entries.len(),
        //         page.current_size
        //     );
        //     // for j in 0..MAX_ENTRY {
        //     //     let entry = &page.hashEntries[j];
        //     //     if !Page::is_empty_entry(entry, i, self.pages.capacity()) {
        //     //         println!("Entry: {:?}", entry);
        //     //     }
        //     // }
        // }
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
    use std::collections::HashMap;

    use super::*;
    use rand::random;
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

    #[test]
    fn test_page_oram_medium() {
        let mut page_oram = PageOram::new();
        let round = 10000;
        let mut ref_vec: Vec<(HashEntry<usize>, Vec<u8>)> = Vec::new();

        for i in 0..round {
            let mut entry = HashEntry::new();
            entry.set_idx([random(), random()]);
            let val_len = random::<usize>() % 100;
            let value: Vec<u8> = (0..val_len).map(|_| random::<u8>()).collect();
            let new_page_id = random::<usize>();
            let result = page_oram.write(&entry, &value, new_page_id);
            assert_eq!(result, None);
            entry.set_val(new_page_id);
            ref_vec.push((entry, value));
        }

        for r in 0..10 {
            for (entry, value) in ref_vec.iter_mut() {
                let new_page_id = random();
                let result = page_oram.read(&entry, new_page_id);
                assert_eq!(result, Some(value.clone()));
                entry.set_val(new_page_id)
            }
        }
        page_oram.print_meta_state();
    }
}
