// const MAX_ENTRY: usize = 32;

// const BUFFER_SIZE: usize = PAGE_SIZE
//     - 2 * MAX_ENTRY * (std::mem::size_of::<HashEntry<usize>>() + std::mem::size_of::<u16>());

use std::vec;

use super::cuckoo::HashEntry;
use crate::params::{KEY_SIZE, MAX_CACHE_SIZE, MIN_SEGMENT_SIZE, PAGE_SIZE};
use crate::tree::dynamictree::{calc_deepest, ORAMTree};
use bytemuck::{Pod, Zeroable};

const BUFFER_SIZE: usize = PAGE_SIZE - 2 * std::mem::size_of::<u16>() - KEY_SIZE;
#[repr(C)]
#[derive(Clone, Copy)]
struct Page {
    filled_bytes: u16,
    buffer: [u8; BUFFER_SIZE],
}
unsafe impl Zeroable for Page {}
unsafe impl Pod for Page {}
impl Page {
    fn new() -> Self {
        Page {
            filled_bytes: 0,
            buffer: [0; BUFFER_SIZE],
        }
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
        let entry_size_bytes = (entry_size as u16).to_ne_bytes();
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
        layer_log_sizes: &Vec<u8>,
    ) -> Option<Vec<u8>> {
        let mut ptr = 0 as usize;
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        let meta_bytes = unsafe {
            std::slice::from_raw_parts(meta_data as *const HashEntry<usize> as *const u8, META_SIZE)
        };
        let mut ret: Option<Vec<u8>> = None;
        while ptr < self.filled_bytes as usize {
            // compare meta data with the bytes starting from page[ptr]
            let entry_size = u16::from_ne_bytes([
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
                let deepest = calc_deepest(self_idx, meta_data.get_val(), layer_log_sizes);
                if deepest <= self_level as u8 {
                    // after fork, some entries may become invalid, i.e., cannot be placed in the current page
                    // we simply remove them
                    rest.push(SortEntry {
                        deepest,
                        src: self_level,
                        len: full_entry_size,
                        offset: ptr as u32,
                    });
                }
            }
            ptr = next_ptr;
        }
        ret
    }
}

#[derive(Clone)]
struct StashEntry {
    pub kvs: Vec<(HashEntry<usize>, Vec<u8>)>,
}

impl StashEntry {
    fn new() -> Self {
        Self { kvs: Vec::new() }
    }
}

struct Stash {
    stash: Vec<StashEntry>,
    versions: Vec<u8>,
    size: usize,
    log_size: u8,
    num_bytes: usize,
    num_kvs: usize,
}

impl Stash {
    const STASH_ENTRY_META_SIZE: usize = std::mem::size_of::<(HashEntry<usize>, Vec<u8>)>();

    pub fn new(init_size: usize) -> Self {
        assert!((init_size & (init_size - 1)) == 0); // must be power of 2
        let log_init_size = init_size.trailing_zeros() as u8;
        let stash = vec![StashEntry::new(); init_size];
        let versions = vec![log_init_size; init_size];
        Self {
            stash,
            versions,
            size: init_size,
            log_size: log_init_size,
            num_bytes: 0,
            num_kvs: 0,
        }
    }
    pub fn scale(&mut self, new_size: usize) {
        assert!((new_size & (new_size - 1)) == 0); // must be power of 2
        if new_size <= self.stash.len() {
            // logical scale up/ down
            self.size = new_size;
            self.log_size = new_size.trailing_zeros() as u8;
            return;
        }
        assert_eq!(self.size, self.stash.len());
        let old_size = self.size;
        self.stash.resize(new_size, StashEntry::new());
        self.versions.resize(new_size, 0u8);
        // copy the versions
        let scale_factor = new_size / old_size;
        for i in 1..scale_factor {
            unsafe {
                std::ptr::copy(
                    self.versions.as_ptr(),
                    self.versions
                        .as_mut_ptr()
                        .offset(i as isize * old_size as isize),
                    old_size,
                );
            }
        }
        self.size = new_size;
        self.log_size = new_size.trailing_zeros() as u8;
    }

    fn split_entry(&mut self, stash_idx: usize) {
        let version = self.versions[stash_idx];
        let num_stash_entry_rec = 1 << version;
        if num_stash_entry_rec >= self.size {
            return; // no need to split
        }
        let from_idx = stash_idx % num_stash_entry_rec;
        let from_entry = &self.stash[from_idx];
        let scaling_factor = self.size / num_stash_entry_rec;
        let mut kvs_after_split = vec![Vec::new(); scaling_factor];
        for (entry, value) in from_entry.kvs.iter() {
            let new_idx = entry.get_val() % self.size;
            kvs_after_split[new_idx / num_stash_entry_rec].push((entry.clone(), value.clone()));
        }
        for i in 0..scaling_factor {
            let to_idx = i * num_stash_entry_rec + from_idx;
            std::mem::swap(&mut self.stash[to_idx].kvs, &mut kvs_after_split[i]);
            self.versions[to_idx] = self.log_size;
        }
    }

    pub fn get_and_remove(&mut self, idx: usize) -> Vec<(HashEntry<usize>, Vec<u8>)> {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
        assert_eq!((1 << self.versions[stash_idx]), self.size);
        let mut ret: Vec<(HashEntry<usize>, Vec<u8>)> = Vec::new();
        std::mem::swap(&mut self.stash[stash_idx].kvs, &mut ret);
        for (_, value) in ret.iter() {
            self.num_bytes -= value.len() + Self::STASH_ENTRY_META_SIZE;
        }
        self.num_kvs -= ret.len();
        ret
    }

    pub fn insert(&mut self, idx: usize, entry: HashEntry<usize>, value: Vec<u8>) {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
                                     // we don't perform merge during insert
        self.num_bytes += value.len() + Self::STASH_ENTRY_META_SIZE;
        self.num_kvs += 1;
        self.stash[stash_idx].kvs.push((entry, value));
    }

    pub fn concat(&mut self, idx: usize, entries: Vec<(HashEntry<usize>, Vec<u8>)>) {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
        self.num_kvs += entries.len();
        for (_, value) in &entries {
            self.num_bytes += value.len() + Self::STASH_ENTRY_META_SIZE;
        }
        if self.stash[stash_idx].kvs.is_empty() {
            self.stash[stash_idx].kvs = entries;
        } else {
            // this should not happen in our usecase
            self.stash[stash_idx].kvs.extend(entries);
        }
    }

    // pub fn avg_load(&self) -> f64 {
    //     self.num_kvs as f64 / self.size as f64
    // }

    pub fn num_kvs(&self) -> usize {
        self.num_kvs
    }

    pub fn num_bytes(&self) -> usize {
        self.num_bytes + self.stash.len() * std::mem::size_of::<StashEntry>()
    }
}

pub struct FlexOram {
    tree: ORAMTree<Page>,
    stash: Stash,
    num_entry: usize,
    num_bytes: usize,
}
#[derive(Clone)]
struct SortEntry {
    deepest: u8,
    src: u8,
    len: u16,
    offset: u32,
}

impl FlexOram {
    pub fn new() -> Self {
        Self {
            tree: ORAMTree::new(MAX_CACHE_SIZE),
            stash: Stash::new(MIN_SEGMENT_SIZE),
            num_entry: 0,
            num_bytes: 0,
        }
    }

    pub fn update<F>(&mut self, entry: &HashEntry<usize>, update_func: F, new_page_id: usize)
    where
        F: FnOnce(Option<Vec<u8>>) -> Option<Vec<u8>>,
    {
        let page_idx = entry.get_val();
        let (path, layer_sizes) = self.tree.read_path(page_idx);
        let num_layer = layer_sizes.len();
        let layer_log_sizes: Vec<u8> = layer_sizes
            .iter()
            .map(|x| x.trailing_zeros() as u8)
            .collect();
        let mut result: Option<Vec<u8>> = None;
        let mut rest: Vec<SortEntry> = Vec::new();
        for (i, page) in path.iter().enumerate() {
            let page_result =
                page.read_entry_and_retrieve_rest(entry, &mut rest, i as u8, &layer_log_sizes);
            if page_result.is_some() {
                result = page_result;
            }
        }
        let stash_vec = self.stash.get_and_remove(page_idx);
        // if stash_vec.len() >= 65536 {
        //     // TODO
        //     println!("stash is catastrophically full");
        //     return None;
        // }
        const META_SIZE: usize = std::mem::size_of::<HashEntry<usize>>();
        // for (chunk_idx, stash_vec) in stash_vecs.iter().enumerate() {
        for (i, (stash_entry, value)) in stash_vec.iter().enumerate() {
            if stash_entry == entry {
                result = Some(value.clone());
            } else {
                let deepest = calc_deepest(stash_entry.get_val(), page_idx, &layer_log_sizes);
                if deepest >= num_layer as u8 {
                    // after the stash forks, some entries may become invalid, i.e., cannot be placed in the current sub vector
                    continue;
                }
                rest.push(SortEntry {
                    deepest,
                    src: num_layer as u8,
                    len: (META_SIZE + 2 + value.len()) as u16,
                    offset: i as u32,
                });
            }
            // }
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
                new_stash_vec.push((stash_entry.clone(), value.clone()));
            } else {
                let src_page = path[entry.src as usize];
                let meta_bytes = unsafe { src_page.buffer.as_ptr().offset(entry.offset as isize) };
                let meta_data = unsafe { &*(meta_bytes as *const HashEntry<usize>) };
                let value = src_page.buffer[entry.offset as usize + META_SIZE + 2
                    ..(entry.offset as usize + entry.len as usize)]
                    .to_vec();
                new_stash_vec.push((meta_data.clone(), value));
            }
        }
        self.stash.concat(page_idx, new_stash_vec);

        // write back path
        self.tree.write_path(page_idx, &new_path);
        if result.is_some() {
            self.num_entry -= 1;
            self.num_bytes -= result.as_ref().unwrap().len() + META_SIZE;
        }
        let result = update_func(result);

        if let Some(result_unwrap) = result {
            self.num_entry += 1;
            self.num_bytes += result_unwrap.len() + META_SIZE;
            let mut new_entry = entry.clone();
            new_entry.set_val(new_page_id);
            self.stash.insert(new_page_id, new_entry, result_unwrap);
        }
        let load_factor = self.num_bytes as f64 / (self.tree.total_size() * BUFFER_SIZE) as f64;
        if load_factor > 0.5 {
            println!(
                "load bytes: {} total bytes: {}",
                self.num_bytes,
                self.tree.total_size() * BUFFER_SIZE
            );
            self.scale();
        }
    }

    fn scale(&mut self) {
        let target_branching_factor = BUFFER_SIZE as usize * self.num_entry / self.num_bytes;
        println!("Scaling to branching factor {}", target_branching_factor);
        self.tree.scale(target_branching_factor);
        let new_stash_size = self.tree.min_layer_size();
        // todo: optimize this with shallow copy
        self.stash.scale(new_stash_size)
    }

    pub fn read(&mut self, entry: &HashEntry<usize>, new_page_id: usize) -> Option<Vec<u8>> {
        let mut ret = None;
        let dummy_func = |x: Option<Vec<u8>>| {
            ret = x.clone();
            x
        };
        self.update(entry, dummy_func, new_page_id);
        ret
    }

    pub fn write(&mut self, entry: &HashEntry<usize>, value: &Vec<u8>, new_page_id: usize) {
        let overwrite_func = |_| Some(value.clone());
        self.update(entry, overwrite_func, new_page_id);
    }

    pub fn read_and_write<V: AsRef<[u8]>>(
        &mut self,
        entry: &HashEntry<usize>,
        value: V,
        new_page_id: usize,
    ) -> Option<Vec<u8>> {
        let mut ret = None;
        let overwrite_func = |x: Option<Vec<u8>>| {
            ret = x;
            Some(value.as_ref().to_vec())
        };
        self.update(entry, overwrite_func, new_page_id);
        ret
    }

    pub fn remove(&mut self, entry: &HashEntry<usize>) -> Option<Vec<u8>> {
        let mut ret = None;
        let remove_func = |x| {
            ret = x;
            None
        };
        self.update(entry, remove_func, 0);
        ret
    }

    pub fn print_meta_state(&self) {
        println!("FlexOram meta state:");
        // println!("FlexOram pages count: {}", self.pages.capacity());
        println!("FlexOram stash kv count: {}", self.stash.num_kvs());
        println!(
            "FlexOram stash size: {} MB",
            self.stash.num_bytes() as f64 / (1024 * 1024) as f64
        );
    }
    // pub fn print_state(&self) {
    //     // for i in 0..self.pages.capacity() {
    //     //     let page = self.pages.get(i).unwrap();
    //     //     println!(
    //     //         "Page {} num entries: {} total bytes {}",
    //     //         i,
    //     //         page.entries.len(),
    //     //         page.current_size
    //     //     );
    //     //     // for j in 0..MAX_ENTRY {
    //     //     //     let entry = &page.hashEntries[j];
    //     //     //     if !Page::is_empty_entry(entry, i, self.pages.capacity()) {
    //     //     //         println!("Entry: {:?}", entry);
    //     //     //     }
    //     //     // }
    //     // }
    //     for i in 0..self.stash.len() {
    //         let stash_vec = &self.stash[i];
    //         if stash_vec.is_empty() {
    //             continue;
    //         }
    //         println!("Stash {}", i);
    //         for (entry, value) in stash_vec {
    //             println!("Entry: {:?}", entry);
    //         }
    //     }
    // }
}

mod tests {
    use super::*;
    use rand::random;
    #[test]
    fn test_flex_oram_simple() {
        let mut flex_oram = FlexOram::new();
        let mut entry = HashEntry::new();
        entry.set_idx([1, 2]);
        entry.set_val(1);
        let value = vec![1, 2, 3, 4];
        let new_page_id = 1;
        flex_oram.write(&entry, &value, new_page_id);

        let result = flex_oram.read(&entry, new_page_id);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_flex_oram_medium() {
        let mut flex_oram = FlexOram::new();
        let round = 100000;
        let mut ref_vec: Vec<(HashEntry<usize>, Vec<u8>)> = Vec::new();

        for _ in 0..round {
            let mut entry = HashEntry::new();
            entry.set_idx([random(), random()]);
            let val_len = random::<usize>() % 32;
            let value: Vec<u8> = (0..val_len).map(|_| random::<u8>()).collect();
            let new_page_id = random::<usize>();
            flex_oram.write(&entry, &value, new_page_id);
            entry.set_val(new_page_id);
            ref_vec.push((entry, value));
        }

        for _ in 0..10 {
            for (entry, value) in ref_vec.iter_mut() {
                let new_page_id = random();
                let result = flex_oram.read(&entry, new_page_id);
                assert_eq!(result, Some(value.clone()));
                entry.set_val(new_page_id)
            }
        }
        flex_oram.print_meta_state();
    }
}
