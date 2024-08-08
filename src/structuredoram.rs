// const MAX_ENTRY: usize = 32;

// const BUFFER_SIZE: usize = PAGE_SIZE
//     - 2 * MAX_ENTRY * (std::mem::size_of::<HashEntry<usize>>() + std::mem::size_of::<u16>());

use std::fmt::Debug;
use std::vec;

use crate::cuckoo::HashEntry;
use crate::dynamictree::{calc_deepest, ORAMTree};
use crate::params::MIN_SEGMENT_SIZE;
use crate::params::{KEY_SIZE, PAGE_SIZE};
use bincode::de;
use bytemuck::{Pod, Zeroable};

const BUFFER_SIZE: usize = PAGE_SIZE - 2 * std::mem::size_of::<u16>() - KEY_SIZE;
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct BlockId {
    page_idx: usize,
    uid: u64,
}

impl BlockId {
    fn new() -> Self {
        Self {
            page_idx: 0,
            uid: 0,
        }
    }
}

trait SimpleVal: Clone + Copy + Pod + Zeroable + Debug {}
impl<T> SimpleVal for T where T: Clone + Copy + Pod + Zeroable + Debug {}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct Page<T: SimpleVal, const N: usize> {
    page_num: usize, // the number of pages recorded, if it doesn't match the global page_num, perform a compaction
    indices: [BlockId; N],
    data: [T; N],
}

unsafe impl<T: SimpleVal, const N: usize> Zeroable for Page<T, N> {}
unsafe impl<T: SimpleVal, const N: usize> Pod for Page<T, N> {}
impl<T: SimpleVal, const N: usize> Page<T, N> {
    fn new() -> Self {
        Page {
            page_num: 0,
            indices: [BlockId::new(); N],
            data: [T::zeroed(); N],
        }
    }

    fn insert(&mut self, idx: u16, meta_data: &BlockId, entry: &T) {
        self.indices[idx as usize] = *meta_data;
        self.data[idx as usize] = *entry;
    }

    fn read_and_remove_entry(&mut self, meta_data: &BlockId) -> Option<T> {
        for i in 0..N {
            if self.indices[i] == *meta_data {
                self.indices[i] = BlockId::new();
                let ret = Some(self.data[i]);
                return ret;
            }
        }
        None
    }
}

#[derive(Clone)]
struct StashEntry<T> {
    pub kvs: Vec<(BlockId, T)>,
}

impl<T: SimpleVal> StashEntry<T> {
    fn new() -> Self {
        Self { kvs: Vec::new() }
    }
}

struct Stash<T> {
    stash: Vec<StashEntry<T>>,
    versions: Vec<u8>,
    size: usize,
    log_size: u8,
    num_kvs: usize,
}

impl<T: SimpleVal> Stash<T> {
    const STASH_ENTRY_SIZE: usize = std::mem::size_of::<(BlockId, T)>();

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
        println!("Splitting entry: {}", stash_idx);
        let from_idx = stash_idx % num_stash_entry_rec;
        let from_entry = &self.stash[from_idx];
        let scaling_factor = self.size / num_stash_entry_rec;
        let mut kvs_after_split = vec![Vec::new(); scaling_factor];
        for (entry, value) in from_entry.kvs.iter() {
            let new_idx = entry.page_idx % self.size;
            kvs_after_split[new_idx / num_stash_entry_rec].push((entry.clone(), value.clone()));
        }
        for i in 0..scaling_factor {
            let to_idx = i * num_stash_entry_rec + from_idx;
            std::mem::swap(&mut self.stash[to_idx].kvs, &mut kvs_after_split[i]);
            self.versions[to_idx] = self.log_size;
        }
    }

    pub fn get_mut(&mut self, idx: usize) -> &mut Vec<(BlockId, T)> {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry

        // let mut ret: Vec<(BlockId, T)> = Vec::new();
        // std::mem::swap(&mut self.stash[stash_idx].kvs, &mut ret);
        // self.num_kvs -= ret.len();
        &mut self.stash[stash_idx].kvs
    }

    pub fn insert(&mut self, idx: usize, entry: BlockId, value: T) {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
                                     // we don't perform merge during insert
        self.num_kvs += 1;
        self.stash[stash_idx].kvs.push((entry, value));
    }

    pub fn concat(&mut self, idx: usize, entries: Vec<(BlockId, T)>) {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
        self.num_kvs += entries.len();
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
        self.num_kvs * Self::STASH_ENTRY_SIZE
            + self.size * (std::mem::size_of::<StashEntry<T>>() + 1)
    }
}
#[derive(Clone, Debug)]
struct EvictInfo {
    is_from_stash: bool,
    src: u8,
    offset: u16,
}

pub struct StructuredOram<T: SimpleVal, const N: usize> {
    tree: ORAMTree<Page<T, N>>,
    stash: Stash<T>,
    num_entry: usize,
    evict_infos_cache: Vec<Vec<EvictInfo>>, // a cache to store the src position of entries to evict
    empty_slots_cache: Vec<Vec<u16>>,       // a cache to store the empty slots in the path
    stash_remain_cache: Vec<u16>,           // cache the idx of stash entries that are not evicted
}
#[derive(Clone)]
struct SortEntry {
    deepest: u8,
    src: u8,
    len: u16,
    offset: u32,
}

impl<T: SimpleVal, const N: usize> StructuredOram<T, N> {
    pub fn new() -> Self {
        Self {
            tree: ORAMTree::new(MIN_SEGMENT_SIZE * 16),
            stash: Stash::new(MIN_SEGMENT_SIZE),
            num_entry: 0,
            evict_infos_cache: vec![Vec::new(); 48],
            empty_slots_cache: vec![Vec::new(); 48],
            stash_remain_cache: Vec::new(),
        }
    }

    fn num_bytes(&self) -> usize {
        self.num_entry * (std::mem::size_of::<(BlockId, T)>()) + self.stash.num_bytes()
    }

    pub fn read_or_write(
        &mut self,
        id: &BlockId,
        value_to_write: Option<&T>,
        new_page_id: usize,
    ) -> Option<T> {
        let path_idx = id.page_idx;
        let (mut path, layer_sizes) = self.tree.read_path(path_idx);
        let num_layer = layer_sizes.len();
        let layer_log_sizes: Vec<u8> = layer_sizes
            .iter()
            .map(|x| x.trailing_zeros() as u8)
            .collect();
        let mut result: Option<T> = None;

        for (i, page) in path.iter_mut().rev().enumerate() {
            let entry = page.read_and_remove_entry(id);
            if entry.is_some() {
                result = entry;
            }
            for j in 0..N {
                let page_idx = page.indices[j].page_idx;
                let deepest = calc_deepest(page_idx, path_idx, &layer_log_sizes);
                if deepest >= num_layer as u8 {
                    self.empty_slots_cache[i].push(j as u16);
                } else if deepest > i as u8 {
                    self.evict_infos_cache[deepest as usize].push(EvictInfo {
                        is_from_stash: false,
                        src: i as u8,
                        offset: j as u16,
                    });
                }
            }
        }
        let stash_vec = self.stash.get_mut(path_idx);
        for (i, (block_id, value)) in stash_vec.iter().enumerate() {
            if block_id == id {
                result = Some(value.clone());
            } else {
                let deepest = calc_deepest(block_id.page_idx, path_idx, &layer_log_sizes);
                assert!(deepest < num_layer as u8);
                self.evict_infos_cache[deepest as usize].push(EvictInfo {
                    is_from_stash: true,
                    src: 0,
                    offset: i as u16,
                });
            }
        }

        let mut curr_evict_info_level = num_layer - 1;
        let mut complete_flag = false;
        for dst in (0..num_layer).rev() {
            let (up_empty_slots, down_empty_slots) = self.empty_slots_cache.split_at_mut(dst);
            for slot_offset in down_empty_slots[0].iter() {
                // if curr_evict_info_level < dst {
                //     // no available block to evict to dst
                //     continue;
                // }
                while self.evict_infos_cache[curr_evict_info_level].is_empty() {
                    if curr_evict_info_level == 0 {
                        complete_flag = true;
                        break;
                    }
                    curr_evict_info_level -= 1;
                }
                if complete_flag {
                    break;
                }
                if curr_evict_info_level < dst {
                    // no available block to evict to dst
                    break;
                }

                let evict_info = self.evict_infos_cache[curr_evict_info_level].pop().unwrap();

                if evict_info.is_from_stash {
                    let (block_id, value) = stash_vec[evict_info.offset as usize];
                    path[dst].insert(*slot_offset, &block_id, &value);
                } else {
                    let src = evict_info.src as usize;
                    if src >= dst {
                        // since the blocks are read from the evict_infos_cache in a top-down order, we can clear the vector and break here
                        self.evict_infos_cache[curr_evict_info_level].clear();
                        break;
                    }
                    let src_offset = evict_info.offset as usize;
                    let dst_offset = *slot_offset as usize;

                    up_empty_slots[src].push(src_offset as u16); // we have new empty slots now
                    let (up_path, down_path) = path.split_at_mut(dst);
                    std::mem::swap(
                        &mut down_path[0].indices[dst_offset],
                        &mut up_path[src].indices[src_offset],
                    );
                    std::mem::swap(
                        &mut down_path[0].data[dst_offset],
                        &mut up_path[src].data[src_offset],
                    );
                }
            }
        }

        // delete the evicted slots from stash
        for dst in 0..num_layer {
            for evict_info in self.evict_infos_cache[dst].iter() {
                if evict_info.is_from_stash {
                    self.stash_remain_cache.push(evict_info.offset);
                }
            }
        }
        self.stash_remain_cache.sort_unstable();
        let mut write_offset = 0;
        for from_idx in self.stash_remain_cache.iter() {
            let from_idx = *from_idx as usize;
            stash_vec.copy_within(from_idx..from_idx + 1, write_offset);
            write_offset += 1;
        }
        let stash_reduced_len = stash_vec.len() - write_offset;
        stash_vec.truncate(write_offset);
        self.stash.num_kvs -= stash_reduced_len;
        for i in 0..num_layer {
            self.evict_infos_cache[i].clear();
            self.empty_slots_cache[i].clear();
        }
        self.stash_remain_cache.clear();
        self.tree.write_path_move(path_idx, path);
        if value_to_write.is_some() || result.is_some() {
            if result.is_none() {
                self.num_entry += 1;
            }
            let new_id = BlockId {
                page_idx: new_page_id,
                uid: id.uid,
            };
            if value_to_write.is_some() {
                self.stash
                    .insert(new_page_id, new_id, value_to_write.unwrap().clone());
            } else {
                self.stash
                    .insert(new_page_id, new_id, result.unwrap().clone());
            }
        }
        let load_factor = self.num_bytes() as f64 / (self.tree.total_size() * BUFFER_SIZE) as f64;
        if load_factor > 0.7 {
            println!(
                "load bytes: {} total bytes: {}",
                self.num_bytes(),
                self.tree.total_size() * BUFFER_SIZE
            );
            self.scale();
        }
        result
    }

    fn scale(&mut self) {
        let target_branching_factor = BUFFER_SIZE as usize / (std::mem::size_of::<(T, BlockId)>());
        println!("Scaling to branching factor {}", target_branching_factor);
        self.tree.scale(target_branching_factor);
        let new_stash_size = self.tree.min_layer_size();
        // todo: optimize this with shallow copy
        self.stash.scale(new_stash_size)
    }

    pub fn read(&mut self, id: &BlockId, new_page_id: usize) -> Option<T> {
        self.read_or_write(id, None, new_page_id)
    }

    pub fn write(&mut self, id: &BlockId, value: &T, new_page_id: usize) -> Option<T> {
        self.read_or_write(id, Some(value), new_page_id)
    }

    pub fn print_meta_state(&self) {
        println!("StructuredOram meta state:");
        // println!("StructuredOram pages count: {}", self.pages.capacity());
        println!("StructuredOram stash kv count: {}", self.stash.num_kvs());
        println!(
            "StructuredOram stash size: {} MB",
            self.stash.num_bytes() as f64 / (1024 * 1024) as f64
        );
    }
    pub fn print_state(&self) {
        for i in 0..self.stash.size {
            let kvs = &self.stash.stash[i].kvs;
            println!("Stash entry: ");
            for (entry, value) in kvs.iter() {
                println!("({:?} {:?})", entry, value);
            }
        }
        self.tree.print_state();
    }
}

mod tests {
    use super::*;
    use rand::random;
    #[test]
    fn test_structured_oram_simple() {
        const BLOCK_PER_PAGE: usize =
            (BUFFER_SIZE / (std::mem::size_of::<(BlockId, u128)>())) as usize;
        let mut page_oram = StructuredOram::<u128, BLOCK_PER_PAGE>::new();
        let mut entry = BlockId::new();
        entry.page_idx = 1;
        entry.uid = 2;
        let value = 123u128;
        let new_page_id = 1;
        let result = page_oram.write(&entry, &value, new_page_id);
        assert_eq!(result, None);
        let result = page_oram.read(&entry, new_page_id);
        assert_eq!(result, Some(value));
    }

    // #[test]
    // fn test_structured_oram_small() {
    //     let round = 1000;
    //     let mut ref_vec: Vec<(BlockId, u128)> = Vec::new();

    //     for _ in 0..round {
    //         let mut entry = BlockId::new();
    //         entry.page_idx = random::<usize>();
    //         entry.uid = random::<u64>();
    //         let value = random::<u128>();
    //         let new_page_id = random::<usize>();
    //         let result = page_oram.write(&entry, &value, new_page_id);
    //         assert_eq!(result, None);
    //         entry.page_idx = new_page_id;
    //         ref_vec.push((entry, value));
    //     }
    // }

    #[test]
    fn test_structured_oram_medium() {
        let mut page_oram = StructuredOram::<u128, 4>::new();
        let round = 200;
        let mut ref_vec: Vec<(BlockId, u128)> = Vec::new();

        for _ in 0..round {
            let mut entry = BlockId::new();
            entry.page_idx = random::<usize>();
            entry.uid = random::<u64>();
            let value = random::<u128>();
            let new_page_id = random::<usize>();
            let result = page_oram.write(&entry, &value, new_page_id);
            assert_eq!(result, None);
            entry.page_idx = new_page_id;
            ref_vec.push((entry, value));
        }
        // page_oram.print_state();

        for _ in 0..10 {
            for (entry, value) in ref_vec.iter_mut() {
                let new_page_id = random();
                // println!("Read entry: {:?}", entry);
                let result = page_oram.read(&entry, new_page_id);
                assert_eq!(result, Some(value.clone()));
                entry.page_idx = new_page_id;
                // page_oram.print_state();
            }
        }
        page_oram.print_meta_state();
    }
}
