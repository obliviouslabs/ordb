use std::vec;

use crate::params::{KEY_SIZE, MAX_CACHE_SIZE, MIN_SEGMENT_SIZE, PAGE_SIZE};
use crate::tree::dynamictree::{calc_deepest, ORAMTree};
use crate::utils::utils::SimpleVal;
use bytemuck::{Pod, Zeroable};

pub const BUFFER_SIZE: usize = PAGE_SIZE - 2 * std::mem::size_of::<u16>() - KEY_SIZE;
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BlockId {
    pub page_idx: usize,
    pub uid: usize,
}

impl BlockId {
    pub fn new() -> Self {
        Self {
            page_idx: 0,
            uid: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Page<T: SimpleVal, const N: usize> {
    indices: [BlockId; N],
    data: [T; N],
}

unsafe impl<T: SimpleVal, const N: usize> Zeroable for Page<T, N> {}
unsafe impl<T: SimpleVal, const N: usize> Pod for Page<T, N> {}
impl<T: SimpleVal, const N: usize> Page<T, N> {
    fn new() -> Self {
        Page {
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
            if self.indices[i] == *meta_data && self.indices[i] != BlockId::new() {
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

        &mut self.stash[stash_idx].kvs
    }

    pub fn insert(&mut self, idx: usize, entry: BlockId, value: T) {
        let stash_idx = idx % self.size;
        self.split_entry(stash_idx); // potentially split the entry
                                     // we don't perform merge during insert
        self.num_kvs += 1;
        self.stash[stash_idx].kvs.push((entry, value));
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn num_kvs(&self) -> usize {
        self.num_kvs
    }

    pub fn num_bytes(&self) -> usize {
        self.num_kvs * Self::STASH_ENTRY_SIZE
            + self.size * (std::mem::size_of::<StashEntry<T>>() + 1)
    }
}
#[derive(Clone)]
struct EvictInfo {
    is_from_stash: bool,
    src: u8,
    offset: u16,
}

pub struct FixOram<T: SimpleVal, const N: usize> {
    tree: ORAMTree<Page<T, N>>,
    stash: Stash<T>,
    num_entry: usize,
    evict_infos_cache: Vec<Vec<EvictInfo>>, // a cache to store the src position of entries to evict
    empty_slots_cache: Vec<Vec<u16>>,       // a cache to store the empty slots in the path
    stash_remain_cache: Vec<u16>,           // cache the idx of stash entries that are not evicted
}

impl<T: SimpleVal, const N: usize> FixOram<T, N> {
    pub fn new() -> Self {
        Self {
            tree: ORAMTree::new(MAX_CACHE_SIZE),
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

    fn scale_if_load_high(&mut self) {
        let load_factor = self.num_bytes() as f64 / (self.tree.total_size() * BUFFER_SIZE) as f64;
        if load_factor > 0.7 {
            println!(
                "load bytes: {} total bytes: {}",
                self.num_bytes(),
                self.tree.total_size() * BUFFER_SIZE
            );
            self.scale();
        }
    }

    fn retrieve(&mut self, id: &BlockId) -> Option<T> {
        let path_idx = id.page_idx;
        let (mut path, layer_sizes) = self.tree.read_path(path_idx);
        let num_layer = layer_sizes.len();
        let layer_log_sizes: Vec<u8> = layer_sizes
            .iter()
            .map(|x| x.trailing_zeros() as u8)
            .collect();
        let mut result: Option<T> = None;

        for (i, page) in path.iter_mut().enumerate() {
            let entry = page.read_and_remove_entry(id);
            if entry.is_some() {
                result = entry;
            }
            for j in 0..N {
                let page_idx = page.indices[j].page_idx;
                let deepest = calc_deepest(page_idx, path_idx, &layer_log_sizes);
                if deepest > i as u8 {
                    self.empty_slots_cache[i].push(j as u16);
                } else if deepest < i as u8 {
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

        let mut curr_evict_info_level = 0;
        let mut complete_flag = false;
        for dst in 0..num_layer {
            let (down_empty_slots, up_empty_slots) = self.empty_slots_cache.split_at_mut(dst + 1);
            for slot_offset in down_empty_slots.last().unwrap().iter() {
                // if curr_evict_info_level < dst {
                //     // no available block to evict to dst
                //     continue;
                // }
                while self.evict_infos_cache[curr_evict_info_level].is_empty() {
                    curr_evict_info_level += 1;
                    if curr_evict_info_level == num_layer {
                        complete_flag = true;
                        break;
                    }
                }
                if complete_flag {
                    break;
                }
                if curr_evict_info_level > dst {
                    // no available block to evict to dst
                    break;
                }

                let evict_info = self.evict_infos_cache[curr_evict_info_level].pop().unwrap();

                if evict_info.is_from_stash {
                    let (block_id, value) = stash_vec[evict_info.offset as usize];
                    path[dst].insert(*slot_offset, &block_id, &value);
                } else {
                    let src = evict_info.src as usize;
                    if src <= dst {
                        // since the blocks are read from the evict_infos_cache in a top-down order, we can clear the vector and break here
                        self.evict_infos_cache[curr_evict_info_level].clear();
                        break;
                    }
                    let src_offset = evict_info.offset as usize;
                    let dst_offset = *slot_offset as usize;

                    up_empty_slots[src - dst - 1].push(src_offset as u16); // we have new empty slots now
                    let (down_path, up_path) = path.split_at_mut(src);
                    std::mem::swap(
                        &mut down_path[dst].indices[dst_offset],
                        &mut up_path[0].indices[src_offset],
                    );
                    std::mem::swap(
                        &mut down_path[dst].data[dst_offset],
                        &mut up_path[0].data[src_offset],
                    );
                }
            }
            if complete_flag {
                break;
            }
        }

        // remove the evicted slots from stash
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
        result
    }

    pub fn update<F>(&mut self, id: &BlockId, update_func: F, new_page_id: usize)
    where
        F: FnOnce(Option<T>, usize) -> (Option<T>, usize),
    {
        let result = self.retrieve(id);
        let found_flag = result.is_some();
        let (result, new_uid) = update_func(result, id.uid);
        let remain_flag = result.is_some();
        if remain_flag {
            self.num_entry += 1;
        }
        if found_flag {
            self.num_entry -= 1;
        }
        if result.is_some() {
            let new_id = BlockId {
                page_idx: new_page_id,
                uid: new_uid,
            };

            self.stash
                .insert(new_page_id, new_id, result.unwrap().clone());
        }
        self.scale_if_load_high();
    }

    pub fn update_and_write_multiple<F>(&mut self, id: &BlockId, update_func: F)
    where
        F: FnOnce(Option<T>, usize) -> Vec<(T, usize, usize)>,
    {
        let result = self.retrieve(id);
        let found_flag = result.is_some();
        let write_backs = update_func(result, id.uid);
        self.num_entry += write_backs.len();
        if found_flag {
            self.num_entry -= 1;
        }
        for (result, new_uid, new_page_id) in write_backs {
            let new_id = BlockId {
                page_idx: new_page_id,
                uid: new_uid,
            };

            self.stash.insert(new_page_id, new_id, result);
        }
        self.scale_if_load_high();
    }

    fn scale(&mut self) {
        let target_branching_factor = N;
        println!("Scaling to branching factor {}", target_branching_factor);
        self.tree.scale(target_branching_factor);
        let new_stash_size = self.tree.min_layer_size();
        // todo: optimize this with shallow copy
        self.stash.scale(new_stash_size)
    }

    pub fn read(&mut self, id: &BlockId, new_page_id: usize) -> Option<T> {
        let mut ret = None;
        let dummy_func = |x: Option<T>, uid| {
            ret = x;
            (x, uid)
        };
        self.update(id, dummy_func, new_page_id);
        ret
    }

    pub fn write(&mut self, id: &BlockId, value: &T, new_page_id: usize) {
        let overwrite_func = |_, uid| (Some(*value), uid);
        self.update(id, overwrite_func, new_page_id);
    }

    pub fn print_meta_state(&self) {
        println!("FixOram meta state:");
        // println!("FixOram pages count: {}", self.pages.capacity());
        println!("FixOram stash kv count: {}", self.stash.num_kvs());
        println!(
            "FixOram stash size: {} MB",
            self.stash.num_bytes() as f64 / (1024 * 1024) as f64
        );
    }
    // pub fn print_state(&self) {
    //     for i in 0..self.stash.size {
    //         let kvs = &self.stash.stash[i].kvs;
    //         println!("Stash entry: ");
    //         for (entry, value) in kvs.iter() {
    //             println!("({:?} {:?})", entry, value);
    //         }
    //     }
    //     self.tree.print_state();
    // }
    pub fn get_all(&self) -> Vec<(BlockId, T)> {
        let mut ret = Vec::new();
        for i in 0..self.stash.size {
            let kvs = &self.stash.stash[i].kvs;
            for (entry, value) in kvs.iter() {
                ret.push((entry.clone(), value.clone()));
            }
        }
        let tree_entries = self.tree.get_all();
        for (idx, level_size, page) in tree_entries.iter() {
            for i in 0..N {
                let entry = page.indices[i];
                let value = page.data[i];
                if entry.page_idx % level_size == *idx && entry != BlockId::new() {
                    ret.push((entry, value));
                }
            }
        }
        ret
    }
}

mod tests {
    use super::*;
    use rand::random;
    #[test]
    fn test_fix_oram_simple() {
        const BLOCK_PER_PAGE: usize =
            (BUFFER_SIZE / (std::mem::size_of::<(BlockId, u128)>())) as usize;
        let mut page_oram = FixOram::<u128, BLOCK_PER_PAGE>::new();
        let mut entry = BlockId::new();
        entry.page_idx = 1;
        entry.uid = 2;
        let value = 123u128;
        let new_page_id = 1;
        page_oram.write(&entry, &value, new_page_id);
        let result = page_oram.read(&entry, new_page_id);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_fix_oram_medium() {
        const BLOCK_PER_PAGE: usize =
            (BUFFER_SIZE / (std::mem::size_of::<(BlockId, u128)>())) as usize;
        let mut page_oram = FixOram::<u128, BLOCK_PER_PAGE>::new();
        let round = 100000;
        let mut ref_vec: Vec<(BlockId, u128)> = Vec::new();

        for _ in 0..round {
            let mut entry = BlockId::new();
            entry.page_idx = random::<usize>();
            entry.uid = random::<usize>();
            let value = random::<u128>();
            let new_page_id = random::<usize>();
            page_oram.write(&entry, &value, new_page_id);
            entry.page_idx = new_page_id;
            ref_vec.push((entry, value));
        }
        // page_oram.print_state();
        let kvs = page_oram.get_all();
        assert_eq!(kvs.len(), round);

        for _ in 0..10 {
            for (entry, value) in ref_vec.iter_mut() {
                let new_page_id = random();
                // println!("Read entry: {:?}", entry);
                let result = page_oram.read(&entry, new_page_id);
                // println!("State after read:");
                // page_oram.print_state();
                assert_eq!(result, Some(value.clone()));
                entry.page_idx = new_page_id;
                // page_oram.print_state();
            }
        }
        page_oram.print_meta_state();
    }
}
