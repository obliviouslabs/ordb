use crate::linearoram::LinearOram;
use crate::recoram::RecOram;
use crate::utils::SimpleVal;
use bytemuck::{Pod, Zeroable};
use rand;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HashEntry<V: SimpleVal> {
    idx: [usize; 2],
    val: V,
}

impl<V: SimpleVal> HashEntry<V> {
    pub fn new() -> Self {
        Self {
            idx: [0 as usize, 0 as usize],
            val: V::zeroed(),
        }
    }

    pub fn is_match(&self, idx: [usize; 2]) -> bool {
        for i in 0..2 {
            if self.idx[i] != idx[i] {
                return false;
            }
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.val == V::zeroed()
    }

    pub fn delete(&mut self) {
        self.idx = [0 as usize, 0 as usize];
        self.val = V::zeroed();
    }

    pub fn eq(&self, other: &Self) -> bool {
        for i in 0..2 {
            if self.idx[i] != other.idx[i] {
                return false;
            }
        }
        self.val == other.val
    }

    pub fn get_val(&self) -> V {
        self.val
    }

    pub fn set_val(&mut self, val: V) {
        self.val = val;
    }

    pub fn set_idx(&mut self, idx: [usize; 2]) {
        self.idx = idx;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HashBkt<V: SimpleVal, const BKT_SIZE: usize> {
    entries: [HashEntry<V>; BKT_SIZE],
}
unsafe impl<V: SimpleVal, const BKT_SIZE: usize> Zeroable for HashBkt<V, BKT_SIZE> {}
unsafe impl<V: SimpleVal, const BKT_SIZE: usize> Pod for HashBkt<V, BKT_SIZE> {}

impl<V: SimpleVal, const BKT_SIZE: usize> HashBkt<V, BKT_SIZE> {
    pub fn new() -> Self {
        Self {
            entries: [HashEntry {
                idx: [0 as usize, 0 as usize],
                val: unsafe { std::mem::zeroed() },
            }; BKT_SIZE],
        }
    }
}

pub struct CuckooHashMap<V: SimpleVal, const BKT_SIZE: usize, const BKT_PER_PAGE: usize> {
    tables: [RecOram<HashBkt<V, BKT_SIZE>, BKT_PER_PAGE>; 2],
    size: usize,
    full_bkt_stash: HashMap<[usize; 2], V>,
    salt: [u8; 32],
}

impl<V: SimpleVal, const BKT_SIZE: usize, const BKT_PER_PAGE: usize>
    CuckooHashMap<V, BKT_SIZE, BKT_PER_PAGE>
{
    pub fn new() -> Self {
        Self {
            tables: [
                RecOram::<HashBkt<V, BKT_SIZE>, BKT_PER_PAGE>::new(128),
                RecOram::<HashBkt<V, BKT_SIZE>, BKT_PER_PAGE>::new(128),
            ],
            size: 0,
            full_bkt_stash: HashMap::new(),
            salt: rand::random::<[u8; 32]>(), // change to secure random
        }
    }

    fn hash_key<K: AsRef<[u8]>>(&self, key: K) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&self.salt);
        hasher.update(key);
        let result = hasher.finalize();
        let mut key_hash = [0 as u8; 32];
        key_hash.copy_from_slice(&result[..]);
        key_hash
    }

    fn get_bkt_idx(key_hash: [u8; 32]) -> [usize; 2] {
        let mut bkt_idx: [usize; 2] = [0 as usize, 0 as usize];
        for i in 0..2 {
            const IDX_SIZE: usize = std::mem::size_of::<usize>();
            let idx_arr: [u8; IDX_SIZE] = key_hash[i * IDX_SIZE..(i + 1) * IDX_SIZE]
                .try_into()
                .unwrap();
            bkt_idx[i] = usize::from_ne_bytes(idx_arr);
        }
        bkt_idx
    }

    pub fn insert_hash_entry(&mut self, hash_entry: &HashEntry<V>) -> Option<V> {
        let mut entry = hash_entry.clone();
        // get hash of key
        if self.size >= self.capacity() {
            self.double_size();
        }

        const MAX_ITER: usize = 10;
        let table_capacity = self.tables[0].size();
        assert!(table_capacity == self.tables[1].size());
        let old_stash_entry = self.full_bkt_stash.remove(&entry.idx);
        self.size -= old_stash_entry.is_some() as usize;
        let mut ret = None;
        let mut inserted_flag = false;
        let mut need_evict_flag = false;

        for iter in 0..MAX_ITER {
            for i in 0..2 {
                let bkt_idx = entry.idx[i] % table_capacity;
                let update_func = |bkt: Option<HashBkt<V, BKT_SIZE>>| {
                    // println!("Read bkt at table {} index {}", i, bkt_idx);
                    // println!("bkt: {:?}", bkt);
                    let mut bkt = bkt.unwrap_or_else(|| HashBkt::new());
                    for j in 0..BKT_SIZE {
                        if iter == 0 && bkt.entries[j].is_match(entry.idx) {
                            // overwrite the entry
                            ret = Some(bkt.entries[j].val);
                            bkt.entries[j].delete();
                            self.size -= 1;
                        }
                        if !inserted_flag && bkt.entries[j].idx[i] % table_capacity != bkt_idx {
                            // insert the entry
                            bkt.entries[j] = entry;
                            self.size += 1;
                            inserted_flag = true;
                        }
                    }
                    if !inserted_flag && need_evict_flag {
                        let evict_idx = rand::random::<usize>() % BKT_SIZE;
                        std::mem::swap(&mut entry, &mut bkt.entries[evict_idx]);
                    }
                    // println!("Write back bkt at table {} index {}", i, bkt_idx);
                    // println!("bkt: {:?}", bkt);
                    Some(bkt)
                };
                self.tables[i].update(bkt_idx, update_func);
                if ret.is_some() {
                    assert!(inserted_flag);
                    return ret;
                }
                if iter + i != 0 && inserted_flag {
                    // the entry is inserted
                    return old_stash_entry;
                }
                need_evict_flag = true; // need to evict an entry if the entry is not inserted
            }
        }

        print!("Cuckoo hash table is full insert to stash\n");
        // insert the entry to the bkt_full stash
        self.size += 1;
        self.full_bkt_stash.insert(entry.idx, entry.val);
        old_stash_entry
    }

    pub fn compute_hash_entry<K: AsRef<[u8]>>(&self, key: K, value: V) -> HashEntry<V> {
        let key_hash = self.hash_key(key);
        let bkt_idx = Self::get_bkt_idx(key_hash);
        HashEntry {
            idx: bkt_idx,
            val: value,
        }
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, value: V) -> Option<V> {
        let entry = self.compute_hash_entry(key.as_ref(), value);
        self.insert_hash_entry(&entry)
    }

    pub fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Option<V> {
        let key_hash = self.hash_key(key);
        let bkt_idx = Self::get_bkt_idx(key_hash);
        let table_capacity = self.tables[0].size();
        assert!(table_capacity == self.tables[1].size());
        for i in 0..2 {
            let bkt = self.tables[i].read(bkt_idx[i] % table_capacity);
            if let Some(bkt) = bkt {
                for j in 0..BKT_SIZE {
                    if bkt.entries[j].is_match(bkt_idx) {
                        return Some(bkt.entries[j].val);
                    }
                }
            }
        }
        self.full_bkt_stash.get(&bkt_idx).cloned()
    }

    pub fn update_hash_entry(&mut self, entry: &HashEntry<V>) -> Option<V> {
        let bkt_idx = entry.idx;
        let table_capacity = self.tables[0].size();
        assert!(table_capacity == self.tables[1].size());
        let mut old_val = None;
        for i in 0..2 {
            let update_func = |bkt: Option<HashBkt<V, BKT_SIZE>>| {
                let mut bkt = bkt.unwrap_or_else(|| HashBkt::new());
                for j in 0..BKT_SIZE {
                    if bkt.entries[j].is_match(bkt_idx) {
                        old_val = Some(bkt.entries[j].val);
                        bkt.entries[j].val = entry.val;
                        return Some(bkt);
                    }
                }
                Some(bkt)
            };
            self.tables[i].update(bkt_idx[i] % table_capacity, update_func);
            if old_val.is_some() {
                return old_val;
            }
        }
        let stash_res = self.full_bkt_stash.get(&bkt_idx);
        if stash_res.is_some() {
            old_val = stash_res.cloned();
            self.full_bkt_stash.insert(bkt_idx, entry.val);
        }
        old_val
    }

    pub fn get_parallel<K: AsRef<[u8]>>(&mut self, key: K) -> Option<V> {
        let key_hash = self.hash_key(key);
        let bkt_idx = Self::get_bkt_idx(key_hash);
        let table_capacity = self.tables[0].size();
        assert!(table_capacity == self.tables[1].size());
        let result = Arc::new(Mutex::new(None));
        self.tables
            .par_iter_mut()
            .zip(bkt_idx)
            .for_each(|(table, idx)| {
                let bkt = table.read(idx % table_capacity);
                if let Some(bkt) = bkt {
                    for j in 0..BKT_SIZE {
                        if bkt.entries[j].is_match(bkt_idx) {
                            let mut res = result.lock().unwrap();
                            *res = Some(bkt.entries[j].val);
                            break;
                        }
                    }
                }
            });
        let res = result.lock().unwrap();
        if res.is_some() {
            return *res;
        }
        self.full_bkt_stash.get(&bkt_idx).cloned()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.tables[0].size() * 3
    }

    pub fn double_size(&mut self) {
        for i in 0..2 {
            self.tables[i].double_size_and_fork_self();
        }
    }

    pub fn print_meta_state(&self) {
        println!("CuckooHashMap meta state:");
        println!("Size: {}", self.size);
        for i in 0..2 {
            println!("Table {}", i);
            self.tables[i].print_meta_state();
        }
        println!("Full bkt stash size: {}", self.full_bkt_stash.len());
    }

    pub fn print_state(&mut self) {
        for i in 0..2 {
            println!("Table {}", i);
            for j in 0..self.tables[i].size() {
                let bkt = self.tables[i].read(j).unwrap();
                for k in 0..BKT_SIZE {
                    println!("Bkt[{}][{}]: {:?}", j, k, bkt.entries[k]);
                }
            }
        }
        println!("Full bkt stash: {:?}", self.full_bkt_stash);
    }
}

mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut map = CuckooHashMap::<u128, 4, 4>::new();
        map.insert("hello", 42);
        assert_eq!(map.get("hello"), Some(42));
        map.insert("123", 123);
        assert_eq!(map.get("123"), Some(123));
        assert_eq!(2, map.size());
    }

    #[test]
    fn dup_test() {
        let mut map = CuckooHashMap::<u128, 4, 4>::new();
        map.insert("hello", 42);
        assert_eq!(map.get("hello"), Some(42));
        map.insert("hello", 43);
        assert_eq!(map.get("hello"), Some(43));
        map.insert("123", 123);
        assert_eq!(map.get("123"), Some(123));
        assert_eq!(2, map.size());
    }

    #[test]
    fn evict_test() {
        let mut map = CuckooHashMap::<u64, 8, 8>::new();
        for i in 0..280 {
            map.insert(&i.to_string(), i);
        }
        for i in 0..280 {
            assert_eq!(map.get(&i.to_string()), Some(i));
        }
        assert_eq!(280, map.size());
    }

    #[test]
    fn scale_test() {
        let mut map = CuckooHashMap::<u64, 16, 8>::new();
        for i in 0..10000 {
            let res = map.insert(&i.to_string(), i);
            assert_eq!(res, None);
        }
        for i in 0..10000 {
            assert_eq!(map.get(&i.to_string()), Some(i));
        }
        assert_eq!(10000, map.size());
    }

    #[test]
    fn scale_and_dup_test() {
        let mut map = CuckooHashMap::<u64, 8, 16>::new();
        for i in 0..10000 {
            map.insert(&i.to_string(), i);
        }
        for i in 0..5000 {
            let res = map.insert(&i.to_string(), i + 1);
            assert_eq!(res, Some(i));
        }
        for i in 0..5000 {
            assert_eq!(map.get(&i.to_string()), Some(i + 1));
        }
        for i in 5000..10000 {
            assert_eq!(map.get(&i.to_string()), Some(i));
        }
        assert_eq!(10000, map.size());
    }
}
