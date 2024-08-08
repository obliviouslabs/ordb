use crate::segvec::SegmentedVector;
use bytemuck::{Pod, Zeroable};
use rand;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Debug;
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct HashEntry<V: Clone + Copy + Eq + Debug + Pod + Zeroable> {
    idx: [usize; 2],
    val: V,
}

impl<V: Clone + Copy + Eq + Debug + Pod + Zeroable> HashEntry<V> {
    pub fn new() -> Self {
        Self {
            idx: [0 as usize, 0 as usize],
            val: unsafe { std::mem::zeroed() },
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
        self.val == unsafe { std::mem::zeroed() }
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

const BKT_SIZE: usize = 2; // Example segment size

#[derive(Clone, Copy)]
struct HashBkt<V: Clone + Copy + Eq + Debug + Pod + Zeroable> {
    entries: [HashEntry<V>; BKT_SIZE],
}
unsafe impl<V: Clone + Copy + Eq + Debug + Pod + Zeroable> Zeroable for HashBkt<V> {}
unsafe impl<V: Clone + Copy + Eq + Debug + Pod + Zeroable> Pod for HashBkt<V> {}

impl<V: Clone + Copy + Eq + Debug + Pod + Zeroable> HashBkt<V> {
    pub fn new() -> Self {
        Self {
            entries: [HashEntry {
                idx: [0 as usize, 0 as usize],
                val: unsafe { std::mem::zeroed() },
            }; BKT_SIZE],
        }
    }
}

pub struct CuckooHashMap<V: Clone + Copy + Eq + Debug + Pod + Zeroable> {
    tables: [SegmentedVector<HashBkt<V>>; 2],
    size: usize,
    full_bkt_stash: HashMap<[usize; 2], V>,
    salt: [u8; 32],
}

impl<V: Clone + Copy + Eq + Debug + Pod + Zeroable> CuckooHashMap<V> {
    pub fn new() -> Self {
        Self {
            tables: [SegmentedVector::new(), SegmentedVector::new()],
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
        let mut bkts: [HashBkt<V>; 2] = [HashBkt::new(), HashBkt::new()];
        let mut bkt_indices: [usize; 2] = [0 as usize, 0 as usize];
        let table_capacity = self.tables[0].capacity();
        assert!(table_capacity == self.tables[1].capacity());
        let old_stash_entry = self.full_bkt_stash.remove(&entry.idx);
        self.size -= old_stash_entry.is_some() as usize;
        for i in 0..2 {
            bkt_indices[i] = entry.idx[i] % table_capacity;
            let bkt_idx = bkt_indices[i];
            bkts[i] = self.tables[i].get(bkt_idx).unwrap().clone();
            let bkt = &mut bkts[i];
            for j in 0..BKT_SIZE {
                if bkt.entries[j].is_match(entry.idx) {
                    // overwrite the entry
                    let old_val = bkt.entries[j].val;
                    bkt.entries[j] = entry;
                    self.tables[i].set(bkt_idx, bkt);
                    return Some(old_val);
                }
            }
        }
        self.size += 1;
        for i in 0..2 {
            let bkt_idx = bkt_indices[i];
            let bkt = &mut bkts[i];
            for j in 0..BKT_SIZE {
                if bkt.entries[j].idx[i] % table_capacity != bkt_idx {
                    // the entry can be overwritten
                    bkt.entries[j] = entry;
                    self.tables[i].set(bkt_idx, bkt);
                    return old_stash_entry;
                }
            }
        }
        // no empty slot found

        for iter in 0..MAX_ITER {
            for i in 0..2 {
                let bkt_idx = bkt_indices[i];
                let bkt = &mut bkts[i];
                if iter != 0 || i != 0 {
                    // otherwise we have already tried to insert the entry
                    for j in 0..BKT_SIZE {
                        if bkt.entries[j].idx[i] % table_capacity != bkt_idx {
                            // the entry can be overwritten
                            bkt.entries[j] = entry;
                            self.tables[i].set(bkt_idx, bkt);
                            return old_stash_entry;
                        }
                    }
                }
                let evict_idx = rand::random::<usize>() % BKT_SIZE;
                // swap the entry with the evicted entry
                let evicted_entry = bkt.entries[evict_idx];
                bkt.entries[evict_idx] = entry;
                entry = evicted_entry;
                self.tables[i].set(bkt_idx, bkt);
                // update the bkt for the other table
                let neg_i = 1 - i;
                bkt_indices[neg_i] = entry.idx[neg_i] % self.tables[neg_i].capacity();
                bkts[neg_i] = self.tables[neg_i].get(bkt_indices[neg_i]).unwrap().clone();
            }
        }
        print!("Cuckoo hash table is full insert to stash\n");
        // insert the entry to the bkt_full stash
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

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        let key_hash = self.hash_key(key);
        let bkt_idx = Self::get_bkt_idx(key_hash);
        let table_capacity = self.tables[0].capacity();
        assert!(table_capacity == self.tables[1].capacity());
        for i in 0..2 {
            let bkt = self.tables[i].get(bkt_idx[i] % table_capacity).unwrap();
            for j in 0..BKT_SIZE {
                if bkt.entries[j].is_match(bkt_idx) {
                    return Some(bkt.entries[j].val);
                }
            }
        }
        self.full_bkt_stash.get(&bkt_idx).cloned()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.tables[0].capacity() * 3
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
            println!("Table capacity: {}", self.tables[i].capacity());
        }
        println!("Full bkt stash size: {}", self.full_bkt_stash.len());
    }

    pub fn print_state(&self) {
        for i in 0..2 {
            println!("Table {}", i);
            for j in 0..self.tables[i].capacity() {
                let bkt = self.tables[i].get(j).unwrap();
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
        let mut map = CuckooHashMap::<u128>::new();
        map.insert("hello", 42);
        assert_eq!(map.get("hello"), Some(42));
        map.insert("123", 123);
        assert_eq!(map.get("123"), Some(123));
        assert_eq!(2, map.size());
    }

    #[test]
    fn dup_test() {
        let mut map = CuckooHashMap::<u128>::new();
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
        let mut map = CuckooHashMap::<u64>::new();
        for i in 0..2800 {
            map.insert(&i.to_string(), i);
        }
        for i in 0..2800 {
            assert_eq!(map.get(&i.to_string()), Some(i));
        }
        assert_eq!(2800, map.size());
    }

    #[test]
    fn scale_test() {
        let mut map = CuckooHashMap::<u64>::new();
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
        let mut map = CuckooHashMap::<u64>::new();
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
