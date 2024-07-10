use core::hash;

use crate::cuckoo::{CuckooHashMap, HashEntry};
use crate::pageoram::PageOram;
use crate::segvec::{SegmentedVector, MIN_SEGMENT_SIZE};
use std::fmt::Debug;
pub struct PageOmap {
    pageoram: PageOram,
    pos_map: CuckooHashMap<usize>,
}

impl PageOmap {
    pub fn new() -> Self {
        Self {
            pageoram: PageOram::new(),
            pos_map: CuckooHashMap::new(),
        }
    }

    pub fn insert<K: AsRef<[u8]> + Debug>(&mut self, key: K, value: &Vec<u8>) -> Option<Vec<u8>> {
        let new_page_id = rand::random::<usize>();
        // println!("key {:?} insert to new page id {:?}", key, new_page_id);
        let mut hash_entry = self.pos_map.compute_hash_entry(key, new_page_id);

        let old_page_id_option = self.pos_map.insert_hash_entry(&hash_entry);
        let old_page_id = match old_page_id_option {
            Some(id) => id,
            None => rand::random::<usize>(),
        };
        hash_entry.set_val(old_page_id);

        self.pageoram.write(&hash_entry, value, new_page_id)
    }

    pub fn get<K: AsRef<[u8]> + Debug>(&mut self, key: K) -> Option<Vec<u8>> {
        let new_page_id = rand::random::<usize>();
        // println!("key {:?} get and insert to new pos {:?}", key, new_page_id);
        let mut hash_entry = self.pos_map.compute_hash_entry(key, new_page_id);
        let old_page_id_option = self.pos_map.insert_hash_entry(&hash_entry);
        // println!("old_page_id_option: {:?}", old_page_id_option);
        let old_page_id = match old_page_id_option {
            Some(id) => id,
            None => rand::random::<usize>(),
        };
        hash_entry.set_val(old_page_id);
        self.pageoram.read(&hash_entry, new_page_id)
    }

    pub fn size(&self) -> usize {
        self.pos_map.size()
    }

    pub fn print_state(&self) {
        println!("PageOmap state:");
        self.pos_map.print_state();
        self.pageoram.print_state();
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_page_omap_simple() {
        let mut page_omap = PageOmap::new();
        let key = "hello";
        let value = vec![1, 2, 3, 4];
        let result = page_omap.insert(key, &value);
        assert_eq!(result, None);
        let result = page_omap.get(key);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn evict_test() {
        let mut map = PageOmap::new();
        let map_size = 16;
        for i in 0..map_size {
            let key = i.to_string();
            let value = vec![i as u8; 123];
            map.insert(&i.to_string(), &value);
        }
        // map.print_state();
        for i in 0..map_size {
            assert_eq!(map.get(&i.to_string()), Some(vec![i as u8; 123]));
            // println!("\nAfter get {:?}", i);
            // map.print_state();
        }
        assert_eq!(map_size, map.size());
    }
}
