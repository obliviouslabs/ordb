use crate::cuckoo::CuckooHashMap;
use crate::flexoram::FlexOram;

use std::fmt::Debug;
pub struct FlexOmap {
    flexoram: FlexOram,
    pos_map: CuckooHashMap<usize, 8, 16>,
}

impl FlexOmap {
    pub fn new() -> Self {
        Self {
            flexoram: FlexOram::new(),
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

        self.flexoram
            .read_and_write(&hash_entry, value, new_page_id)
    }

    pub fn get<K: AsRef<[u8]> + Debug>(&mut self, key: K) -> Option<Vec<u8>> {
        let new_page_id = rand::random::<usize>();
        // println!("key {:?} get and insert to new pos {:?}", key, new_page_id);
        let mut hash_entry = self.pos_map.compute_hash_entry(key, new_page_id);
        let old_page_id_option = self.pos_map.update_hash_entry(&hash_entry);
        // println!("old_page_id_option: {:?}", old_page_id_option);
        let old_page_id = match old_page_id_option {
            Some(id) => id,
            None => rand::random::<usize>(),
        };
        hash_entry.set_val(old_page_id);
        self.flexoram.read(&hash_entry, new_page_id)
    }

    pub fn size(&self) -> usize {
        self.pos_map.size()
    }

    pub fn print_meta_state(&self) {
        println!("FlexOmap meta state:");
        self.pos_map.print_meta_state();
        self.flexoram.print_meta_state();
    }

    pub fn print_state(&mut self) {
        println!("FlexOmap state:");
        self.pos_map.print_state();
        // self.flexoram.print_state();
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_flex_omap_simple() {
        let mut flex_omap = FlexOmap::new();
        let key = "hello";
        let value = vec![1, 2, 3, 4];
        let result = flex_omap.insert(key, &value);
        assert_eq!(result, None);
        let result = flex_omap.get(key);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_flex_omap_dup() {
        let mut flex_omap = FlexOmap::new();
        let key = "hello";
        let value = vec![1, 2, 3, 4];
        let result = flex_omap.insert(key, &value);
        assert_eq!(result, None);
        let result = flex_omap.get(key);
        assert_eq!(result, Some(value));
        let value = vec![5, 6, 7, 8, 9];
        let result = flex_omap.insert(key, &value);
        assert_eq!(result, Some(vec![1, 2, 3, 4]));
        let result = flex_omap.get(key);
        assert_eq!(result, Some(value));
    }

    #[test]
    fn evict_test() {
        let mut map = FlexOmap::new();
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

    #[test]
    fn scale_and_dup_test() {
        let mut map = FlexOmap::new();
        for i in 0..10000 {
            map.insert(&i.to_string(), &vec![i as u8; 43]);
        }
        for i in 0..5000 {
            let res = map.insert(&i.to_string(), &vec![(i + 1) as u8; 125]);
            assert_eq!(res, Some(vec![i as u8; 43]));
        }
        for i in 0..5000 {
            assert_eq!(map.get(&i.to_string()), Some(vec![(i + 1) as u8; 125]));
        }
        for i in 5000..10000 {
            assert_eq!(map.get(&i.to_string()), Some(vec![i as u8; 43]));
        }
        assert_eq!(10000, map.size());
        map.print_meta_state();
    }

    #[test]
    fn omap_fix_size() {
        let mut map = FlexOmap::new();
        let size = 1000000;
        for i in 0..size {
            map.insert(&i.to_string(), &vec![i as u8; 32]);
        }
        let read_round = 1000000;
        for r in 0..read_round {
            let i = (r * 929) % size;
            assert_eq!(map.get(&i.to_string()), Some(vec![i as u8; 32]));
        }
        assert_eq!(size, map.size());
    }

    #[test]
    fn omap_large() {
        let mut map = FlexOmap::new();
        let size = 1000000;
        for i in 0..size {
            map.insert(&i.to_string(), &vec![i as u8; i % 400]);
        }
        let read_round = 1000000;
        for r in 0..read_round {
            let i = (r * 929) % size;
            assert_eq!(map.get(&i.to_string()), Some(vec![i as u8; i % 400]));
        }
        assert_eq!(size, map.size());
        map.print_meta_state();
    }
}
