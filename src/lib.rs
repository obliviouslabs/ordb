mod oblivious;
mod params;
mod storage;
mod tree;
mod utils;

use oblivious::flexomap::FlexOmap;
use std::sync::Mutex;
pub struct ObliviousDB {
    flexomap: Mutex<FlexOmap>,
}

impl ObliviousDB {
    pub fn new() -> Self {
        Self {
            flexomap: Mutex::new(FlexOmap::new()),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.flexomap.lock().unwrap().get(key)
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) {
        self.flexomap.lock().unwrap().insert(key, value);
    }

    pub fn print_meta_state(&self) {
        self.flexomap.lock().unwrap().print_meta_state();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
