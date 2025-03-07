use super::segvec::SegmentedVec;
use bytemuck::{Pod, Zeroable};
pub struct ORAMTree<T: Clone + Copy + Pod + Zeroable> {
    tree: Vec<SegmentedVec<T>>,
    top_vec_max_size: usize,
    total_size: usize,
}

impl<T: Clone + Copy + Pod + Zeroable> ORAMTree<T> {
    pub fn new(top_vec_max_size: usize) -> Self {
        let mut tree = Vec::new();
        tree.push(SegmentedVec::new());
        let total_size = tree[0].capacity();
        Self {
            tree,
            top_vec_max_size,
            total_size,
        }
    }

    fn read_node(&self, index: usize, level: usize) -> T {
        self.tree[level]
            .get(index % self.tree[level].capacity())
            .unwrap()
    }

    pub fn read_path(&self, index: usize) -> (Vec<T>, Vec<usize>) {
        let mut path = Vec::new();
        let mut capacities = Vec::new();
        path.reserve(self.tree.len());
        capacities.reserve(self.tree.len());
        for vec in self.tree.iter() {
            path.push(vec.get(index % vec.capacity()).unwrap());
            capacities.push(vec.capacity());
        }
        (path, capacities)
    }

    pub fn write_path(&mut self, index: usize, path: &Vec<T>) {
        for (i, vec) in self.tree.iter_mut().enumerate() {
            vec.set(index % vec.capacity(), &path[i]);
        }
    }

    pub fn scale(&mut self, mut target_branching_factor: usize) {
        if target_branching_factor < 2 {
            target_branching_factor = 2;
        }
        let init_min_layer_size = self.min_layer_size();
        let mut min_branching_factor = usize::max_value() / 2;
        let mut min_branching_factor_layer = 0;
        for i in 0..self.tree.len() - 1 {
            let branching_factor = self.tree[i].capacity() / self.tree[i + 1].capacity();
            if branching_factor < min_branching_factor {
                min_branching_factor = branching_factor;
                min_branching_factor_layer = i;
            }
        }
        if min_branching_factor * 2 <= target_branching_factor {
            // scale the layer with the smallest branching factor
            self.total_size += self.tree[min_branching_factor_layer].capacity();
            self.tree[min_branching_factor_layer].double_size_and_fork_self();
        } else {
            // scale the top layer
            self.total_size += self.tree.last().unwrap().capacity();
            self.tree.last_mut().unwrap().double_size_and_fork_self();
            if self.tree.last().unwrap().capacity() > self.top_vec_max_size {
                // add a new layer
                let mut new_top_vec = SegmentedVec::new();
                while new_top_vec.capacity() < init_min_layer_size {
                    new_top_vec.double_size_and_fork_self();
                }
                self.total_size += new_top_vec.capacity();
                self.tree.push(new_top_vec);
            }
        }
    }

    pub fn min_layer_size(&self) -> usize {
        self.tree.last().unwrap().capacity()
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    // pub fn print_state(&self) {
    //     for (i, vec) in self.tree.iter().enumerate() {
    //         println!("Layer {}:", i);
    //         for j in 0..vec.capacity() {
    //             println!("{}: {:?}", j, vec.get(j).unwrap());
    //         }
    //     }
    // }

    pub fn get_all(&self) -> Vec<(usize, usize, T)> {
        let mut all = Vec::new();
        all.reserve(self.total_size);
        for vec in self.tree.iter() {
            for i in 0..vec.capacity() {
                all.push((i, vec.capacity(), vec.get(i).unwrap()));
            }
        }
        all
    }
}

pub fn calc_deepest(self_idx: usize, other_idx: usize, layer_log_sizes: &Vec<u8>) -> u8 {
    let tzcnt = (self_idx ^ other_idx).trailing_zeros() as u8;
    for (i, log_layer_size) in layer_log_sizes.iter().enumerate() {
        if tzcnt >= *log_layer_size {
            return i as u8;
        }
    }
    layer_log_sizes.len() as u8
}
