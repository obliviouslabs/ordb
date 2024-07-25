use crate::segvec::{SegmentedVector, MIN_SEGMENT_SIZE};

pub struct ORAMTree<T: Clone + Copy> {
    tree: Vec<SegmentedVector<T>>,
    max_branching_factor: usize,
    top_vec_max_size: usize,
    total_size: usize,
}

impl<T: Clone + Copy> ORAMTree<T> {
    pub fn new(top_vec_max_size: usize) -> Self {
        let mut tree = Vec::new();
        tree.push(SegmentedVector::new());
        let total_size = tree[0].capacity();
        Self {
            tree,
            max_branching_factor: 16,
            top_vec_max_size,
            total_size,
        }
    }

    pub fn read_path(&self, index: usize) -> (Vec<&T>, Vec<usize>) {
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

    pub fn write_path_move(&mut self, index: usize, path: Vec<T>) {
        for (i, vec) in self.tree.iter_mut().enumerate() {
            vec.set_move(index % vec.capacity(), path[i]);
        }
    }

    pub fn scale(&mut self, target_branching_factor: usize) {
        let mut below_layer_size = self.tree[0].capacity() * (target_branching_factor + 1);
        // if the current branching factor is too large, don't scale the bottom layer. Instead, first scale the middle layers
        if self.max_branching_factor > target_branching_factor {
            below_layer_size = self.tree[0].capacity();
        }
        // scale starting from the bottom layer
        self.total_size = 0;
        for vec in self.tree.iter_mut() {
            if vec.capacity() * target_branching_factor < below_layer_size {
                vec.double_size_and_fork_self();
            } else if vec.capacity() * self.max_branching_factor < below_layer_size {
                // max branching factor could potentially increase if the layer below is scaled
                // but the current layer is not
                self.max_branching_factor = below_layer_size / vec.capacity();
            }
            below_layer_size = vec.capacity();
            self.total_size += below_layer_size;
        }
        if below_layer_size > self.top_vec_max_size {
            // add a new layer
            let mut new_top_vec = SegmentedVector::new();
            while new_top_vec.capacity() * target_branching_factor < below_layer_size {
                new_top_vec.double_size();
            }
            self.total_size += new_top_vec.capacity();
            self.tree.push(new_top_vec);
        }
    }

    pub fn min_layer_size(&self) -> usize {
        self.tree.last().unwrap().capacity()
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }
}
