use crate::fixoram::{BlockId, FixOram};
use crate::utils::RandGen;
use aes_gcm::aes::Block;
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
struct PosBlock<const B: usize> {
    pos: [usize; B],
}

impl<const B: usize> PosBlock<B> {
    pub fn new() -> Self {
        Self { pos: [0; B] }
    }
}

unsafe impl<const B: usize> Zeroable for PosBlock<B> {}
unsafe impl<const B: usize> Pod for PosBlock<B> {}

pub struct RecOramPosMap<const N: usize, const B: usize> {
    base_level: Vec<usize>,
    ext_levels: Vec<FixOram<PosBlock<B>, N>>,
    rand_gen: RandGen,
}

impl<const N: usize, const B: usize> RecOramPosMap<N, B> {
    pub fn new(size: usize) -> Self {
        let base_level = vec![0; size];
        let ext_levels = Vec::new();
        Self {
            base_level,
            ext_levels,
            rand_gen: RandGen::new(),
        }
    }

    pub fn get_pos(&mut self, uid: usize) -> usize {
        let base_idx = uid % self.base_level.len();
        let mut next_id = BlockId {
            page_idx: self.base_level[base_idx],
            uid,
        };
        if next_id.page_idx == 0 {
            // read a random path in the next level position map for obliviousness
            next_id.page_idx = self.rand_gen.gen();
        }
        let mut new_pos = self.rand_gen.gen();
        self.base_level[base_idx] = new_pos;
        let mut remain_offsets = uid / self.base_level.len();
        for level in self.ext_levels.iter_mut() {
            let block_offset = remain_offsets % B;
            remain_offsets /= B;
            let mut next_pos = 0 as usize;
            let next_new_pos = self.rand_gen.gen();
            let update_func = |pos_block: Option<PosBlock<B>>| {
                if let Some(mut pos_block_unwrap) = pos_block {
                    next_pos = pos_block_unwrap.pos[block_offset];
                    pos_block_unwrap.pos[block_offset] = next_new_pos;
                    pos_block
                } else {
                    next_pos = self.rand_gen.gen();
                    let mut new_pos_block = PosBlock::new();
                    new_pos_block.pos[block_offset] = next_new_pos;
                    Some(new_pos_block)
                }
            };
            level.update(&next_id, update_func, new_pos);
            next_id.page_idx = next_pos;
            new_pos = next_new_pos;
        }
        next_id.page_idx
    }
}
