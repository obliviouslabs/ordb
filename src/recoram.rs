use crate::fixoram::{BlockId, FixOram};
use crate::utils::{get_low_bits, RandGen};
use aes_gcm::aes::Block;
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
struct PosBlock<const B: usize> {
    pos: [usize; B],
    versions: [u8; B], // the current version of the index in the next map
}

impl<const B: usize> PosBlock<B> {
    pub fn new() -> Self {
        Self {
            pos: [0; B],
            versions: [0; B],
        }
    }
}

unsafe impl<const B: usize> Zeroable for PosBlock<B> {}
unsafe impl<const B: usize> Pod for PosBlock<B> {}

pub struct RecOramPosMap<const N: usize, const B: usize> {
    base_level_pos: Vec<usize>,
    base_level_versions: Vec<u8>,
    ext_levels: Vec<FixOram<PosBlock<B>, N>>,
    ext_level_log_sizes: Vec<u8>,
    rand_gen: RandGen,
}

impl<const N: usize, const B: usize> RecOramPosMap<N, B> {
    pub fn new(size: usize) -> Self {
        Self {
            base_level_pos: vec![0; size],
            base_level_versions: vec![size.trailing_zeros() as u8; size],
            ext_levels: Vec::new(),
            ext_level_log_sizes: Vec::new(),
            rand_gen: RandGen::new(),
        }
    }

    pub fn get_pos(&mut self, uid: usize) -> usize {
        let base_idx = uid % self.base_level_pos.len();
        let mut next_version = self.base_level_versions[base_idx];
        let mut next_id = BlockId {
            page_idx: self.base_level_pos[base_idx],
            uid: get_low_bits(uid, next_version),
        };
        if next_id.page_idx == 0 {
            // read a random path in the next level position map for obliviousness
            next_id.page_idx = self.rand_gen.gen();
        }
        let mut new_pos = self.rand_gen.gen();
        self.base_level_pos[base_idx] = new_pos;
        let mut remain_offsets = uid / self.base_level_pos.len();
        for (i, level) in self.ext_levels.iter_mut().enumerate() {
            let block_offset = remain_offsets % B;
            remain_offsets /= B;
            let mut next_pos = 0 as usize;

            let next_new_pos = self.rand_gen.gen();
            let updated_this_version = self.base_level_versions[i];
            let updated_next_version = self.ext_level_log_sizes[i + 1];
            let this_version = next_version;
            let updated_this_uid = get_low_bits(uid, updated_this_version);

            let update_func = |pos_block: Option<PosBlock<B>>, _| {
                let mut ret_block = if let Some(pos_block_unwrap) = pos_block {
                    next_pos = pos_block_unwrap.pos[block_offset];
                    next_version = pos_block_unwrap.versions[block_offset];
                    pos_block_unwrap
                } else {
                    next_pos = self.rand_gen.gen();
                    next_version = updated_next_version;
                    PosBlock::new()
                };
                ret_block.pos[block_offset] = next_new_pos;
                ret_block.versions[block_offset] = updated_next_version;
                (Some(ret_block), updated_this_uid)
            };
            level.update(&next_id, update_func, new_pos);
            if this_version != updated_this_version {
                // the version has changed, need to clone the entry
                let scale_factor = 1 << (updated_this_version - this_version);
                for j in 0..scale_factor {
                    let uid_to_write = get_low_bits(uid, this_version) * j;
                    if uid_to_write != updated_this_uid {}
                }
            }
            next_id.page_idx = next_pos;
            next_id.uid = get_low_bits(uid, next_version);
            new_pos = next_new_pos;
        }
        next_id.page_idx
    }

    pub fn double_size_and_fork_self(&mut self) {
        for level in self.ext_levels.iter_mut() {
            // level.double_size_and_fork_self();
        }
    }
}
