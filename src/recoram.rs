use crate::fixoram::{BlockId, FixOram};
use crate::utils::{get_low_bits, RandGen, SimpleVal};
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PosBlock<const B: usize> {
    pos: [usize; B],
    versions: [u8; B], // the current version of the position in the next map
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

struct RecOramPosMap<const N: usize, const B: usize> {
    base_level_pos: Vec<usize>,
    base_level_versions: Vec<u8>,
    ext_levels: Vec<FixOram<PosBlock<B>, N>>,
    base_level_log_size: u8,
    ext_level_log_sizes: Vec<u8>,
    rand_gen: RandGen,
}

impl<const N: usize, const B: usize> RecOramPosMap<N, B> {
    pub fn new(size: usize) -> Self {
        Self {
            base_level_pos: vec![0; size],
            base_level_versions: vec![size.trailing_zeros() as u8; size],
            ext_levels: Vec::new(),
            base_level_log_size: size.trailing_zeros() as u8,
            ext_level_log_sizes: Vec::new(),
            rand_gen: RandGen::new(),
        }
    }

    /**
     * Get the position for the given uid and set the new positions for all the relevant uninitialized uids in the map.
     */
    pub fn get_and_set_new_positions(&mut self, uid: usize) -> (usize, u8, Vec<usize>) {
        let base_idx = uid;
        let version = self.base_level_versions[base_idx];
        let scaling_factor = 1 << (self.base_level_log_size - version);
        let actual_base_idx = get_low_bits(base_idx, version);
        let pos = self.base_level_pos[actual_base_idx];
        let mut new_positions = vec![0; scaling_factor];
        for i in 0..scaling_factor {
            new_positions[i] = self.rand_gen.gen();
            let idx = actual_base_idx + (i << version);
            self.base_level_pos[idx] = new_positions[i];
            self.base_level_versions[idx] = self.base_level_log_size;
        }
        (pos, version, new_positions)
    }

    pub fn double_size_and_fork_self(&mut self) {
        self.base_level_pos.resize(self.base_level_pos.len() * 2, 0);
        self.base_level_versions
            .extend_from_within(0..self.base_level_versions.len());
        self.base_level_log_size += 1;
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

    pub fn get_rand_new_pos(&mut self) -> usize {
        self.rand_gen.gen()
    }

    pub fn size(&self) -> usize {
        self.base_level_pos.len()
    }

    pub fn print_state(&self) {
        println!("RecOramPosMap state:");
        println!("base_level_pos: {:?}", self.base_level_pos);
        println!("base_level_versions: {:?}", self.base_level_versions);
    }
}

pub struct RecOram<T: SimpleVal, const N: usize> {
    pos_map: RecOramPosMap<16, 16>,
    val_ram: FixOram<T, N>,
}

impl<T: SimpleVal, const N: usize> RecOram<T, N> {
    pub fn new(size: usize) -> Self {
        Self {
            pos_map: RecOramPosMap::new(size),
            val_ram: FixOram::new(),
        }
    }

    pub fn update<F>(&mut self, uid: usize, update_func: F)
    where
        F: FnOnce(Option<T>) -> Option<T>,
    {
        let (page_idx, version, new_positions) = self.pos_map.get_and_set_new_positions(uid);

        let val_ram_update_func = |val: Option<T>, id: usize| {
            let old_val_unwrap = val.unwrap_or(T::zeroed());
            let new_val = update_func(val);
            let mut ret = Vec::new();
            ret.reserve(new_positions.len());
            for (i, new_pos) in new_positions.iter().enumerate() {
                let uid_to_write = id + (i << version);
                if uid_to_write == uid {
                    if new_val.is_some() {
                        ret.push((new_val.unwrap(), uid_to_write, *new_pos));
                    }
                } else {
                    ret.push((old_val_unwrap, uid_to_write, *new_pos));
                }
            }
            ret
        };
        let base_uid = get_low_bits(uid, version);
        self.val_ram.update_and_write_multiple(
            &BlockId {
                page_idx,
                uid: base_uid,
            },
            val_ram_update_func,
        );
    }

    pub fn read(&mut self, uid: usize) -> Option<T> {
        let mut ret = None;
        let update_func = |val: Option<T>| {
            ret = val;
            val
        };
        self.update(uid, update_func);
        ret
    }

    pub fn write(&mut self, uid: usize, val: T) {
        let update_func = |_: Option<T>| Some(val);
        self.update(uid, update_func);
    }

    pub fn double_size_and_fork_self(&mut self) {
        self.pos_map.double_size_and_fork_self();
    }

    pub fn size(&self) -> usize {
        self.pos_map.size()
    }

    pub fn print_state(&self) {
        println!("RecOram state:");
        self.pos_map.print_state();
        self.val_ram.print_state();
    }
}

mod tests {
    use super::*;
    use bincode::de::read;
    use rand::random;
    #[test]
    fn test_rec_oram_simple() {
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(4);
        rec_oram.write(0, 1);
        rec_oram.write(1, 2);
        rec_oram.write(2, 3);
        rec_oram.write(3, 4);
        rec_oram.print_state();
        assert_eq!(rec_oram.read(0), Some(1));
        assert_eq!(rec_oram.read(1), Some(2));
        assert_eq!(rec_oram.read(2), Some(3));
        assert_eq!(rec_oram.read(3), Some(4));
    }

    #[test]
    fn test_rec_oram_rand() {
        let size = 128;
        let mut ref_ram = vec![0; size];
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(size);
        for _ in 0..1000 {
            let write_uid = random::<usize>() % size;
            let val = random::<u32>();
            ref_ram[write_uid] = val;
            rec_oram.write(write_uid, val);
            let read_uid = random::<usize>() % size;
            let read_res = rec_oram.read(read_uid);
            let real_val = read_res.unwrap_or(0);
            assert_eq!(real_val, ref_ram[read_uid]);
        }
        for i in 0..size {
            let read_res = rec_oram.read(i);
            let real_val = read_res.unwrap_or(0);
            assert_eq!(real_val, ref_ram[i]);
        }
    }

    #[test]
    fn test_rec_oram_scale_simple() {
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(4);
        rec_oram.write(0, 1);
        rec_oram.write(1, 2);
        rec_oram.write(2, 3);
        rec_oram.write(3, 4);
        rec_oram.double_size_and_fork_self();
        assert_eq!(rec_oram.read(0), Some(1));
        assert_eq!(rec_oram.read(1), Some(2));
        assert_eq!(rec_oram.read(2), Some(3));
        assert_eq!(rec_oram.read(3), Some(4));
        assert_eq!(rec_oram.read(4), Some(1));
        assert_eq!(rec_oram.read(5), Some(2));
        assert_eq!(rec_oram.read(6), Some(3));
        assert_eq!(rec_oram.read(7), Some(4));
    }

    #[test]
    fn test_rec_oram_scale_simple2() {
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(4);
        rec_oram.write(0, 1);
        rec_oram.write(1, 2);
        rec_oram.write(2, 3);
        rec_oram.write(3, 4);
        rec_oram.double_size_and_fork_self();
        rec_oram.write(3, 5);
        rec_oram.write(6, 8);
        assert_eq!(rec_oram.read(0), Some(1));
        assert_eq!(rec_oram.read(1), Some(2));
        assert_eq!(rec_oram.read(2), Some(3));
        assert_eq!(rec_oram.read(3), Some(5));
        assert_eq!(rec_oram.read(4), Some(1));
        assert_eq!(rec_oram.read(5), Some(2));
        assert_eq!(rec_oram.read(6), Some(8));
        assert_eq!(rec_oram.read(7), Some(4));
    }

    #[test]
    fn test_rec_oram_scale_repeat() {
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(4);
        rec_oram.write(0, 1);
        rec_oram.write(1, 2);
        rec_oram.write(2, 3);
        rec_oram.write(3, 4);
        rec_oram.double_size_and_fork_self();
        rec_oram.write(3, 5);
        rec_oram.write(6, 8);
        rec_oram.double_size_and_fork_self();
        rec_oram.write(1, 6);
        rec_oram.write(6, 7);
        rec_oram.write(14, 4);
        assert_eq!(rec_oram.read(0), Some(1));
        assert_eq!(rec_oram.read(1), Some(6));
        assert_eq!(rec_oram.read(2), Some(3));
        assert_eq!(rec_oram.read(3), Some(5));
        assert_eq!(rec_oram.read(4), Some(1));
        assert_eq!(rec_oram.read(5), Some(2));
        assert_eq!(rec_oram.read(6), Some(7));
        assert_eq!(rec_oram.read(7), Some(4));
        assert_eq!(rec_oram.read(8), Some(1));
        assert_eq!(rec_oram.read(9), Some(2));
        assert_eq!(rec_oram.read(10), Some(3));
        assert_eq!(rec_oram.read(11), Some(5));
        assert_eq!(rec_oram.read(12), Some(1));
        assert_eq!(rec_oram.read(13), Some(2));
        assert_eq!(rec_oram.read(14), Some(4));
        assert_eq!(rec_oram.read(15), Some(4));
    }

    #[test]
    fn test_rec_oram_rand_scale() {
        let mut size = 128;
        let mut ref_ram = vec![0; size];
        let mut rec_oram: RecOram<u32, 4> = RecOram::new(size);
        let round = 10000;
        for i in 0..round {
            if i % 2000 == 0 {
                rec_oram.double_size_and_fork_self();
                ref_ram.extend_from_within(0..size);
                size *= 2;
            }
            let write_uid = random::<usize>() % size;
            let val = random::<u32>();
            ref_ram[write_uid] = val;
            rec_oram.write(write_uid, val);
            let read_uid = random::<usize>() % size;
            let read_res = rec_oram.read(read_uid);
            let real_val = read_res.unwrap_or(0);
            assert_eq!(real_val, ref_ram[read_uid]);
        }
        for i in 0..size {
            let read_res = rec_oram.read(i);
            let real_val = read_res.unwrap_or(0);
            assert_eq!(real_val, ref_ram[i]);
        }
    }
}
