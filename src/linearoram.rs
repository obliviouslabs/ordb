use crate::utils::SimpleVal;
use bytemuck::{Pod, Zeroable};
pub struct LinearOram<T: SimpleVal, const N: usize> {
    val: Vec<T>,
}

impl<T: SimpleVal, const N: usize> LinearOram<T, N> {
    pub fn new(size: usize) -> Self {
        Self {
            val: vec![T::zeroed(); size],
        }
    }

    pub fn update<F>(&mut self, uid: usize, update_func: F)
    where
        F: FnOnce(Option<T>) -> Option<T>,
    {
        self.val[uid] = update_func(Some(self.val[uid])).unwrap();
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
        self.val.extend_from_within(0..self.size());
    }

    pub fn size(&self) -> usize {
        self.val.len()
    }

    pub fn print_state(&self) {
        println!("LinearOram state:");
        for i in 0..self.size() {
            println!("{}: {:?}", i, self.val[i]);
        }
    }
}

mod tests {
    use super::*;
    use bincode::de::read;
    use rand::random;
    #[test]
    fn test_rec_oram_simple() {
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(4);
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
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(size);
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
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(4);
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
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(4);
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
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(4);
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
        let mut rec_oram: LinearOram<u32, 4> = LinearOram::new(size);
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
