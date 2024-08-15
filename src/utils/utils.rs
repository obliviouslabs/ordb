use bytemuck::{Pod, Zeroable};
use rand::distributions::{Distribution, Standard};
use rand::rngs::OsRng;
use rand::Rng;
use std::fmt::Debug;

// Define a struct that wraps the RNG
pub struct RandGen {
    rng: OsRng,
}

impl RandGen {
    pub fn new() -> Self {
        RandGen {
            rng: OsRng::default(),
        }
    }

    // Generic method to generate random values of any type T
    pub fn gen<T>(&mut self) -> T
    where
        Standard: Distribution<T>,
    {
        self.rng.gen::<T>()
    }
}

pub fn get_low_bits(value: usize, num_bits: u8) -> usize {
    value & ((1 << num_bits) - 1)
}

pub trait SimpleVal:
    Clone + Copy + Pod + Zeroable + PartialEq + Eq + std::marker::Send + std::marker::Sync
{
}
impl<T> SimpleVal for T where
    T: Clone + Copy + Pod + Zeroable + PartialEq + Eq + std::marker::Send + std::marker::Sync
{
}
