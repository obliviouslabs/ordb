use rand::distributions::{Distribution, Standard};
use rand::rngs::OsRng;
use rand::Rng;

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
