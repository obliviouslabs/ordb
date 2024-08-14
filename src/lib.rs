mod cuckoo;
mod dynamictree;
mod encvec;
mod fixoram;
mod flexomap;
mod flexoram;
mod linearoram;
mod pagefile;
mod params;
mod recoram;
mod segvec;
mod storage;
mod utils;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
