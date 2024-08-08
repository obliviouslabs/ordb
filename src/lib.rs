mod adaptiveomap;
mod cuckoo;
mod dynamictree;
mod encvec;
mod pageomap;
mod pageoram;
mod params;
mod segvec;
mod structuredoram;

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
