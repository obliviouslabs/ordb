mod adaptiveomap;
mod cuckoo;
mod dynamictree;
mod encvec;
mod fixedoram;
mod flexomap;
mod flexoram;
mod params;
mod segvec;

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
