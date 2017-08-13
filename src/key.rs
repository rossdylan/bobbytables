use std::ascii::AsciiExt;

const KEY_SIZE: usize = 16;
const DJB2_START: usize = 5318;

pub type HashKey = [u8; KEY_SIZE];

/// Clean a given key string. This involves stripping non ascii characters out
/// and limiting the key to 16 characters. The resulting key is stack allocated
/// so be careful with it. If you need it long term put it in a Box.
pub fn clean_key(key: &str) -> HashKey {
    let mut cleaned: HashKey = [0; KEY_SIZE]; // Stack allocate a temporary key.
    for (index, v) in key.bytes().filter(|c|{c.is_ascii()}).take(KEY_SIZE).enumerate() {
        cleaned[index] = v;
    }
    cleaned
}

/// An implementation of the djb2 hash using rust iterators over the contents
/// of the `HashKey` array.
pub fn djb2_hash(key: &HashKey) -> usize {
    key.into_iter().take_while(|c| { **c as usize != 0 }).fold(DJB2_START, |hash, c| { (hash * 33) ^ (*c as usize) })
}

pub fn hashkey_to_string(key: &HashKey) -> String {
    let mut nk = String::new();
    for c in key.into_iter().take_while(|c| { **c != 0 }){
        nk.push(*c as char);
    }
    return nk;
}


#[cfg(test)]
mod tests {
    use test::Bencher;
    use super::*;

    #[bench]
    fn bench_clean_key(b: &mut Bencher) {
        b.iter(|| clean_key("foobar"));
    }

    #[bench]
    fn bench_djb2_hash(b: &mut Bencher) {
        let key = clean_key("foobar");
        b.iter(|| djb2_hash(&key));
    }

    #[test]
    fn test_hk_to_string() {
        let key = clean_key("foobar");
        assert_eq!(hashkey_to_string(&key), format!("foobar"))
    }

    #[bench]
    fn bench_hashkey_to_string(b: &mut Bencher) {
        let key = clean_key("foobar");
        b.iter(|| hashkey_to_string(&key));
    }
}
