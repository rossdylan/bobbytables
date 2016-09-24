#![feature(test)]
extern crate test;


use std::sync::atomic;
use std::sync::atomic::Ordering;
use std::ptr;


const KEYSZ: usize = 16;
const INITIAL_SIZE: usize = 128;


type RawKey = [u8; 16];
type HashKey = Box<RawKey>;

#[derive(Debug)]
struct HashSlot {
    key: atomic::AtomicPtr<RawKey>,
    count: atomic::AtomicUsize,
}

impl HashSlot {
    pub fn new() -> HashSlot {
        HashSlot {
            key: atomic::AtomicPtr::new(ptr::null_mut()),
            count: atomic::AtomicUsize::new(0),
        }
    }
}


pub struct Counter {
    size: usize,
    used: atomic::AtomicUsize,
    slots: Vec<HashSlot>,
    prev: atomic::AtomicPtr<Counter>,
    next: atomic:: AtomicPtr<Counter>,
}

fn djb2_hash(key: &HashKey) -> usize {
    let start: usize = 5381;
    key.into_iter().take_while(|c| { **c != 0 }).fold(start, |hash, c| {
        (hash * 33) ^ (*c as usize)})
}

#[inline]
fn is_ascii(c: &u8) -> bool {
    return (*c > 47 && *c < 58) || (*c > 64 && *c < 91) || (*c > 96 && *c < 123);
}

fn clean_key(key: &str) -> HashKey {
    let mut cleaned: HashKey = Box::new([0; 16]);
    for (index, v) in key.bytes().take(16).filter(is_ascii).enumerate() {
        cleaned[index] = v;
    }
    return cleaned;
}

impl Counter {
    pub fn new() -> Counter {
        let mut slots: Vec<HashSlot> = Vec::with_capacity(INITIAL_SIZE);
        for _ in 0..INITIAL_SIZE {
            slots.push(HashSlot::new());
        }
        let counter: Counter = Counter {
            size: INITIAL_SIZE,
            used: atomic::AtomicUsize::new(0),
            slots: slots,
            prev: atomic::AtomicPtr::new(ptr::null_mut()),
            next: atomic::AtomicPtr::new(ptr::null_mut()),
        };
        return counter;
    }

    unsafe fn print_slot(&self, index: usize) {
        let slot = &self.slots[index];
        let key = slot.key.load(Ordering::Relaxed);
        println!("slot {}: {:?}", index, slot);
        if key.is_null() {
            println!("\t key=null")
        }
        else {
            println!("\t key={:?}", *key);
        }
    }

    unsafe fn unsafe_get(&self, key: HashKey) -> usize {
        let size = self.size;
        let mut index = djb2_hash(&key) % size;
        let raw_key = Box::into_raw(key);
        loop {
            let slot = &self.slots[index];
            let other_key = slot.key.load(Ordering::Relaxed);
            if other_key.is_null() {
                return 0;
            }
            else if *other_key == *raw_key{
                return slot.count.load(Ordering::Relaxed);
            }
            index = (index + 1) % size;
        }
    }

    unsafe fn unsafe_incr(&mut self, mut key: HashKey, count: usize) -> usize {
        let size = self.size;
        let mut index = djb2_hash(&key) % size;
        let raw_key = Box::into_raw(key);
        loop {
            let slot = &self.slots[index];
            let other_key = slot.key.load(Ordering::Relaxed);
            if other_key.is_null() {
                let res = slot.key.compare_exchange(other_key,
                                                    raw_key,
                                                    Ordering::Relaxed,
                                                    Ordering::Relaxed);
                let status = match res {
                    Ok(v) => v.is_null() || *v == *raw_key,
                    Err(v) => v.is_null() || *v == *raw_key,
                };
                if status {
                    self.used.fetch_add(1, Ordering::Relaxed);
                    //TODO(rossdylan) handle resizing
                    let added = slot.count.fetch_add(count, Ordering::Relaxed);
                    return added
                }
            }
            else if *other_key == *raw_key {
                return slot.count.fetch_add(count, Ordering::Relaxed);
            }
            index = (index + 1) % size
        }
    }

    pub fn incr(&mut self, key: &str, count: usize) -> usize {
        return unsafe { self.unsafe_incr(clean_key(key), count) };
    }

    pub fn get(&self, key: &str) -> usize {
        return unsafe { self.unsafe_get(clean_key(key)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[test]
    fn increment() {
        let mut counter = Counter::new();
        let res = counter.incr("foo", 1);
        assert_eq!(res, 0);
    }

    #[test]
    fn get() {
        let mut counter = Counter::new();
        let res = counter.incr("foo", 1);
        let count = counter.get("foo");
        assert_eq!(count, 1);
    }

    #[bench]
    fn bench_incr(b: &mut Bencher) {
        let mut counter = Counter::new();
        b.iter(|| counter.incr("foo", 1))
    }

    #[bench]
    fn bench_get(b: &mut Bencher) {
        let mut counter = Counter::new();
        counter.incr("foo", 100);
        b.iter(|| counter.get("foo"))
    }
}
