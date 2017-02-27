#![feature(test)]
extern crate test;

use std::sync::atomic;
use std::sync::atomic::Ordering;
use std::cell::UnsafeCell;
use std::ptr;


const KEY_SIZE: usize = 16;
const INITIAL_SIZE: usize = 128;
const DJB2_START: usize = 5318;


type HashKey = [u8; KEY_SIZE];


#[derive(Debug)]
struct HashSlot {
    key: atomic::AtomicPtr<HashKey>,
    count: atomic::AtomicUsize,
}


/// The interior threadsafe counter struct. Everything here is atomic or
/// composed of atomics.
struct InnerCounter {
    size: usize,
    used: atomic::AtomicUsize,
    slots: Vec<HashSlot>,
}

/// The public Counter structure which contains the inner mutable state. This
/// allows us to put this straight into an Arc and safely share between threads
pub struct Counter {
    inner: UnsafeCell<InnerCounter>
}

pub struct CounterIterator<'a> {
    counter: &'a Counter,
    index: usize,
    size: usize,
}


/// An implementation of the djb2 hash using rust iterators over the contents
/// of the HashKey array.
pub fn djb2_hash(key: &HashKey) -> usize {
    key.into_iter().take_while(|c| { **c != 0 }).fold(DJB2_START, |hash, c| { (hash * 33) ^ (*c as usize) })
}

/// A small inlined helper function that checks if a given character is ascii
fn is_ascii(c: &u8) -> bool {
    return (*c > 47 && *c < 58) || (*c > 64 && *c < 91) || (*c > 96 && *c < 123);
}

/// Clean a given key string. This involves stripping non ascii characters out
/// and limiting the key to 16 characters. The resulting key is stack allocated
/// so be careful with it. If you need it long term put it in a Box.
pub fn clean_key(key: &str) -> HashKey {
    let mut cleaned: HashKey = [0; KEY_SIZE]; // Stack allocate a temporary key.
    for (index, v) in key.bytes().take(KEY_SIZE).filter(is_ascii).enumerate() {
        cleaned[index] = v;
    }
    return cleaned;
}

impl<'a> IntoIterator for &'a Counter {
    type Item = (String, usize);
    type IntoIter = CounterIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        CounterIterator { counter: self, index: 0 , size: self.size() }
    }
}

impl<'a> Iterator for CounterIterator<'a> {
    type Item = (String, usize);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.size {
                match self.counter.get_index(self.index) {
                    Some((key, val)) => {
                        self.index += 1;
                        return Some((key, val))
                    },
                    None => self.index += 1,
                }
            } else {
                return None;
            }
        }
    }
}

impl HashSlot {
    /// Create a new HashSlot struct with the default values. The key is
    /// initially set to null so get/incr know this slot is empty.
    pub fn new() -> HashSlot {
        HashSlot {
            key: atomic::AtomicPtr::new(ptr::null_mut()),
            count: atomic::AtomicUsize::new(0),
        }
    }
}

impl InnerCounter {
    /// Create a new InnerCounter. Sets up all the top level atomic values for
    /// keeping track of global hashmap state.
    pub fn new() -> InnerCounter {
        let mut slots: Vec<HashSlot> = Vec::with_capacity(INITIAL_SIZE);
        for _ in 0..INITIAL_SIZE {
            slots.push(HashSlot::new());
        }
        let counter: InnerCounter = InnerCounter {
            size: INITIAL_SIZE,
            used: atomic::AtomicUsize::new(0),
            slots: slots,
        };
        return counter;
    }

    pub unsafe fn unsafe_get_index(&self, index: usize) -> Option<(HashKey, usize)> {
        if index > 0 && index < self.size {
            let slot = &self.slots[index];
            let key = slot.key.load(Ordering::Relaxed);
            if key.is_null() {
                return None;
            } else {
                let value = slot.count.load(Ordering::Relaxed);
                let key_copy: *mut HashKey = &mut [0; KEY_SIZE];
                ptr::copy(key, key_copy, 1);
                return Some((*key_copy, value));
            }
        } else {
            return None;
        }
    }

    /// Unsafe method for returning the value of a counter.
    pub unsafe fn unsafe_get(&self, key: HashKey) -> usize {
        let size = self.size;
        let mut index = djb2_hash(&key) % size;
        loop {
            let slot = &self.slots[index];
            let other_key = slot.key.load(Ordering::Relaxed);
            if other_key.is_null() {
                return 0;
            }
            else if *other_key == key{
                return slot.count.load(Ordering::Relaxed);
            }
            index = (index + 1) % size;
        }
    }

    /// Unsafe method for incrementing a counter by the given amount. Returns
    /// the previous value of the counter.
    pub unsafe fn unsafe_incr(&mut self, key: HashKey, count: usize) -> usize {
        let size = self.size;
        let mut index = djb2_hash(&key) % size;
        let mut slot: &HashSlot;
        loop {
            slot = &self.slots[index];
            let other_key = slot.key.load(Ordering::Relaxed);
            if other_key.is_null() {
                let boxed_key = Box::new(key);
                let res = slot.key.compare_exchange(other_key,
                                                    Box::into_raw(boxed_key),
                                                    Ordering::Relaxed,
                                                    Ordering::Relaxed);
                let ptr = res.unwrap_or_else(|v| { v });
                if ptr.is_null() || *ptr == key {
                    self.used.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            else if *other_key == key {
                break;
            }
            index = (index + 1) % size
        }
        return slot.count.fetch_add(count, Ordering::Relaxed);
    }
}

impl Drop for InnerCounter {
    /// Custom drop implementation to go through and explicitly re-box all the
    /// heap memory used to store keys within the Vec<HashSlot>. During the
    /// first call to unsafe_incr() the key is copied into a Box so it will
    /// persist.
    fn drop(&mut self) {
        for hs in &self.slots {
            let key = hs.key.load(Ordering::Relaxed);
            if !key.is_null() {
                let _: Box<HashKey> = unsafe { Box::from_raw(key) };
            }
        }
    }
}

unsafe impl Sync for Counter {}
unsafe impl Send for Counter {}

impl Counter {
    /// Create a new counter, which is has an UnsafeCell wrapping the actual
    /// counter implementation.
    pub fn new() -> Counter {
        Counter {
            inner: UnsafeCell::new(InnerCounter::new())
        }
    }

    /// Extract the mutable reference to the counter implementation and
    /// increment the given key.
    pub fn incr(&self, key: &str, count: usize) -> usize {
        return unsafe { 
            (*self.inner.get()).unsafe_incr(clean_key(key), count)
        };
    }

    /// Extract the mutable reference to the counter implementation and
    /// return the current counter value for the given key.
    pub fn get(&self, key: &str) -> usize {
        return unsafe {
            (*self.inner.get()).unsafe_get(clean_key(key))
        };
    }

    pub fn get_index(&self, index: usize) -> Option<(String, usize)> {
        return unsafe {
            (*self.inner.get())
                .unsafe_get_index(index)
                .map(|(hk, c)| (std::str::from_utf8(&hk).unwrap().to_owned(), c))
        };
    }

    pub fn size(&self) -> usize {
        return unsafe {
            (*self.inner.get()).size
        };
    }
}



#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::Arc;
    use test::Bencher;
    use super::*;



    #[test]
    fn increment() {
        let counter = Counter::new();
        let res = counter.incr("foo", 1);
        assert_eq!(res, 0);
    }

    #[test]
    fn sequential_increment() {
        let counter = Counter::new();
        for _ in 0..100000 {
            counter.incr("foo", 1);
        }
        assert_eq!(counter.get("foo"), 100000);
    }

    #[test]
    fn sequential_iter() {
        let counter = Counter::new();
        for i in 0..20 {
            let key = format!("key_{0}", i);
            counter.incr(&key, 1);
        }
        for (key, val) in (&counter).into_iter() {
            println!("k: {}, v: {}", key, val)
        }
    }

    #[test]
    fn concurrent_iter() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let nthreads = 8;
        let c = shared.clone();
        for i in 0..20 {
            let key = format!("key_{0}", i);
            c.incr(&key, 1);
        }
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::spawn(move|| {
                for (key, val) in (&c).into_iter() {
                    assert_eq!(val, 1);
                }
            }));
        }
        for t in children {
            let _ = t.join();
        }
    }

    #[test]
    fn concurrent_iter_and_incr() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let mut children = vec![];
        let c1 = shared.clone();
        children.push(thread::spawn(move|| {
            for _ in 0..200 {
                for (key, val) in (&c1).into_iter() {
                    assert_eq!(key, "key_1")
                }
            }
        }));
        let c2 = shared.clone();
        children.push(thread::spawn(move|| {
            for _ in 0..10000 {
                c2.incr("key_1", 1);
            }
        }));

        for t in children {
            let _ = t.join();
        }
    }

    #[test]
    fn concurrent_increment() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let nthreads = 8;
        let nincr = 10000;
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::spawn(move|| {
                for _ in 0..nincr {
                    c.incr("foo", 1);
                }
            }));
        }
        for t in children {
            let _ = t.join();
        }
        assert_eq!(shared.get("foo"), nincr * nthreads);
    }

    #[test]
    fn concurrent_multi_key_increment() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let nthreads = 8;
        let nincr = 10000;
        let mut children = vec![];
        for i in 0..nthreads {
            let c = shared.clone();
            let key = format!("thread_{}", i);
            children.push(thread::spawn(move|| {
                for _ in 0..nincr {
                    c.incr(&key, 1);
                }
            }));
        }
        for t in children {
            let _ = t.join();
        }
        for i in 0..nthreads {
            let key = format!("thread_{}", i);
            assert_eq!(shared.get(&key), nincr);
        }
    }

    #[test]
    fn get() {
        let counter = Counter::new();
        let _ = counter.incr("foo", 1);
        let count = counter.get("foo");
        assert_eq!(count, 1);
    }

    #[bench]
    fn bench_clean_key(b: &mut Bencher) {
        b.iter(|| clean_key("foobar"));
    }

    #[bench]
    fn bench_djb2_hash(b: &mut Bencher) {
        let key = clean_key("foobar");
        b.iter(|| djb2_hash(&key));
    }

    #[bench]
    fn bench_incr(b: &mut Bencher) {
        let counter = Counter::new();
        b.iter(|| counter.incr("foo", 1))
    }

    #[bench]
    fn bench_existing_incr(b: &mut Bencher) {
        let counter = Counter::new();
        counter.incr("foo", 1);
        b.iter(|| counter.incr("foo", 1));
    }

    #[bench]
    fn bench_get(b: &mut Bencher) {
        let counter = Counter::new();
        counter.incr("foo", 100);
        b.iter(|| counter.get("foo"))
    }

    #[bench]
    fn bench_into_iter(b: &mut Bencher) {
        let counter = Counter::new();
        let key = format!("key_{0}", 1);
        counter.incr(&key, 1);
        b.iter(|| (&counter).into_iter())
    }

    #[bench]
    fn bench_iter(b: &mut Bencher) {
        let counter = Counter::new();
        let key = format!("key_{0}", 1);
        counter.incr(&key, 1);
        b.iter(|| {
            for (key, val) in counter.into_iter() {
                drop(key);
                drop(val);
            }
        })

    }


    #[bench]
    fn bench_arc_incr(b: &mut Bencher) {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let clone = shared.clone();
        b.iter(|| clone.incr("foo", 1))
    }

    #[bench]
    fn bench_arc_get(b: &mut Bencher) {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let clone = shared.clone();
        clone.incr("foo", 1);
        b.iter(|| clone.get("foo"))
    }
}
