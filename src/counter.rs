use std::cell::RefCell;
use std::ptr;
use std::sync::atomic;
use std::sync::atomic::Ordering;

use ::key::{clean_key, HashKey};
use ::state::{AtomicState, ResizeState};
use ::table::VectorTable;

const INITIAL_SIZE: usize = 128;
const MAX_LOAD_FACTOR: f64 = 0.70;
const COPY_QUOTA: f64 = 2.0;

thread_local!(static LAST_INDEX: RefCell<usize> = RefCell::new(0));

struct Counter{
    state: AtomicState<ResizeState>,
    current: atomic::AtomicPtr<VectorTable>,
    previous: atomic::AtomicPtr<VectorTable>,
    copied: atomic::AtomicUsize,
    active_writers: atomic::AtomicUsize,
}

impl Counter {
    fn new() -> Counter {
        let vec_table = Box::new(VectorTable::new(INITIAL_SIZE));
        Counter{
            state: AtomicState::default(),
            current: atomic::AtomicPtr::new(Box::into_raw(vec_table)),
            previous: atomic::AtomicPtr::default(),
            copied: atomic::AtomicUsize::new(0),
            active_writers: atomic::AtomicUsize::new(0),
        }
    }

    fn resize_finished(&self) -> bool {
        let slots = self.previous.load(Ordering::Acquire);
        if !slots.is_null() {
            let used = unsafe { (*slots).used() };
            used == self.copied.load(Ordering::Relaxed)
        } else {
            false
        }
    }

    /// Calculate the current slot utilization of our current table.
    fn resize_needed(&self) -> bool {
        let slots = self.current.load(Ordering::Acquire);
        let used = unsafe { (*slots).used() };
        let cap = unsafe { (*slots).cap() };
        (used as f64 / cap as f64 ) >= MAX_LOAD_FACTOR
    }

    /// Try and acquire (CAS) exclusive access to allocate the new vector.
    /// Actually allocate it and store it in self.current
    fn allocate_new_table(&self) -> bool {
        let res = self.state.set_cas(ResizeState::Complete, ResizeState::Allocating);
        if res {
            let table = self.current.load(Ordering::Acquire);
            let next_cap = unsafe {
                let fval = (*table).cap() as f64 * (((COPY_QUOTA+1.0)/COPY_QUOTA) + 1.0);
                fval.ceil() as usize
            };
            let vec_table = Box::new(VectorTable::new(next_cap));

            while self.active_writers.load(Ordering::Relaxed) > 0 { }

            self.current.store(Box::into_raw(vec_table), Ordering::Release);
            self.previous.store(table, Ordering::Release);
            self.state.set(ResizeState::Copying);
            return true
        }
        return false
    }

    /// Attempt to copy a single slot based on the given key to the new table.
    fn copy_current_key(&self, key: HashKey) {
        let table = self.current.load(Ordering::Acquire);
        let prev_table = self.previous.load(Ordering::Acquire);
        if prev_table.is_null() {
            return
        }
        unsafe {
            let res = (*prev_table).copy_key_to(key, &(*table));
            if res {
                self.copied.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Move through the entirety of the previous table copying at least `COPY_QUOTA`
    /// slots. Once the quota is hit, record the index in a thread local and exit.
    fn copy_some_slots(&self) {
        let table = self.current.load(Ordering::Acquire);
        let prev_table = self.previous.load(Ordering::Acquire);
        if prev_table.is_null() {
            return
        }
        LAST_INDEX.with(|li| {
            let mut val = li.borrow_mut();
            let mut moved = 0;
            unsafe {
                for index in *val..((*prev_table).cap()) {
                    if (*prev_table).copy_index_to(index, &(*table)) {
                        moved += 1;
                    }
                    if moved as f64 >= COPY_QUOTA {
                        *val = index;
                        break
                    }
                }
            }
            self.copied.fetch_add(moved, Ordering::Relaxed);
        });
    }

    fn deallocate_old_table(&self) {
        let res = self.state.set_cas(ResizeState::Copying, ResizeState::Deallocating);
        if res {
            let table = self.previous.load(Ordering::Acquire);
            let _: Box<VectorTable> = unsafe { Box::from_raw(table) };
            self.previous.store(ptr::null_mut(), Ordering::Release);
            self.copied.store(0, Ordering::Relaxed);
            self.state.set(ResizeState::Complete);
        }
    }

    /// This bad boy drives the resize state machine.
    /// Step 1: Check to see if we need a resize by looking at current util
    /// Step 2: Try and CAS state to `ResizeState::Allocating` so we can make our new table.
    /// Step 3: Allocate a new table and prepare other resize state.
    /// Step 5: Move to `ResizeState::Copying`
    /// Step 6: Try and copy the provided key to the new table
    /// Step 7: Check for completetion
    /// Step 8: Attempt to CAS to `ResizeState::Deallocating`
    /// Step 9: Deallocate previous
    /// Step 10: CAS to `ResizeState::Complete`
    fn drive_resize(&self, key: HashKey) {
        match self.state.get() {
            ResizeState::Complete => { // Check if we need to resize, attempt to start if so
                // XXX: This sucks, but idk how to reset the tl after a copy.
                LAST_INDEX.with(|li| {
                    let mut val = li.borrow_mut();
                    *val = 0;
                });

                if self.resize_needed() {
                    if !self.allocate_new_table() {
                        loop {
                            match self.state.get() {
                                ResizeState::Complete | ResizeState::Allocating => continue,
                                _ => break,
                            }
                        }
                    }
                    self.copy_current_key(key);
                    self.copy_some_slots();
                    if self.resize_finished() {
                        self.deallocate_old_table();
                    }
                }
            },
            ResizeState::Allocating => loop {
                match self.state.get() {
                    ResizeState::Allocating => continue,
                    _ => break,
                }
            },
            ResizeState::Copying => { // perform actual copying logic and check if we can deallocate
                self.copy_current_key(key);
                self.copy_some_slots();
                if self.resize_finished() {
                    self.deallocate_old_table();
                }
                return
            },
            ResizeState::Deallocating => return, //no-op all operatations go to the current table
        }
    }

    pub fn cap(&self) -> usize {
        let table = self.current.load(Ordering::Acquire);
        unsafe { (*table).cap() }
    }

    pub fn used(&self) -> usize {
        let table = self.current.load(Ordering::Acquire);
        unsafe { (*table).used() }
    }

    pub fn get(&self, key: &str) -> Option<usize> {
        let hk = clean_key(key);
        let table = self.current.load(Ordering::Acquire);
        match self.state.get() {
            ResizeState::Copying => self.copy_current_key(hk),
            _ => {},
        }
        unsafe{ (*table).get(hk) }
    }

    pub fn incr(&self, key: &str, val: usize) -> usize {
        let hk = clean_key(key);
        self.drive_resize(hk);
        self.active_writers.fetch_add(1, Ordering::Relaxed);
        let table = self.current.load(Ordering::Acquire);
        unsafe {
            let val = (*table).incr(hk, val);
            self.active_writers.fetch_sub(1, Ordering::Relaxed);
            val
        }

    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::Arc;
    use test::Bencher;
    use rand;
    use rand::Rng;
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
        assert_eq!(counter.get("foo").unwrap(), 100000);
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
        assert_eq!(shared.get("foo").unwrap(), nincr * nthreads);
    }

    #[test]
    fn resize() {
        let counter = Counter::new();
        for i in 0..200 {
            let key = format!("foo{}", i);
            counter.incr(&key, 1);
        }

        for i in 0..200 {
            let key = format!("foo{}", i);
            assert_eq!(counter.get(&key).unwrap(), 1);
        }
    }

    #[test]
    fn concurrent_resize() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let nthreads = 8;
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::spawn(move|| {
                for i in 0..90 {
                    let key = format!("foo{}", i);
                    c.incr(&key, 1);
                }
            }));
        }
        for t in children {
            let _ = t.join();
        }
        for i in 0..90 {
            let key = format!("foo{}", i);
            assert_eq!(shared.get(&key).unwrap(), nthreads);
        }
    }

    #[test]
    fn huge_concurrent_resize() {
        let counter = Counter::new();
        let shared = Arc::new(counter);
        let nthreads = 8;
        let nkeys = 100000;
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::spawn(move|| {
                let mut rng = rand::thread_rng();
                let mut keys: Vec<usize> = (0..nkeys).collect();
                rng.shuffle(keys.as_mut_slice());
                for i in keys {
                    let key = format!("foo{}", i);
                    c.incr(&key, 1);
                }
            }));
        }
        for t in children {
            let _ = t.join();
        }
        for i in 0..100000 {
            let key = format!("foo{}", i);
            assert_eq!(shared.get(&key).unwrap(), nthreads);
        }
    }

    #[test]
    fn huge_table() {
        let counter = Counter::new();
        for i in 0..100000 {
            let key = format!("foo{}", i);
            counter.incr(&key, 1);
        }
        for i in 0..100000 {
            let key = format!("foo{}", i);
            assert_eq!(counter.get(&key).unwrap(), 1);
        }
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
            assert_eq!(shared.get(&key).unwrap(), nincr);
        }
    }

    #[test]
    fn get() {
        let counter = Counter::new();
        let _ = counter.incr("foo", 1);
        let count = counter.get("foo").unwrap();
        assert_eq!(count, 1);
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
    fn bench_incr_resize(b: &mut Bencher) {
        let counter = Counter::new();
        for i in 0..INITIAL_SIZE {
            let key = format!("foo{}", i);
            counter.incr(&key, 1);
        }
        b.iter(|| counter.incr("foo1", 1))
    }

    #[bench]
    fn bench_get_resize(b: &mut Bencher) {
        let counter = Counter::new();
        for i in 0..87 {
            let key = format!("foo{}", i);
            counter.incr(&key, 1);
        }
        b.iter(|| counter.get("foo1"))
    }

    #[bench]
    fn bench_incr_1000_keys(b: &mut Bencher) {
        let counter = Counter::new();
        let keys: Vec<String> = (0..1000).map(|i|{format!("foo_{}", i)}).collect();
        b.iter(|| {
            for i in 0..1000 {
                counter.incr(&keys[i], 1);
            }
        })
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
