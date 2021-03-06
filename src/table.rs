/// table defines the underline storage method of hashing values to positions
/// into a vector of slots. It takes care of the concurrent accesses and
/// provides an unsafe api for grabbing the slots out of that storage.

use std::ptr;
use std::sync::atomic;
use std::sync::atomic::Ordering;

use ::state::{AtomicState, SlotState};
use ::key::{HashKey, djb2_hash, hashkey_to_string};

/// `IncrResult` is used to tell the caller if we need to retry the incr. This 
/// should only happen if a resize begins in the middle of an increment
/// operation.
pub enum IncrResult {
    Current(usize),
    Outdated,
}


#[derive(Default)]
pub struct Slot {
    pub state: AtomicState<SlotState>,
    pub key: atomic::AtomicPtr<HashKey>,
    pub value: atomic::AtomicUsize,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let key = self.key.load(Ordering::Relaxed);
        if !key.is_null() {
            let _: Box<HashKey> = unsafe { Box::from_raw(key) };
        }
    }
}


pub struct VectorTable {
    slots: Vec<Slot>,
    used: atomic::AtomicUsize,
    active_threads: atomic::AtomicUsize,
}

impl VectorTable {
    pub fn new(size: usize) -> Self {
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(Slot::default())
        }
        VectorTable{
            slots: slots,
            used: atomic::AtomicUsize::new(0),
            active_threads: atomic::AtomicUsize::new(0),
        }
    }

    pub fn add_thread(&self) {
        self.active_threads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_thread(&self) {
        self.active_threads.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn active_threads(&self) -> usize {
        self.active_threads.load(Ordering::Relaxed)
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn cap(&self) -> usize {
        self.slots.len()
    }

    fn find_index(&self, key: HashKey) -> Option<usize> {
        let size = self.slots.len();
        let hash = djb2_hash(&key) % size;
        let mut index = hash;
        loop {
            let slot = &self.slots[index];
            let slot_key = slot.key.load(Ordering::Relaxed);
            if slot_key.is_null() || unsafe { *slot_key == key }{
                break
            }
            index = (index + 1) % size;
            if index == hash {
                return None;
            }
        }
        Some(index)
    }

    pub fn get_index(&self, index: usize) -> Option<&Slot> {
        self.slots.get(index)
    }

    fn get_slot(&self, key: HashKey) -> Option<&Slot> {
        self.find_index(key).and_then(|i|{self.get_index(i)})
    }

    pub fn get(&self, key: HashKey) -> Option<usize> {
        let size = self.slots.len();
        let hash = djb2_hash(&key) % size;
        let mut index = hash;
        loop {
            let slot = &self.slots[index];
            let slot_key = slot.key.load(Ordering::Relaxed);
            if !slot_key.is_null() && unsafe { *slot_key == key }{
                let ret = Some(slot.value.load(Ordering::Relaxed));
                return ret
            }
            index = (index + 1) % size;
            if index == hash {
                return None
            }
        }
    }

    /// incr attempts to find the given key in the table and increment its value
    /// by the provided amount. If the key is not present in the table it will
    /// attempt to add it. We have two CAS operations that ensure we safely fill
    /// the slot. First we attempt to CAS the state from `SlotState::Dead` to
    /// `SlotState::Allocating`. This gives us exclusive access to it until we
    /// finish allocating the key. The transition to `SlotState::Alive` releases
    /// that "lock" and we increment the value as normal. If a slot was copied
    /// out from under us, we return an error
    pub fn incr(&self, key: HashKey, val: usize) -> IncrResult {
        loop {
            match self.get_slot(key) {
                Some(slot) => match slot.state.get() {
                    SlotState::Dead => {
                        let acquired = slot.state.set_cas(SlotState::Dead, SlotState::Allocating);
                        if acquired {
                            let res = slot.key.compare_and_swap(
                                ptr::null_mut(),
                                Box::into_raw(Box::new(key)),
                                Ordering::Relaxed,
                            );
                            if !res.is_null() {
                                panic!(
                                    "Acquired slot, key was not null: key:{}, slot.key:{}",
                                    hashkey_to_string(&key),
                                    unsafe{ hashkey_to_string(&(*res)) },
                                );
                            }
                            self.used.fetch_add(1, Ordering::Relaxed);
                            slot.state.set(SlotState::Alive);
                            let ret = slot.value.fetch_add(val, Ordering::Relaxed);
                            return IncrResult::Current(ret)
                        }
                    },
                    SlotState::Alive => {
                        let slot_key = slot.key.load(Ordering::Relaxed);
                        if unsafe{ *slot_key == key }{
                            let ret = slot.value.fetch_add(val, Ordering::Relaxed);
                            return IncrResult::Current(ret)
                        }
                    },
                    SlotState::Allocating => {},
                    SlotState::Copying | SlotState::Copied => return IncrResult::Outdated,
                },
                None => panic!("VectorTable is out of space"),
            }
        }
    }

    pub fn copy_index_to(&self, index: usize, table: &VectorTable) -> bool {
        match self.slots.get(index) {
            Some(slot) => match slot.state.get() {
                SlotState::Dead | SlotState::Copying | SlotState::Copied => false,
                SlotState::Allocating => panic!("Tried to copy a slot in state Allocating"),
                SlotState::Alive => {
                    let acquired = slot.state.set_cas(SlotState::Alive, SlotState::Copying);
                    if acquired {
                        let slot_key = slot.key.load(Ordering::Relaxed);
                        if slot_key.is_null() {
                            panic!("Slot in Alive state has null key");
                        }
                        unsafe {
                            if let IncrResult::Outdated = table.incr(*slot_key, slot.value.load(Ordering::Relaxed)) {
                                panic!("copy_index_to destination contains outdated data");
                            }
                        }
                        slot.state.set(SlotState::Copied);
                        true
                    } else {
                        false
                    }
                },
            },
            None => false,
        }
    }

    pub fn copy_key_to(&self, key: HashKey, table: &VectorTable) -> bool {
        match self.get_slot(key) {
            Some(slot) => match slot.state.get() {
                SlotState::Dead | SlotState::Copying | SlotState::Copied => false,
                SlotState::Allocating => panic!("Tried to copy a slot in state Allocating"),
                SlotState::Alive => {
                    let acquired = slot.state.set_cas(SlotState::Alive, SlotState::Copying);
                    if acquired {
                        let slot_key = slot.key.load(Ordering::Relaxed);
                        if slot_key.is_null() {
                            panic!("Slot in Alive state has null key");
                        }
                        if unsafe{ *slot_key != key } {
                            panic!("Slot found doesn't have correct key");
                        }
                        if let IncrResult::Outdated = table.incr(key, slot.value.load(Ordering::Relaxed)) {
                            panic!("The table we are copying to is outdated");
                        }
                        slot.state.set(SlotState::Copied);
                        true
                    } else {
                        false
                    }
                },
            },
            None => false,
        }
    }
}


#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::Arc;
    use test::Bencher;
    use super::*;
    use rand;
    use rand::Rng;
    use ::key::clean_key;

    const INITIAL_SIZE: usize = 128;

    #[test]
    fn increment() {
        let table = VectorTable::new(INITIAL_SIZE);
        let res = match table.incr(clean_key("foo"), 1) {
            IncrResult::Current(v) => v,
            IncrResult::Outdated => panic!("Increment test returned Outdated"),
        };
        assert_eq!(res, 0);
        let slot = table.get_slot(clean_key("foo")).unwrap();
        assert_eq!(slot.value.load(Ordering::Relaxed), 1);
        assert_eq!(table.cap(), INITIAL_SIZE);
        assert_eq!(table.used(), 1);
    }

    #[test]
    fn sequential_increment() {
        let table = VectorTable::new(INITIAL_SIZE);

        for _ in 0..100000 {
            table.incr(clean_key("foo"), 1);
        }
        let slot = table.get_slot(clean_key("foo")).unwrap();
        assert_eq!(slot.value.load(Ordering::Relaxed), 100000);
        assert_eq!(table.cap(), INITIAL_SIZE);
        assert_eq!(table.used(), 1);
    }

    #[test]
    fn concurrent_multikey_increment() {
        let table = VectorTable::new(INITIAL_SIZE);
        let shared = Arc::new(table);
        let nthreads = 8;
        let nkeys = INITIAL_SIZE;
        let nincr = 10000;
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::Builder::new().name("table-cmki".to_string()).spawn(move|| {
                let mut rng = rand::thread_rng();
                for _ in 0..nincr {
                    let mut keys: Vec<usize> = (0..nkeys).collect();
                    rng.shuffle(keys.as_mut_slice());
                    for i in keys {
                        c.incr(clean_key(&format!("foo_{}", i)), 1);
                    }
                }
            }).unwrap());
        }
        for t in children {
            let _ = t.join();
        }
        for i in 0..INITIAL_SIZE {
            let slot = shared.get_slot(clean_key(&format!("foo_{}", i))).unwrap();
            assert_eq!(slot.value.load(Ordering::Relaxed), nincr * nthreads);
            assert_eq!(slot.state.get(), SlotState::Alive);
        }
        assert_eq!(shared.cap(), INITIAL_SIZE);
        assert_eq!(shared.used(), INITIAL_SIZE);
    }

    #[test]
    fn concurrent_increment() {
        let table = VectorTable::new(INITIAL_SIZE);
        let shared = Arc::new(table);
        let nthreads = 8;
        let nincr = 10000;
        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            children.push(thread::Builder::new().name("table-ci".to_string()).spawn(move|| {
                for _ in 0..nincr {
                    c.incr(clean_key("foo"), 1);
                }
            }).unwrap());
        }
        for t in children {
            let _ = t.join();
        }
        assert_eq!(shared.get_slot(clean_key("foo")).unwrap().value.load(Ordering::Relaxed), nincr * nthreads);
        assert_eq!(shared.get_slot(clean_key("foo")).unwrap().state.get(), SlotState::Alive);
    }

    #[test]
    fn fill_table() {
        let table = VectorTable::new(INITIAL_SIZE);

        for i in 0..INITIAL_SIZE {
            table.incr(clean_key(&format!("foo_{}", i)), i);
        }

        for i in 0..INITIAL_SIZE {
            let slot = table.get_slot(clean_key(&format!("foo_{}", i))).unwrap();
            assert_eq!(slot.value.load(Ordering::Relaxed), i);
            assert_eq!(slot.state.get(), SlotState::Alive);
        }
        assert_eq!(table.cap(), INITIAL_SIZE);
        assert_eq!(table.used(), INITIAL_SIZE);
    }

    #[test]
    #[should_panic]
    fn over_fill_table() {
        let table = VectorTable::new(INITIAL_SIZE);

        for i in 0..INITIAL_SIZE+1 {
            table.incr(clean_key(&format!("foo_{}", i)), i);
        }
    }

    #[test]
    fn copy_key() {
        let table = VectorTable::new(INITIAL_SIZE);
        let new_table = VectorTable::new(INITIAL_SIZE*3);
        table.incr(clean_key("foobar"), 100);

        let res = table.copy_key_to(clean_key("foobar"), &new_table);
        assert_eq!(res, true);

        let old_slot = table.get_slot(clean_key("foobar")).unwrap();
        let new_slot = new_table.get_slot(clean_key("foobar")).unwrap();

        assert_eq!(old_slot.state.get(), SlotState::Copied);
        assert_eq!(new_slot.state.get(), SlotState::Alive);
        assert_eq!(new_slot.value.load(Ordering::Relaxed), 100);
        assert_eq!(unsafe{*(new_slot.key.load(Ordering::Relaxed))}, clean_key("foobar"))
    }

    #[test]
    fn concurrent_copy_key() {
        let table = VectorTable::new(INITIAL_SIZE);
        let shared = Arc::new(table);
        let new_table = VectorTable::new(INITIAL_SIZE*3);
        let new_shared = Arc::new(new_table);
        let nthreads = 8;

        for i in 0..INITIAL_SIZE {
            shared.incr(clean_key(&format!("foobar_{}", i)), i);
        }

        let mut children = vec![];
        for _ in 0..nthreads {
            let c = shared.clone();
            let nc = new_shared.clone();

            children.push(thread::Builder::new().name("table-cck".to_string()).spawn(move|| {
                let mut rng = rand::thread_rng();
                let mut keys: Vec<usize> = (0..INITIAL_SIZE).collect();
                rng.shuffle(keys.as_mut_slice());
                for k in keys {
                    c.copy_key_to(clean_key(&format!("foobar_{}", k)), &nc);
                }
            }).unwrap());
        }
        for t in children {
            let _ = t.join();
        }
        for i in 0..INITIAL_SIZE {
            let slot = new_shared.get_slot(clean_key(&format!("foobar_{}", i))).unwrap();
            assert_eq!(slot.value.load(Ordering::Relaxed), i);
        }

    }

    #[bench]
    fn bench_incr(b: &mut Bencher) {
        let table = VectorTable::new(INITIAL_SIZE);
        let key = clean_key("foo");
        b.iter(|| table.incr(key, 1))
    }

    #[bench]
    fn bench_existing_incr(b: &mut Bencher) {
        let table = VectorTable::new(INITIAL_SIZE);
        let key = clean_key("foo");
        table.incr(key, 1);
        b.iter(|| table.incr(key, 1));
    }

    #[bench]
    fn bench_get_slot(b: &mut Bencher) {
        let table = VectorTable::new(INITIAL_SIZE);
        let key = clean_key("foo");
        table.incr(key, 100);
        b.iter(|| table.get_slot(key))
    }
}
