use std::sync::atomic::Ordering;

use ::key::{hashkey_to_string};
use ::table::*;
use ::state::SlotState;

pub struct CounterIter<'a> {
    pub slots: &'a VectorTable,
    pub index: usize,
}

impl<'a> Iterator for CounterIter<'a> {
    type Item = (String, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let ret;
        loop {
            let slot_opt = self.slots.get_index(self.index);
            match slot_opt {
                Some(slot) => match slot.state.get() {
                    SlotState::Alive | SlotState::Copying | SlotState::Copied => {
                        let key_ptr = slot.key.load(Ordering::Acquire);
                        let val = slot.value.load(Ordering::Relaxed);
                        if key_ptr.is_null() {
                            panic!("Iterator found an active slot with a null key");
                        }
                        let key = unsafe { hashkey_to_string(&(*key_ptr)) };
                        self.index += 1;
                        ret = Some((key, val));
                        break;
                    },
                    _ => {
                        self.index += 1;
                    }
                },
                None => {
                    self.slots.remove_thread();
                    ret = None;
                    break;
                },
            }
        }
        ret
    }
}
