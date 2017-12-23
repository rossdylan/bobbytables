use std::fmt;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::sync::atomic;
use std::sync::atomic::Ordering;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResizeStateTryFromError(());

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SlotStateTryFromError(());

impl fmt::Display for ResizeStateTryFromError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "converted usize out of range of `ResizeState`".fmt(f)
    }
}

impl fmt::Display for SlotStateTryFromError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "converted usize out of range of `SlotState`".fmt(f)
    }
}

/// This enum is used to describe the state transitions during a resize operation
/// on the hashtable.
/// 1. The steady-state mode is `Complete`. In this state the load factor of the
///    hashtable is below `MAX_LOAD_FACTOR`, and no allocation, or copying operations
///    are taking place.
/// 2. When the load factor meets or exceeds `MAX_LOAD_FACTOR` an attempt is made to switch the
///    state to `Allocating`. this is the signal that a thread has started allocating the new
///    backing vector. Threads that lose the race for allocation will do nothing until the state
///    switches to `Copying`.
/// 3. After switching to `Copying` all threads will try and move some percentage of `HashSlot`
///    entries to the new table. When copying a slot we check to see if it has a tombstone and if
///    not we add it's value to the new table.
/// 4. Once the old table has a used value of 0 we return to `Complete`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ResizeState {
    Complete = 0,
    Allocating = 1,
    Copying = 2,
    Deallocating = 3,
}

impl Default for ResizeState {
    fn default() -> Self {
        ResizeState::Complete
    }
}

impl TryFrom<usize> for ResizeState {
    type Error = ResizeStateTryFromError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ResizeState::Complete),
            1 => Ok(ResizeState::Allocating),
            2 => Ok(ResizeState::Copying),
            3 => Ok(ResizeState::Deallocating),
            _ => Err(ResizeStateTryFromError(())),
        }
    }
}

impl From<ResizeState> for usize {
    #[inline]
    fn from(value: ResizeState) -> usize {
        match value {
            ResizeState::Complete => 0,
            ResizeState::Allocating => 1,
            ResizeState::Copying => 2,
            ResizeState::Deallocating => 3,
        }
    }
}

/// The `SlotState` enum is used to track the current status of a slot during the resize operation.
/// 1. Slots are created in `SlotState::Alive` and spend most of their life here
/// 2. During a call to `resize_copy` a thread will attempt to CAS the state to `SlotState::Copying`. This locks
///    the slot and prevents other threads from moving it, but allows calls to `unsafe_get` to still
///    use the value in this slot.
/// 3. After the slot has been copied into the main slots vector the state moves to
///    `SlotState::Dead` to indicate this slot can be ignored.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    Dead = 0,
    Allocating = 1,
    Alive = 2,
    Copying = 3,
    Copied = 4,
}

impl Default for SlotState {
    fn default() -> Self {
        SlotState::Dead
    }
}

impl TryFrom<usize> for SlotState {
    type Error = SlotStateTryFromError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SlotState::Dead),
            1 => Ok(SlotState::Allocating),
            2 => Ok(SlotState::Alive),
            3 => Ok(SlotState::Copying),
            4 => Ok(SlotState::Copied),
            _ => Err(SlotStateTryFromError(())),
        }
    }
}

impl From<SlotState> for usize {
    #[inline]
    fn from(value: SlotState) -> usize {
        match value {
            SlotState::Dead => 0,
            SlotState::Allocating => 1,
            SlotState::Alive => 2,
            SlotState::Copying => 3,
            SlotState::Copied => 4,
        }
    }
}


/// `AtomicState` is a struct and impl that handles all the conversions and error
/// checking for driving a state-machine via an internal `AtomicUsize`. It is generic
/// across enums that implement `Default`, `PartialEq`, `Copy`, and `TryFrom`<usize>.
/// an usize impl for `From`<insert enum here> is also required.
pub struct AtomicState<E: TryFrom<usize> + Default + PartialEq + Copy> {
    state: atomic::AtomicUsize,
    state_type: PhantomData<E>,
}



impl<E: TryFrom<usize> + Default + PartialEq + Copy> AtomicState<E>  where
    usize: From<E>,
    <E as TryFrom<usize>>::Error: fmt::Display {

    /// Generate a new AtomicState using, initialized using the default value
    /// of the enum.
    fn new() -> Self {
        AtomicState{
            state: atomic::AtomicUsize::new(usize::from(E::default())),
            state_type: PhantomData,
        }
    }

    /// Return the current value of the state. Uses `Ordering::Relaxed`.
    pub fn get(&self) -> E {
        let result = E::try_from(self.state.load(Ordering::Acquire));
        match result {
            Ok(val) => val,
            Err(err) => panic!("Failed to convert AtomicUsize into enum: {}", err),
        }
    }

    /// Set the current state using `Ordering::Release`. NOTE: Only use in
    /// conjunction with set_cas to provide a safe transition between states.
    pub fn set(&self, state: E) {
        self.state.store(usize::from(state), Ordering::Release);
    }

    /// Run a CAS operation on the current state to provide a safe state
    /// transition and gain exclusive access until set is called again. It
    /// provides this guarentee using `Ordering::Acquire`.
    pub fn set_cas(&self, prev: E, next: E) -> bool {
        let result = E::try_from(self.state.compare_and_swap(
            usize::from(prev),
            usize::from(next),
            Ordering::Acquire,
        ));
        match result {
            Ok(val) =>  val == prev,
            Err(err) => panic!("Failed to convert AtomicUsize into enum: {}", err),
        }
    }
}

impl<E: TryFrom<usize> + Default + PartialEq + Copy> Default for AtomicState<E>  where
    usize: From<E>,
    <E as TryFrom<usize>>::Error: fmt::Display {

    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_resize() {
        let state: AtomicState<ResizeState> = AtomicState::new();
        assert_eq!(state.get(), ResizeState::default());
    }

    #[test]
    fn new_slot() {
        let state: AtomicState<SlotState> = AtomicState::new();
        assert_eq!(state.get(), SlotState::default());
    }

    #[test]
    fn set_resize() {
        let state: AtomicState<ResizeState> = AtomicState::new();
        assert_eq!(state.get(), ResizeState::default());

        state.set(ResizeState::Copying);
        assert_eq!(state.get(), ResizeState::Copying);
    }
}
