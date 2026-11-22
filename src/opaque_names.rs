//! An append-only table of exact strings. A resident name is owned by the
//! table slot that published it. Slot indices are stable integer handles.
use memmap2::{Advice, MmapMut};
use std::{
    ptr::{self, NonNull},
    sync::atomic::{
        AtomicPtr,
        Ordering::{AcqRel, Acquire, Relaxed},
    },
};

struct Name {
    hash: u64,
    bytes: Box<[u8]>,
}

pub(super) struct Table {
    _mapping: MmapMut,
    start: NonNull<AtomicPtr<Name>>,
    slots: usize,
}

// The mapping has a stable address and every access to a slot is atomic.
// Names are immutable after publication and live until the table is dropped.
unsafe impl Send for Table {}
unsafe impl Sync for Table {}

struct Unpublished(*mut Name);

impl Drop for Unpublished {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // The table never took ownership of this allocation.
            unsafe { drop(Box::from_raw(self.0)) };
        }
    }
}

pub(super) fn hash(bytes: &[u8]) -> u64 {
    const HASH: ahash::RandomState = ahash::RandomState::with_seeds(
        0x243f6a8885a308d3,
        0x13198a2e03707344,
        0xa4093822299f31d0,
        0x082efa98ec4e6c89,
    );
    HASH.hash_one(bytes)
}

impl Table {
    pub(super) fn with_capacity(slots: usize) -> Self {
        debug_assert!(slots.is_power_of_two());
        let bytes = slots * size_of::<AtomicPtr<Name>>();
        let mut mapping = MmapMut::map_anon(bytes).unwrap();
        let _ = mapping.advise(Advice::HugePage);
        if bytes >= (16 << 20) {
            std::thread::scope(|scope| {
                let mapping = &mapping;
                let step = bytes / 4;
                for start in (0..bytes).step_by(step) {
                    scope.spawn(move || {
                        let _ = mapping.advise_range(Advice::PopulateWrite, start, step);
                    });
                }
            });
        }
        let start = NonNull::new(mapping.as_mut_ptr().cast::<AtomicPtr<Name>>()).unwrap();
        Self {
            _mapping: mapping,
            start,
            slots,
        }
    }

    fn cells(&self) -> &[AtomicPtr<Name>] {
        // The Linux AMD64 mapping is page-aligned and zero-initialized.
        // A zero slot represents a null pointer. The mutable mapped bytes
        // are accessed only through this typed, atomic view.
        unsafe { std::slice::from_raw_parts(self.start.as_ptr(), self.slots) }
    }

    pub(super) fn intern(&self, bytes: &[u8], hash: u64) -> Option<usize> {
        let cells = self.cells();
        let mask = cells.len() - 1;
        let mut at = hash as usize & mask;
        let mut candidate = Unpublished(ptr::null_mut());
        for _ in 0..cells.len().min(128) {
            let cell = &cells[at];
            let mut current = cell.load(Acquire);
            if current.is_null() {
                if candidate.0.is_null() {
                    candidate.0 = Box::into_raw(Box::new(Name {
                        hash,
                        bytes: bytes.into(),
                    }));
                }
                match cell.compare_exchange(ptr::null_mut(), candidate.0, AcqRel, Acquire) {
                    Ok(_) => {
                        // The pointer was obtained from Box::into_raw before
                        // publication. Only the table can release it now.
                        candidate.0 = ptr::null_mut();
                        return Some(at);
                    }
                    Err(occupied) => current = occupied,
                }
            }
            // Acquire above pairs with publication. The table retains the
            // initialized object until its last borrower has gone away.
            let existing = unsafe { &*current };
            if existing.hash == hash && existing.bytes.as_ref() == bytes {
                return Some(at);
            }
            at = (at + 1) & mask;
        }
        // Every examined slot is occupied permanently. The same string
        // will always take this same overflow path after this point.
        None
    }

    pub(super) fn get(&self, at: usize) -> &[u8] {
        let name = self.cells()[at].load(Acquire);
        assert!(!name.is_null(), "unknown opaque ID");
        // Only a published slot can yield a handle, and it remains owned by
        // this table for the duration of the returned borrow.
        unsafe { (*name).bytes.as_ref() }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        for cell in self.cells() {
            let name = cell.load(Relaxed);
            if !name.is_null() {
                // No reader retains a borrow of the table after Drop starts.
                unsafe { drop(Box::from_raw(name)) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_publish_preserves_full_string_equality() {
        let table = Table::with_capacity(16);
        std::thread::scope(|scope| {
            let jobs = (0..8)
                .map(|_| {
                    let table = &table;
                    scope.spawn(move || {
                        (
                            table
                                .intern(b"a-wider-key-with-a-long-common-prefix-a", 7)
                                .unwrap(),
                            table
                                .intern(b"a-wider-key-with-a-long-common-prefix-b", 7)
                                .unwrap(),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let pairs = jobs
                .into_iter()
                .map(|job| job.join().unwrap())
                .collect::<Vec<_>>();
            assert!(pairs.iter().all(|pair| *pair == pairs[0]));
            assert_ne!(pairs[0].0, pairs[0].1);
            assert_eq!(
                table.get(pairs[0].0),
                b"a-wider-key-with-a-long-common-prefix-a"
            );
            assert_eq!(
                table.get(pairs[0].1),
                b"a-wider-key-with-a-long-common-prefix-b"
            );
        });
    }
}
