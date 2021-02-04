//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let guard = unsafe { unprotected() };

        for inner in self.inner.iter_mut() {
            let atomic_segment = unsafe { &*(inner as *const _ as *const Atomic<Segment>) };
            let mut atomic_value = atomic_segment.load(Ordering::Acquire, guard);
            if atomic_value.is_null() {
                return;
            }
            let value = unsafe { atomic_value.deref_mut() };
            drop(value);
            drop(inner);
        }
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        let guard = unsafe { unprotected() };
        let mut root = self.root.load(Ordering::Acquire, guard);
        if root.is_null() {
            return;
        }
        let value = unsafe { root.deref_mut() };
        drop(value);
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        let mut parent = self.root.load(Ordering::Acquire, &guard);

        // Increase the height until it is necessary to do so
        while {
            // Update values
            parent = self.root.load(Ordering::Acquire, &guard);
            let length = parent.tag() * SEGMENT_LOGSIZE;
            length < index || length == 0
        } {
            // Insert the new segment
            let mut new_segment = Segment::new();
            new_segment[0] = AtomicUsize::new(parent.into_usize());
            let owned_new_segment = Owned::new(new_segment).with_tag(parent.tag() + 1);
            match self
                .root
                .compare_and_set(parent, owned_new_segment, Ordering::Release, &guard)
            {
                Ok(_) => (),
                Err(e) => drop(e.new),
            };
        }

        let mut current_height = parent.tag();
        let mut current_segment: Atomic<Segment> = Atomic::from(parent);

        // Loop until you find the entry in the lowest depth of the tree, which has height 1
        loop {
            let segment_index =
                (index >> ((current_height - 1) * SEGMENT_LOGSIZE)) & (SEGMENT_LOGSIZE - 1);
            let next_current_segment = unsafe {
                current_segment
                    .load(Ordering::Acquire, &guard)
                    .deref()
                    .get_unchecked(segment_index)
            };
            if current_height == 1 {
                return unsafe { &*(next_current_segment as *const _ as *const Atomic<T>) };
            }
            let next_usize_value = next_current_segment.load(Ordering::Acquire);
            let next_current_segment_shared = unsafe { Shared::from_usize(next_usize_value) };

            // Insert empty Segment if necessary
            if next_current_segment_shared.is_null() {
                let new_segment = Segment::new();
                let owned_new_segment = Owned::new(new_segment);
                let new_segment_usize = owned_new_segment.into_usize();
                match next_current_segment.compare_and_swap(
                    next_usize_value,
                    new_segment_usize,
                    Ordering::AcqRel,
                ) {
                    // If successfully updated the next current segment from none to new segment usize, set the current segment as next current segment
                    value if value == next_usize_value => {
                        current_segment =
                            unsafe { Atomic::from(Shared::from_usize(new_segment_usize)) };
                    }
                    _value => {
                        // Do nothing since value was inserted by some other thread
                    }
                }
            }
            // If the segment exists, continue until height becomes 1
            else {
                current_segment = Atomic::from(next_current_segment_shared);
                current_height -= 1;
            }
        }
    }
}
