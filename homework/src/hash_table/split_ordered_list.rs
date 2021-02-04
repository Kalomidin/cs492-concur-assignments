//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        let bucket_atomic_value = self.buckets.get(index, guard);
        let bucket_shared_value = bucket_atomic_value.load(Ordering::Acquire, guard);
        if !bucket_shared_value.is_null() {
            return unsafe { Cursor::from_raw(bucket_atomic_value, bucket_shared_value.as_raw()) };
        }

        // Inser to the list if necessary and create empty entries to \
        // make same as bucket
        let mut parent_node = self.size.load(Ordering::Acquire);
        let list_idx = loop {
            // Search for the value
            if parent_node > index {
                parent_node = parent_node >> 1;
            } else {
                break parent_node;
            }
        };
        let mut list_cursor = if list_idx == index {
            self.list.head(guard)
        } else {
            self.lookup_bucket(index, guard)
        };

        let mut new_node = Owned::new(Node::new(index, None));

        let updated_list_cursor = loop {
            // Find the given value
            let is_there = loop {
                match list_cursor.find_harris(&index, guard) {
                    Ok(value) => break value,
                    Err(_) => (),
                }
            };
            if is_there {
                drop(new_node);
                break list_cursor;
            }
            match list_cursor.insert(new_node, guard) {
                Ok(()) => break list_cursor,
                Err(e) => new_node = e,
            };
        };

        // Set the cursor curr value in the Bucket
        match bucket_atomic_value.compare_and_set(
            Shared::null(),
            updated_list_cursor.curr(),
            Ordering::Acquire,
            guard,
        ) {
            Ok(response) => drop(response),
            Err(e) => drop(e.new),
        };
        updated_list_cursor
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let key = key.to_owned();
        let mut cursor = self.lookup_bucket(key, guard);
        let size = self.size.load(Ordering::Acquire);

        loop {
            match cursor.find_harris(&key, guard) {
                Ok(response) => return (size, response, cursor),
                Err(_) => (),
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if !found {
            return None;
        }
        cursor.lookup().unwrap().as_ref()
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let mut node = Owned::new(Node::new(key.to_owned(), Some(value)));

        loop {
            let (size, found, mut cursor) = self.find(key, guard);
            if found {
                let value = node.into_box().into_value().unwrap();
                return Err(value);
            }
            match cursor.insert(node, guard) {
                Ok(()) => {
                    let load_factor = SplitOrderedList::<V>::LOAD_FACTOR;
                    let count = self.count.fetch_add(1, Ordering::Release);
                    if count > size * load_factor {
                        self.size
                            .compare_and_swap(size, size * load_factor, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(value) => node = value,
            }
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_size, found, cursor) = self.find(key, guard);
        if !found {
            return Err(());
        }
        match cursor.delete(guard) {
            Ok(value) => {
                self.count.fetch_sub(1, Ordering::Release);
                Ok(value.as_ref().unwrap())
            }
            Err(()) => Err(()),
        }
    }
}
