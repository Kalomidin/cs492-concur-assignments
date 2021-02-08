//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;


const HI_MASK: usize = 0x8000000000000000usize;

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
           let key = index.reverse_bits();

           let bucket = self.buckets.get(key, guard);
           let shared_bucket = bucket.load(Ordering::Acquire, guard);
   
           if !shared_bucket.is_null() {
               return unsafe {
                   Cursor::from_raw(
                       bucket,
                       shared_bucket.as_raw()
                   )
               };
           }
   
           let parent = {
               let mut size = self.size.load(Ordering::Acquire);
               while {
                size = size >> 1;
                size > index
               } {};
               index - size
           };

           // Get the cursor from the list and insert node if necessary
           let cursor =
               if parent == 0
               { self.list.head(guard) } else
               { self.lookup_bucket(parent, guard) };
   
           let mut sentinel_node = Owned::new(
               Node::new(key, None)
           );
   
           let inserted_cursor = loop {
               let (is_there, mut my_cursor) = loop {
                   let mut my_cursor = cursor.clone();
   
                   match my_cursor.find_harris(&key, guard) {
                       Ok(found) => break (found, my_cursor),
                       Err(_) => ()
                   }
               };
   
               if is_there {
                   drop(sentinel_node);
                   break my_cursor;
               }
   
               match my_cursor.insert(sentinel_node, guard) {
                   Ok(_) => break my_cursor,
                   Err(e) => { sentinel_node = e; }
               };
           };
           

           // Set the Cursor to the bucket
           match bucket.compare_and_set(
               Shared::null(),
               inserted_cursor.curr(),
               Ordering::Release,
               guard
           ) {
               Err(e) => drop(e.new),
               _ => ()
           };
   
           inserted_cursor
       }
   
       /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
       /// Returns `(size, found, cursor)`
       fn find<'s>(
           &'s self,
           key: &usize,
           guard: &'s Guard,
       ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
           let key = key.to_owned();
           let size = self.size.load(Ordering::Acquire);
   
           // Take the index since size * 2 > count
           let index = key % size;
           let cursor = self.lookup_bucket(index, guard);
           let size = self.size.load(Ordering::Acquire);
           let converted_key = get_converted_key(&key);
           loop {
               let mut my_cursor = cursor.clone();
               match my_cursor.find_harris(&converted_key, guard) {
                   Ok(found) => break (size, found, my_cursor),
                   Err(_) => ()
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
        let content_key = get_converted_key(key);
        let mut node = Owned::new(Node::new(content_key, Some(value)));
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

fn get_converted_key(key: &usize) -> usize { (key | HI_MASK ).reverse_bits() }