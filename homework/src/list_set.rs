#![allow(clippy::mutex_atomic)]
use std::cmp::Ordering;
use std::mem;
use std::ptr;
use std::sync::{Mutex, MutexGuard};
use std::{borrow::Borrow, cmp};

use mem::replace;

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        let cursor = &self.0;
        if (*cursor).is_null() {
            return false;
        }

        let is_equal = unsafe {(***cursor).data.cmp(key)};
        match is_equal {
            Ordering::Equal => return true,
            Ordering::Greater => return false,
            _ =>(),
        }

        let mut cursor = unsafe {(***cursor).next.lock().unwrap()};

        let response = loop {
            if (cursor).is_null() {
                break(false)
            }
            let is_equal = unsafe {(**cursor).data.cmp(key)};
            match is_equal {
                Ordering::Equal => break (true),
                Ordering::Greater => break(false),
                Ordering::Less => {
                    cursor = unsafe {(**cursor).next.lock().unwrap()};
                }
            }
        };
        self.0 = cursor;
        return response;
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord + std::fmt::Debug> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let value = self.head.lock().unwrap();

        let mut cursor = Cursor(value);
        let contains = cursor.find(&key);
        (contains, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let (is_there, _cursor) = self.find(key);
        return is_there;
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
            let (contains, mut cursor) = self.find(&key);
            if contains {
                return Err(key);
            }

            let next_node = *cursor.0;
            let new_node = Node::new(key, next_node);
            (*cursor.0) = new_node;

            Ok(())
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let (contains, cursor) = self.find(&key);
        if !contains {
            return Err(());
        }
        let Cursor(mut deleted_node) = cursor;

        let deleted_node_box = unsafe { Box::from_raw(*deleted_node) };
        let next_node = *deleted_node_box.next.lock().unwrap();
        (*deleted_node) = next_node;
        Ok(deleted_node_box.data)
    }
}
#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>, bool);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()), true)
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        let (mutex_value, is_owned) = match &self.0 {
            None => return None,
            Some(mutex_value) => {
                (mutex_value, self.1)
            }
        };

        if (*mutex_value).is_null() {
            return None
        }

        let node = unsafe { & *(*(*mutex_value)) };

        if is_owned {
            self.1 = false;
            return Some(& node.data);
        }

        let next_value = node.next.lock().unwrap();

        if (*next_value).is_null() {
            self.0 = None;
            None
        } else {
            let next_node = unsafe { & *(*next_value) };
            self.0 = Some(next_value);

            Some(& next_node.data)
        }
    }
}

// Why below code is buggy ???
// #[derive(Debug)]
// pub struct Iter<'l, T>(Option<(MutexGuard<'l, *mut Node<T>>, &'l T)>);

// impl<T> OrderedListSet<T> {
//     /// An iterator visiting all elements.
//     pub fn iter(&self) -> Iter<T> {
//         let head = self.head.lock().unwrap();
//         if (*head).is_null() {
//             return Iter(None)
//         }
//         let value = &unsafe{&(**head)}.data;
//         Iter(Some((head, value)))
//     }
// }

// impl<'l, T> Iterator for Iter<'l, T> {
//     type Item = &'l T;

//     fn next(&mut self) -> Option<Self::Item> {
//         match self {
//             Iter(Some( __)) => {
//                 let (head, data) = mem::replace(&mut self.0, None).unwrap();
//                 let next_head = unsafe{&(**head)}.next.lock().unwrap();

//                 if (*next_head).is_null() {
//                     return Some(data);
//                 }

//                 let new_data = &unsafe{&(**next_head)}.data;
//                 self.0 = Some((next_head, new_data));
//                 return Some(data)

//             }
//             Iter(None) => None,
//         }
//     }
// }

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut_mutex = self.head.get_mut().unwrap();
        loop {
            if (*mut_mutex) .is_null() {
                return;
            }
                let mutex_box = unsafe{Box::from_raw(*mut_mutex)};
                let next = (mutex_box).next;
                (*mut_mutex) = next.into_inner().unwrap();
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}