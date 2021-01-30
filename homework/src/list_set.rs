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
        let cursor = *self.0;
        if (cursor).is_null() {
            return false;
        }
        unsafe {

            // Move the cursor
            let next_value = (*cursor).next.lock().unwrap();
            let is_equal = (*cursor).data.cmp(key);

            match is_equal {
                Ordering::Equal => (true),
                Ordering::Greater => (false),
                _ => {
                    self.0 = next_value;
                    self.find(key)
                }
            }
        }
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
        //let sample_node = Node::new(data, next)
        let mut cursor = Cursor(value);
        let contains = cursor.find(&key);
        (contains, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let mut iter = self.iter();
        while let Some(value) = iter.next() {
            match value.cmp(key) {
                Ordering::Equal => return true,
                _ => (),
            }
        }
        false
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let mut head = self.head.lock().unwrap();
        if !(*head).is_null() && unsafe { Ordering::Greater == (**head).data.cmp(&key) } {
            let current_head = *head;
            let mut new_node = Node::new(key, current_head);
            mem::swap(&mut new_node, &mut (*head));

            Ok(())
        } else {
            drop(head);
            let (contains, mut cursor) = self.find(&key);
            if contains {
                return Err(key);
            }

            let next_node = *cursor.0;
            let mut new_node = Node::new(key, next_node);
            mem::swap(&mut new_node, &mut (*cursor.0));

            Ok(())
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let (contains, cursor) = self.find(&key);
        if !contains {
            return Err(());
        }
        let Cursor(mut cursor) = cursor;
        unsafe {
            let cursor_box = Box::from_raw(*cursor);
            let value = cursor_box.data;
            let next_cursor = *cursor_box.next.lock().unwrap();
            let _response = mem::replace(&mut (*cursor), next_cursor );

            Ok(value)
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter(Some(value)) => {
                unsafe {
                    if (*value).is_null() {
                        self.0 = None;
                        return None;
                    }

                    // Get the item
                    let box_value = Box::from_raw(**value);
                    let inner_value = Box::leak(box_value);
                    let item: Self::Item = &inner_value.data;

                    // Shift to next
                    let next_value = inner_value.next.lock().unwrap();
                    if next_value.is_null() {
                        self.0 = None;
                    } else {
                        self.0 = Some(next_value);
                    }

                    Some(item)
                }
            }
            Iter(None) => None,
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut_mutex = self.head.get_mut().unwrap();
        *mut_mutex = ptr::null_mut();
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
