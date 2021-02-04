use crossbeam_epoch::{self as epoch, Atomic, Owned};
use std::sync::atomic::Ordering::SeqCst;

fn main() {
    let a: Atomic<i32> = Atomic::null();
    let guard = &epoch::pin();
    let shared = a.load(SeqCst, &guard);
    let shared_2 = shared.with_tag(2);
    assert_eq!(shared, shared_2);
}
