#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::{Arc,
                atomic::{AtomicU64, Ordering}};

use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeBuilder};
use dashmap::DashMap;

#[bench]
fn bench_dashmap_get(b: &mut Bencher) {
    let map = Arc::new(DashMap::new());
    for index in 0..16 {
        map.insert(index, Arc::new(AtomicU64::new(0)));
    }

    let map_copy = map.clone();
    b.iter(move || {
        let mut joins = Vec::with_capacity(16);
        for index in 0..16 {
            let map_clone = map_copy.clone();
            let join = thread::spawn(move || {
                for _ in 0..100000 {
                    if let Some(item) = map_clone.get(&index) {
                        for _ in 0..100 {
                            item.value().fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
            joins.push(join);
        }

        for join in joins {
            assert!(join.join().is_ok());
        }
    });

    println!("map: {:?}", map);
}