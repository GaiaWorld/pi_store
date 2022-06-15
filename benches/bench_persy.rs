#![feature(test)]

extern crate test;
use test::Bencher;

use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, bounded};
use rand::{Rng, SeedableRng, seq::SliceRandom, rngs::SmallRng};
use persy::{Config, Persy, ValueMode, PersyId, ToSegmentId};

use r#async::rt::multi_thread::MultiTaskRuntimeBuilder;

#[bench]
fn bench_random_insert_persy(b: &mut Bencher) {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let mut rng = SmallRng::from_entropy();
    let mut vec: Vec<u64> = (0..10).collect();
    vec.shuffle(&mut rng);

    let persy = Persy::open_or_create_with("./db/test_persy.db",
                                           Config::new(),
                                           |_persy| Ok(())).unwrap();
    let mut tx = persy.begin().unwrap();
    if !tx.exists_segment("test_seg").unwrap() {
        tx.create_segment("test_seg").unwrap();
    }
    tx.create_index::<u64, PersyId>("test_index", ValueMode::Replace);
    let prepared = tx.prepare().unwrap();
    prepared.commit().unwrap();

    b.iter(move || {
        let (sender, receiver) = unbounded();

        for idx in &vec {
            let sender_copy = sender.clone();

            let index = *idx;
            let mut tx = persy.begin().unwrap();
            rt.spawn(rt.alloc(), async move {
                if let Ok(id) = tx.insert("test_seg", index.to_le_bytes().as_slice()) {
                    if let Ok(_) = tx.put("test_index", index, id) {
                        if let Ok(prepared) = tx.prepare() {
                            if let Ok(_) = prepared.commit() {
                                sender_copy.send(Ok(()));
                            } else {
                                sender_copy.send(Err("".to_string()));
                            }
                        } else {
                            sender_copy.send(Err("".to_string()));
                        }
                    } else {
                        sender_copy.send(Err("".to_string()));
                    }
                } else {
                    sender_copy.send(Err("".to_string()));
                }
            });
        }

        let mut error_count = 0;
        let mut count = 0;
        loop {
            match receiver.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt.timing_len(),
                        rt.len(),
                        e
                    );
                    continue;
                },
                Ok(result) => {
                    if result.is_err() {
                        error_count += 1;
                    }

                    count += 1;
                    if count >= 10 {
                        break;
                    }
                },
            }
        }
    });
}