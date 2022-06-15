#![feature(test)]

extern crate test;
use test::Bencher;

use rand::{Rng, SeedableRng, seq::SliceRandom, rngs::SmallRng};

use pi_store::free_lock::b_plus_tree::{CowBtreeMap, KeyRefGuard, KeyIterator, KVPairIterator};

#[bench]
fn bench_ascending_insert_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        for index in 0..100000 {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
        map.clear(None);
    });
}

#[bench]
fn bench_descending_insert_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        let mut index = 99999;
        for _ in 0..100000 {
            if let Ok(None) = map.upsert(index, index, None) {
                index -= 1;
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
        map.clear(None);
    });
}

#[bench]
fn bench_random_insert_tree(b: &mut Bencher) {
    let mut rng = SmallRng::from_entropy();
    let mut vec: Vec<usize> = (0..100000).collect();
    vec.shuffle(&mut rng);

    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        for index in &vec {
            if let Ok(None) = map.upsert(*index, *index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
        map.clear(None);
    });
}

#[bench]
fn bench_ascending_update_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    for index in 0..100000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        for index in 0..100000 {
            if let Ok(Some(_)) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
    });
}

#[bench]
fn bench_descending_update_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    for index in 0..100000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        let mut index = 99999;
        for _ in 0..100000 {
            if let Ok(Some(_)) = map.upsert(index, index, None) {
                index -= 1;
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
    });
}

#[bench]
fn bench_random_update_tree(b: &mut Bencher) {
    let mut rng = SmallRng::from_entropy();
    let mut vec: Vec<usize> = (0..100000).collect();
    vec.shuffle(&mut rng);

    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    for index in 0..100000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        for index in &vec {
            if let Ok(Some(_)) = map.upsert(*index, *index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }
    });
}

#[bench]
fn bench_ascending_remove_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        for index in 0..100000 {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }

        for index in 0..100000 {
            if let Ok(Some(_)) = map.remove(&index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid remove result");
            }
        }
    });
}

#[bench]
fn bench_descending_remove_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        for index in 0..100000 {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }

        let mut index = 99999;
        for _ in 0..100000 {
            if let Ok(Some(_)) = map.remove(&index, None) {
                index -= 1;
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid remove result");
            }
        }
    });
}

#[bench]
fn bench_random_remove_tree(b: &mut Bencher) {
    let mut rng = SmallRng::from_entropy();
    let mut vec: Vec<usize> = (0..100000).collect();
    vec.shuffle(&mut rng);

    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
    b.iter(move || {
        for index in 0..100000 {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid upsert result");
            }
        }

        for index in &vec {
            if let Ok(Some(_)) = map.remove(index,None) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid remove result");
            }
        }
    });
}

#[bench]
fn bench_ascending_get_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        for index in 0..1000000 {
            if let Some(_) = map.get(&index) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid get result");
            }
        }
    });
}

#[bench]
fn bench_descending_get_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        let mut index = 999999;
        for _ in 0..1000000 {
            if let Some(_) = map.get(&index) {
                index -= 1;
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid get result");
            }
        }
    });
}

#[bench]
fn bench_random_get_tree(b: &mut Bencher) {
    let mut rng = SmallRng::from_entropy();
    let mut vec: Vec<usize> = (0..1000000).collect();
    vec.shuffle(&mut rng);

    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        for index in &vec {
            if let Some(_) = map.get(index) {
                continue;
            } else {
                panic!("Bench tree failed, reason: invalid get result");
            }
        }
    });
}

#[bench]
fn bench_ascending_keys_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        if let KeyIterator::Ascending(mut keys) = map.keys(None, false) {
            let mut count = 0;
            while let Some(_) = keys.next() {
                count += 1;
                continue;
            }

            if count != 1000000 {
                panic!("Bench tree failed, reason: invalid iterate result");
            }
        } else {
            panic!("Bench tree failed, reason: invalid keys result");
        }
    });
}

#[bench]
fn bench_descending_keys_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        if let KeyIterator::Descending(mut keys) = map.keys(None, true) {
            let mut count = 0;
            while let Some(_) = keys.next() {
                count += 1;
                continue;
            }

            if count != 1000000 {
                panic!("Bench tree failed, reason: invalid iterate result");
            }
        } else {
            panic!("Bench tree failed, reason: invalid keys result");
        }
    });
}

#[bench]
fn bench_ascending_values_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        if let KVPairIterator::Ascending(mut values) = map.values(None, false) {
            let mut count = 0;
            while let Some((_, _)) = values.next() {
                count += 1;
                continue;
            }

            if count != 1000000 {
                panic!("Bench tree failed, reason: invalid iterate result");
            }
        } else {
            panic!("Bench tree failed, reason: invalid values result");
        }
    });
}

#[bench]
fn bench_descending_values_tree(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(128);
    for index in 0..1000000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Bench tree failed, reason: invalid upsert result");
        }
    }

    b.iter(move || {
        if let KVPairIterator::Descending(mut values) = map.values(None, true) {
            let mut count = 0;
            while let Some((_, _)) = values.next() {
                count += 1;
                continue;
            }

            if count != 1000000 {
                panic!("Bench tree failed, reason: invalid iterate result");
            }
        } else {
            panic!("Bench tree failed, reason: invalid values result");
        }
    });
}