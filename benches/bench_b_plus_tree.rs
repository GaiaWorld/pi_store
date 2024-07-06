#![feature(test)]

extern crate test;
use test::Bencher;

use std::thread;

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};
use redb::{Builder, StorageBackend, TableDefinition, Table, ReadOnlyTable, backends::InMemoryBackend, ReadableTable};

use pi_store::free_lock::b_plus_tree::{CowBtreeMap, KeyIterator, KVPairIterator};

#[bench]
fn bench_ascending_get_std_tree(b: &mut Bencher) {
    let map = parking_lot::RwLock::new(std::collections::BTreeMap::new());
    {
        let mut locked = map.write();
        for index in 0..1000000usize {
            let _ = locked.insert(index, index);
        }
    }

    b.iter(move || {
        for index in 0..1000000usize {
            if let Some(_) = map.read().get(&index) {
                continue;
            }
        }
    });
}

#[bench]
fn bench_ascending_insert_transaction_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();

    b.iter(move || {
        for index in 0..10000 {
            let tr = db.begin_write().unwrap();
            {
                let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
                if let Err(e) = table.insert(index, index) {
                    panic!("Bench redb failed, reason: {:?}", e);
                };
            }
            tr.commit().unwrap();
        }

        let tr = db.begin_write().unwrap();
        {
            let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            table.extract_if(|_key, _val| {
                true
            }).unwrap();
        }
        tr.commit().unwrap();
    });
}

#[bench]
fn bench_ascending_insert_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();

    b.iter(move || {
        let tr = db.begin_write().unwrap();
        for index in 0..10000 {
            let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            if let Err(e) = table.insert(index, index) {
                panic!("Bench redb failed, reason: {:?}", e);
            };
        }
        tr.commit().unwrap();

        let tr = db.begin_write().unwrap();
        {
            let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            table.extract_if(|_key, _val| {
                true
            }).unwrap();
        }
        tr.commit().unwrap();
    });
}

#[bench]
fn bench_ascending_get_transaction_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();
    let tr = db.begin_write().unwrap();
    for index in 0..1000000 {
        let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
        if let Err(e) = table.insert(index, index) {
            panic!("Bench redb failed, reason: {:?}", e);
        };
    }
    tr.commit().unwrap();

    b.iter(move || {
        for index in 0..1000000 {
            let tr = db.begin_read().unwrap();
            {
                let table: ReadOnlyTable<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
                if let Ok(Some(_)) = table.get(index) {
                    continue;
                } else {
                    panic!("Bench redb failed");
                }
            }
        }
    });
}

#[bench]
fn bench_ascending_get_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();
    let tr = db.begin_write().unwrap();
    for index in 0..1000000 {
        let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
        if let Err(e) = table.insert(index, index) {
            panic!("Bench redb failed, reason: {:?}", e);
        };
    }
    tr.commit().unwrap();

    b.iter(move || {
        let tr = db.begin_read().unwrap();
        for index in 0..1000000 {
            let table: ReadOnlyTable<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            if let Ok(Some(_)) = table.get(index) {
                continue;
            } else {
                panic!("Bench redb failed");
            }
        }
    });
}

#[bench]
fn bench_ascending_remove_transaction_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();

    b.iter(move || {
        for index in 0..10000 {
            let tr = db.begin_write().unwrap();
            {
                let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
                if let Err(e) = table.insert(index, index) {
                    panic!("Bench redb failed, reason: {:?}", e);
                };
            }
            tr.commit().unwrap();
        }

        for index in 0..10000 {
            let tr = db.begin_write().unwrap();
            {
                let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
                if let Err(e) = table.remove(index) {
                    panic!("Bench redb failed, reason: {:?}", e);
                };
            }
            tr.commit().unwrap();
        }
    });
}

#[bench]
fn bench_ascending_remove_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();

    b.iter(move || {
        let tr = db.begin_write().unwrap();
        for index in 0..100000 {
            let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            if let Err(e) = table.insert(index, index) {
                panic!("Bench redb failed, reason: {:?}", e);
            };
        }
        tr.commit().unwrap();

        let tr = db.begin_write().unwrap();
        for index in 0..100000 {
            let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
            if let Err(e) = table.remove(index) {
                panic!("Bench redb failed, reason: {:?}", e);
            };
        }
        tr.commit().unwrap();
    });
}

#[bench]
fn bench_ascending_iterator_redb(b: &mut Bencher) {
    let db = Builder::new().create_with_backend(InMemoryBackend::new()).unwrap();
    let tr = db.begin_write().unwrap();
    for index in 0..1000000 {
        let mut table: Table<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
        if let Err(e) = table.insert(index, index) {
            panic!("Bench redb failed, reason: {:?}", e);
        };
    }
    tr.commit().unwrap();

    b.iter(move || {
        let tr = db.begin_read().unwrap();
        let table: ReadOnlyTable<u64, u64> = tr.open_table(TableDefinition::new("test")).unwrap();
        let mut count = 0;
        if let Ok(mut range) = table.iter() {
            while let Some(_) = range.next() {
                count += 1;
            }
            assert_eq!(count, 1000000);
        } else {
            panic!("Bench redb failed");
        }
    });
}

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
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);
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

#[bench]
fn bench_ascending_insert_tree_by_concurrency(b: &mut Bencher) {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(32);

    b.iter(move || {
        map.clear(None);
        let map0 = map.clone();
        let map1 = map.clone();
        let map2 = map.clone();
        let map3 = map.clone();

        let join0 = thread::spawn(move || {
            for index in 0..25000 {
                if let Err(e) = map0.upsert(index, index, None) {
                    println!("Test insert tree failed, reason: {:?}", e);
                    break;
                }
            }
        });
        let join1 = thread::spawn(move || {
            for index in 25000..50000 {
                if let Err(e) = map1.upsert(index, index, None) {
                    println!("Test insert tree failed, reason: {:?}", e);
                    break;
                }
            }
        });
        let join2 = thread::spawn(move || {
            for index in 50000..75000 {
                if let Err(e) = map2.upsert(index, index, None) {
                    println!("Test insert tree failed, reason: {:?}", e);
                    break;
                }
            }
        });
        let join3 = thread::spawn(move || {
            for index in 75000..100000 {
                if let Err(e) = map3.upsert(index, index, None) {
                    println!("Test insert tree failed, reason: {:?}", e);
                    break;
                }
            }
        });
        join0.join();
        join1.join();
        join2.join();
        join3.join();

        assert_eq!(map.b_factor(), 32);
        assert_eq!(map.len(), 100000);
        assert_eq!(map.depth(), 3);
        assert_eq!(*map.min_key().unwrap().key(), 0);
        assert_eq!(*map.max_key().unwrap().key(), 99999);
    });
}

