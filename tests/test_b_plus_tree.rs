use std::thread;
use std::time::{Duration, Instant};

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use pi_store::free_lock::b_plus_tree::{CowBtreeMap, KeyIterator, KVPairIterator};

use redb::{ReadableTable, Builder, StorageBackend, TableDefinition, Table, ReadOnlyTable, Durability};

#[test]
fn test_redb_delay_commit() {
    //初始化数据库，并清空表
    let now = Instant::now();
    let mut db = Builder::new()
        .set_cache_size(16 * 1024 * 1024)
        .create("./redb/db.dat")
        .unwrap();
    println!("======> load db finish, time: {:?}", now.elapsed());

    //测试延迟提交，被事务打开的表必须全部关闭后，事务提交或关闭时才会完整释放
    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::None); //采用非持久化的提交
    for index in 0..100000 {
        let mut table: Table<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        let _ = table.insert(index, index).unwrap();
    }
    tr.commit().unwrap();
    println!("======> write finish, time: {:?}", now.elapsed());

    //验证上一个写事务是否已提交，被事务打开的表必须全部关闭后，事务提交或关闭时才会完整释放
    let now = Instant::now();
    let tr = db.begin_read().unwrap();
    {
        let table: ReadOnlyTable<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        for index in 0..100000 {
            if let Ok(Some(val)) = table.get(index) {
                let value = val.value();
                if value != index {
                    panic!("!!!!!!> read failed, index: {:?}, value: {:?}, reason: value not match", index, value);
                }
            } else {
                panic!("!!!!!!> read failed, index: {:?}, reason: invalid last write tranasaction", index);
            }
        }
    }
    tr.close();
    println!("======> read finish, time: {:?}", now.elapsed());

    //延迟持久化的提交
    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::Immediate);
    tr.commit().unwrap();
    println!("======> durability commit finish, time: {:?}", now.elapsed());

    //重新启动数据库，验证数据是否已持久化，有任何未关闭的事务时，数据库将无法关闭并释放
    drop(db);
    let now = Instant::now();
    let mut db = Builder::new()
        .set_cache_size(16 * 1024 * 1024)
        .create("./redb/db.dat")
        .unwrap();
    println!("======> reload db finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let tr = db.begin_read().unwrap();
    {
        let table: ReadOnlyTable<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        for index in 0..100000 {
            //验证上一个数据库实例的写事务是否已持久化，被事务打开的表必须全部关闭后，事务提交或关闭时才会完整释放
            if let Ok(Some(val)) = table.get(index) {
                let value = val.value();
                if value != index {
                    panic!("!!!!!!> check failed, index: {:?}, value: {:?}, reason: value not match", index, value);
                }
            } else {
                panic!("!!!!!!> check failed, index: {:?}, reason: invalid last write tranasaction", index);
            }
        }
    }
    tr.close();
    println!("======> check finish, time: {:?}", now.elapsed());

    //压缩数据库，没有任何已打开事务时压缩才会成功
    let now = Instant::now();
    let _ = db.compact().unwrap();
    println!("======> compact db finish, time: {:?}", now.elapsed());
}

#[test]
fn test_redb_savepoint() {
    //初始化数据库，并清空表
    let now = Instant::now();
    let mut db = Builder::new()
        .set_cache_size(16 * 1024 * 1024)
        .create("./redb/db.dat")
        .unwrap();
    println!("======> load db finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::None); //采用非持久化的提交
    for index in 0..100000 {
        let mut table: Table<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        let _ = table.insert(index, index).unwrap();
    }
    tr.commit().unwrap();
    println!("======> write finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::Immediate);
    tr.commit().unwrap();
    println!("======> durability commit finish, time: {:?}", now.elapsed());

    //执行持久化快照，有任何未关闭的写事务，数据库将无法快照
    let now = Instant::now();
    let mut tr = db.begin_write().unwrap(); //默认Durability::Immediate
    let savepoint0 = tr.persistent_savepoint().unwrap();
    tr.commit().unwrap();
    println!("======> save finish, savepoint: {:?}, time: {:?}", savepoint0, now.elapsed());

    //重新启动数据库，有任何未关闭的事务时，数据库将无法关闭并释放
    drop(db);
    let now = Instant::now();
    let mut db = Builder::new()
        .set_cache_size(16 * 1024 * 1024)
        .create("./redb/db.dat")
        .unwrap();
    println!("======> reload db finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::None); //采用非持久化的提交
    for index in 100000..200000 {
        let mut table: Table<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        let _ = table.insert(index, index).unwrap();
    }
    tr.commit().unwrap();
    println!("======> write finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    tr.set_durability(Durability::Immediate);
    tr.commit().unwrap();
    println!("======> durability commit finish, time: {:?}", now.elapsed());

    let now = Instant::now();
    let mut tr = db.begin_write().unwrap(); //默认Durability::Immediate
    let savepoint1 = tr.persistent_savepoint().unwrap();
    tr.commit().unwrap();
    println!("======> save other finish, savepoint: {:?}, time: {:?}", savepoint1, now.elapsed());

    //切换快照，有任何未关闭的事务时，数据库将无法切换快照
    //注：切换到旧快照的数据库后，将不包括最近的快照
    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    let mut savepoints = tr.list_persistent_savepoints().unwrap();
    while let Some(savepoint) = savepoints.next() {
        println!("======> exist savepoint, {:?}", savepoint);
    }
    let savepoint = tr.get_persistent_savepoint(savepoint0).unwrap();
    tr.restore_savepoint(&savepoint).unwrap();
    tr.commit().unwrap();
    println!("======> restore finish, savepoint: {:?}, time: {:?}", savepoint0, now.elapsed());

    //验证切换的快照，被事务打开的表必须全部关闭后，事务提交或关闭时才会完整释放
    let now = Instant::now();
    let tr = db.begin_read().unwrap();
    {
        let table: ReadOnlyTable<u32, u32> = tr.open_table(TableDefinition::new("test001")).unwrap();
        for index in 0..100000 {
            if let Ok(Some(val)) = table.get(index) {
                let value = val.value();
                assert_eq!(value, index);
            } else {
                panic!("!!!!!!> read failed, index: {:?}, reason: invalid last write tranasaction", index);
            }
        }
        assert!(table.get(100000).ok().unwrap().is_none());
    }
    tr.close();
    println!("======> read finish, time: {:?}", now.elapsed());

    //删除所有快照
    let now = Instant::now();
    let mut tr = db.begin_write().unwrap();
    let mut savepoints = tr.list_persistent_savepoints().unwrap();
    while let Some(savepoint) = savepoints.next() {
        let _ = tr.delete_persistent_savepoint(savepoint).unwrap();
    }
    tr.commit().unwrap();
    println!("======> delete savepoint finish, time: {:?}", now.elapsed());

    //压缩数据库，没有任何已打开事务时压缩才会成功
    let now = Instant::now();
    let _ = db.compact().unwrap();
    println!("======> compact db finish, time: {:?}", now.elapsed());
}

#[test]
fn test_create_tree() {
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);

    let pairs = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];
    let pairs_len = pairs.len() as u64;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::with_pairs(5, pairs);
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), pairs_len);
    assert_eq!(map.depth(), 1);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 6);
}

#[test]
fn test_insert_tree() {
    //顺序插入
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //倒序插入
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //随机插入
    for _ in 0..100 {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
        for index in vec {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Test insert tree failed, reason: invalid upsert result");
            }
        }
        assert_eq!(map.b_factor(), 5);
        assert_eq!(map.len(), 1000);
        assert_eq!(*map.min_key().unwrap().key(), 0);
        assert_eq!(*map.max_key().unwrap().key(), 999);
    }
}

#[test]
fn test_update_tree() {
    //顺序插入，并顺序更新
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 0..1000 {
        if let Ok(Some(old_value)) = map.upsert(index, index * 1000, None) {
            if old_value != index {
                panic!("Test update tree failed, real_old_value: {}, old_value: {}, reason: invalid old value", index, old_value);
            }

            continue;
        } else {
            panic!("Test update tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //倒序插入，并倒序更新
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    index = 999;
    for _ in 0..1000 {
        if let Ok(Some(old_value)) = map.upsert(index, index * 1000, None) {
            if old_value != index {
                panic!("Test update tree failed, real_old_value: {}, old_value: {}, reason: invalid old value", index, old_value);
            }

            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test update tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //随机插入，并随机更新
    for _ in 0..100 {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
        for index in vec {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Test insert tree failed, reason: invalid upsert result");
            }
        }
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        for index in vec {
            if let Ok(Some(old_value)) = map.upsert(index, index * 1000, None) {
                if old_value != index {
                    panic!("Test update tree failed, real_old_value: {}, old_value: {}, reason: invalid old value", index, old_value);
                }

                continue;
            } else {
                panic!("Test update tree failed, reason: invalid upsert result");
            }
        }
        assert_eq!(map.b_factor(), 5);
        assert_eq!(map.len(), 1000);
        assert_eq!(*map.min_key().unwrap().key(), 0);
        assert_eq!(*map.max_key().unwrap().key(), 999);
    }
}

#[test]
fn test_delete_tree() {
    //顺序插入并删除
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 0..1000 {
        if let Ok(Some(_)) = map.remove(&index, None) {
            continue;
        } else {
            panic!("Test remove tree failed, reason: invalid remove result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);

    //顺序插入，并倒序删除
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut index = 999;
    for _ in 0..1000 {
        if let Ok(Some(_)) = map.remove(&index, None) {
            index = index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test remove tree failed, reason: invalid remove result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);

    //随机插入，并随机删除
    for _ in 0..100 {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
        for index in vec {
            if let Ok(None) = map.upsert(index, index, None) {
                continue;
            } else {
                panic!("Test insert tree failed, reason: invalid upsert result");
            }
        }
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        for index in vec {
            if let Ok(Some(_)) = map.remove(&index, None) {
                continue;
            } else {
                panic!("Test remove tree failed, reason: invalid remove result");
            }
        }
        assert_eq!(map.b_factor(), 5);
        assert_eq!(map.len(), 0);
        assert_eq!(map.depth(), 0);
        assert_eq!(map.min_key().is_none(), true);
        assert_eq!(map.max_key().is_none(), true);
    }
}

#[test]
fn test_get_tree() {
    //顺序插入，并顺序获取
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);
    for index in 0..1000 {
        if let Some(val) = map.get(&index) {
            let value = *val.value();
            assert_eq!(value, index);
        } else {
            panic!("Test get tree failed, reason: invalid get result");
        }
    }

    //倒序插入，并倒序获取
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);
    let index = 999;
    for _ in 0..1000 {
        if let Some(val) = map.get(&index) {
            let value = *val.value();
            assert_eq!(value, index);
        } else {
            panic!("Test get tree failed, reason: invalid get result");
        }
    }

    //随机插入，随机更新和随机获取
    let mut rng = SmallRng::from_entropy();
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);

    for _ in 0..100 {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        for index in vec {
            let _ = map.upsert(index, index * 1000, None);
        }
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        for index in vec {
            if let Ok(Some(old_value)) = map.upsert(index, index, None) {
                if old_value != index * 1000 {
                    panic!("Test update tree failed, real_old_value: {}, old_value: {}, reason: invalid old value", index, old_value);
                }

                continue;
            } else {
                panic!("Test update tree failed, reason: invalid upsert result");
            }
        }
        assert_eq!(map.b_factor(), 5);
        assert_eq!(map.len(), 1000);
        assert_eq!(*map.min_key().unwrap().key(), 0);
        assert_eq!(*map.max_key().unwrap().key(), 999);
    }
    let mut vec: Vec<usize> = (0..1000).collect();
    vec.shuffle(&mut rng);
    for index in vec {
        if let Some(val) = map.get(&index) {
            let value = *val.value();
            assert_eq!(value, index);
        } else {
            panic!("Test get tree failed, reason: invalid get result");
        }
    }

    //随机插入，并随机删除和随机获取
    for _ in 0..100 {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..1000).collect();
        vec.shuffle(&mut rng);
        let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
        for index in vec {
            let _ = map.upsert(index, index, None);
        }
        let mut vec: Vec<usize> = (0..500).collect();
        vec.shuffle(&mut rng);
        for index in vec {
            if let Ok(Some(_)) = map.remove(&index, None) {
                continue;
            } else {
                panic!("Test remove tree failed, reason: invalid remove result");
            }
        }
        assert_eq!(map.b_factor(), 5);
        assert_eq!(map.len(), 500);
        assert_eq!(map.depth(), 4);
        assert_eq!(*map.min_key().unwrap().key(), 500);
        assert_eq!(*map.max_key().unwrap().key(), 999);
    }
    let mut vec: Vec<usize> = (500..1000).collect();
    vec.shuffle(&mut rng);
    for index in vec {
        if let Some(val) = map.get(&index) {
            let value = *val.value();
            assert_eq!(value, index);
        } else {
            panic!("Test get tree failed, reason: invalid get result");
        }
    }
}

#[test]
fn test_keys_ascending_tree() {
    //顺序插入，并顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(None, false);
    if let KeyIterator::Ascending(keys) = iterator {
        let mut index = 0;
        for key in keys {
            assert_eq!(*key, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从头开始顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&0), false);
    if let KeyIterator::Ascending(keys) = iterator {
        let mut index = 0;
        for key in keys {
            assert_eq!(*key, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从指定关键字开始顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&700), false);
    if let KeyIterator::Ascending(keys) = iterator {
        let mut index = 700;
        for key in keys {
            assert_eq!(*key, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从尾开始顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&999), false);
    if let KeyIterator::Ascending(mut keys) = iterator {
        assert_eq!(*keys.next().unwrap(), 999);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从不存在的最小关键字开始顺序迭代关键字
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 2000..3000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&-1), false);
    if let KeyIterator::Ascending(mut keys) = iterator {
        let mut index = 0isize;
        for key in keys {
            assert_eq!(*key, index);
            index += 1;
            if index == 1000 {
                index = 2000;
            }
        }
        assert_eq!(index, 3000);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.keys(Some(&1500), false);
    if let KeyIterator::Ascending(mut keys) = iterator {
        let mut index = 2000isize;
        for key in keys {
            assert_eq!(*key, index);
            index += 1;
        }
        assert_eq!(index, 3000);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.keys(Some(&3000), false);
    if let KeyIterator::Ascending(mut keys) = iterator {
        assert_eq!(keys.next().is_none(), true);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 2000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 2999);
}

#[test]
fn test_keys_descending_tree() {
    //倒序插入，并倒序迭代关键字
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(None, true);
    if let KeyIterator::Descending(keys) = iterator {
        let mut index = 999isize;
        for key in keys {
            assert_eq!(*key, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //倒序插入，并从头开始倒序迭代关键字
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&0), true);
    if let KeyIterator::Descending(mut keys) = iterator {
        assert_eq!(*keys.next().unwrap(), 0);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从指定关键字开始倒序迭代关键字
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&700), true);
    if let KeyIterator::Descending(keys) = iterator {
        let mut index = 700isize;
        for key in keys {
            assert_eq!(*key, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从尾开始倒序迭代关键字
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&999), true);
    if let KeyIterator::Descending(mut keys) = iterator {
        let mut index = 999isize;
        for key in keys {
            assert_eq!(*key, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从不存在的最小关键字开始倒序迭代关键字
    let mut index = 999;
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut index = 2999;
    for _ in 2000..3000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&-1), true);
    if let KeyIterator::Descending(mut keys) = iterator {
        assert_eq!(keys.next().is_none(), true);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.keys(Some(&1500), true);
    if let KeyIterator::Descending(mut keys) = iterator {
        let mut index = 999isize;
        for key in keys {
            assert_eq!(*key, index);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.keys(Some(&3000), true);
    if let KeyIterator::Descending(mut keys) = iterator {
        let mut index = 2999isize;
        for key in keys {
            assert_eq!(*key, index);
            index -= 1;
            if index == 1999 {
                index = 999;
            }
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 2000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 2999);
}

#[test]
fn test_values_ascending_tree() {
    //顺序插入，并顺序迭代键值对
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(None, false);
    if let KVPairIterator::Ascending(values) = iterator {
        let mut index = 0;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从头开始顺序迭代键值对
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&0), false);
    if let KVPairIterator::Ascending(values) = iterator {
        let mut index = 0;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);


    //顺序插入，并从指定关键字开始顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&700), false);
    if let KVPairIterator::Ascending(values) = iterator {
        let mut index = 700;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index += 1;
        }
        assert_eq!(index, 1000);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从尾开始顺序迭代关键字
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&999), false);
    if let KVPairIterator::Ascending(mut values) = iterator {
        assert_eq!(values.next().unwrap(), (&999, &999));
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从不存在的最小关键字开始顺序迭代关键字
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    for index in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 2000..3000 {
        if let Ok(None) = map.upsert(index, index, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&-1), false);
    if let KVPairIterator::Ascending(mut values) = iterator {
        let mut index = 0isize;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index += 1;
            if index == 1000 {
                index = 2000;
            }
        }
        assert_eq!(index, 3000);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    let mut iterator = map.values(Some(&1500), false);
    if let KVPairIterator::Ascending(mut values) = iterator {
        let mut index = 2000isize;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index += 1;
        }
        assert_eq!(index, 3000);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    let mut iterator = map.values(Some(&3000), false);
    if let KVPairIterator::Ascending(mut values) = iterator {
        assert_eq!(values.next().is_none(), true);
    } else {
        panic!("Iterate key value pair tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 2000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 2999);
}

#[test]
fn test_values_descending_tree() {
    //倒序插入，并倒序迭代键值对
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(None, true);
    if let KVPairIterator::Descending(values) = iterator {
        let mut index = 999isize;
        for (key, value) in values {
            assert_eq!(*key, index as usize);
            assert_eq!(*value, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //倒序插入，并从头开始倒序迭代键值对
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&0), true);
    if let KVPairIterator::Descending(mut values) = iterator {
        assert_eq!(values.next().unwrap(), (&0, &0));
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从指定关键字开始顺序迭代键值对
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&700), true);
    if let KVPairIterator::Descending(values) = iterator {
        let mut index = 700isize;
        for (key, value) in values {
            assert_eq!(*key, index as usize);
            assert_eq!(*value, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从尾开始顺序迭代键值对
    let mut index = 999;
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&999), true);
    if let KVPairIterator::Descending(mut values) = iterator {
        let mut index = 999isize;
        for (key, value) in values {
            assert_eq!(*key, index as usize);
            assert_eq!(*value, index as usize);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 1000);
    assert_eq!(map.depth(), 5);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 999);

    //顺序插入，并从不存在的最小关键字开始顺序迭代键值对
    let mut index = 999;
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    for _ in 0..1000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut index = 2999;
    for _ in 2000..3000 {
        if let Ok(None) = map.upsert(index, index, None) {
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&-1), true);
    if let KVPairIterator::Descending(mut values) = iterator {
        assert_eq!(values.next().is_none(), true);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.values(Some(&1500), true);
    if let KVPairIterator::Descending(mut values) = iterator {
        let mut index = 999isize;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index -= 1;
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    let mut iterator = map.values(Some(&3000), true);
    if let KVPairIterator::Descending(mut values) = iterator {
        let mut index = 2999isize;
        for (key, value) in values {
            assert_eq!(*key, index);
            assert_eq!(*value, index);
            index -= 1;
            if index == 1999 {
                index = 999;
            }
        }
        assert_eq!(index, -1);
    } else {
        panic!("Iterate keys tree failed, reason: invalid iterator");
    }
    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 2000);
    assert_eq!(map.depth(), 6);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 2999);
}

#[test]
fn test_insert_tree_by_by_concurrency() {
    //顺序插入
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
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
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 9);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //倒序插入
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 9);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //随机插入
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
}

#[test]
fn test_update_tree_by_by_concurrency() {
    //顺序插入，顺序更新
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
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
        for index in 0..25000 {
            if let Err(e) = map0.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
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
        for index in 25000..50000 {
            if let Err(e) = map1.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
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
        for index in 50000..75000 {
            if let Err(e) = map2.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
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
        for index in 75000..100000 {
            if let Err(e) = map3.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 9);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
    assert_eq!(*map.get(&*map.min_key().unwrap().key()).unwrap().value(), 0);
    assert_eq!(*map.get(&*map.max_key().unwrap().key()).unwrap().value(), 99999000);

    //倒序插入，倒序更新
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }

        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }

        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }

        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }

        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 9);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
    assert_eq!(*map.get(&*map.min_key().unwrap().key()).unwrap().value(), 0);
    assert_eq!(*map.get(&*map.max_key().unwrap().key()).unwrap().value(), 99999000);

    //随机插入，随机更新
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }

        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }

        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }

        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }

        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index * 1000, None) {
                println!("Test update tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
    assert_eq!(*map.get(&*map.min_key().unwrap().key()).unwrap().value(), 0);
    assert_eq!(*map.get(&*map.max_key().unwrap().key()).unwrap().value(), 99999000);
}

#[test]
fn test_delete_tree_by_by_concurrency() {
    //顺序插入，顺序删除
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
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
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        for index in 0..25000 {
            if let Err(e) = map0.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        for index in 25000..50000 {
            if let Err(e) = map1.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        for index in 50000..75000 {
            if let Err(e) = map2.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        for index in 75000..100000 {
            if let Err(e) = map3.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);

    //倒序插入，倒序删除
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);

    //随机插入，随机删除
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);
}

#[test]
fn test_get_tree_by_by_concurrency() {
    //顺序插入，顺序获取
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
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
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        for index in 0..25000 {
            if let Some(val) = map0.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        for index in 25000..50000 {
            if let Some(val) = map1.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        for index in 50000..75000 {
            if let Some(val) = map2.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        for index in 75000..100000 {
            if let Some(val) = map3.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //倒序插入，倒序获取
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Some(val) = map0.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Some(val) = map1.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Some(val) = map2.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Some(val) = map3.get(&index) {
                if *val.value() != index {
                    println!("Test get tree failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //随机插入，随机删除和随机获取
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Some(val) = map0.get(&index) {
                if *val.value() != index {
                    println!("Test get tree value failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Some(val) = map1.get(&index) {
                if *val.value() != index {
                    println!("Test get tree value failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Some(val) = map2.get(&index) {
                if *val.value() != index {
                    println!("Test get tree value failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Some(val) = map3.get(&index) {
                if *val.value() != index {
                    println!("Test get tree value failed, key: {:?}", index);
                    break;
                }
            } else {
                println!("Test get tree is empty, key: {:?}", index);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.remove(&index, None) {
                println!("Test delete tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if map0.get(&index).is_some() {
                println!("Test get tree is valuable, key: {:?}", index);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if map1.get(&index).is_some() {
                println!("Test get tree is valuable, key: {:?}", index);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if map2.get(&index).is_some() {
                println!("Test get tree is valuable, key: {:?}", index);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if map3.get(&index).is_some() {
                println!("Test get tree is valuable, key: {:?}", index);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);
}

#[test]
fn test_keys_ascending_tree_by_by_concurrency() {
    //随机插入
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(None, false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(None, false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(None, false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(None, false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从头开始顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&0), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&0), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&0), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&0), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从指定关键字开始顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&1), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 1;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&10000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 10000;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&90000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 90000;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&99999), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从尾部开始顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&99999), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&99999), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&99999), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&99999), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最小关键字开始顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&-1), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&-1), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&-1), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&-1), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最大关键字开始顺序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&100000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&100000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&100000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&100000), false);
        if let KeyIterator::Ascending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
}

#[test]
fn test_keys_descending_tree_by_by_concurrency() {
    //随机插入
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(None, true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(None, true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(None, true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(None, true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从头开始倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&0), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&0), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&0), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&0), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从指定关键字开始倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&1), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 1;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&10000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 10000;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&90000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 90000;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&99999), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从尾部开始倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&99999), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&99999), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&99999), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&99999), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 99999;
            for key in keys {
                assert_eq!(*key, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最小关键字开始倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&-1), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&-1), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&-1), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&-1), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 0;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最大关键字开始倒序迭代关键字
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.keys(Some(&100000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.keys(Some(&100000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.keys(Some(&100000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.keys(Some(&100000), true);
        if let KeyIterator::Descending(keys) = iterator {
            let mut index = 100000;
            for key in keys {
                index -= 1;
                assert_eq!(*key, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate keys tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
}

#[test]
fn test_values_ascending_tree_by_by_concurrency() {
    //随机插入
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(None, false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(None, false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(None, false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(None, false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从头开始顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&0), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&0), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&0), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&0), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从指定关键字开始顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&1), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 1;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&10000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 10000;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&90000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 90000;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&99999), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从尾部开始顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&99999), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&99999), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&99999), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&99999), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最小关键字开始顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&-1), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&-1), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&-1), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&-1), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 100000);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最大关键字开始顺序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&100000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&100000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&100000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&100000), false);
        if let KVPairIterator::Ascending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index += 1;
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
}

#[test]
fn test_values_descending_tree_by_by_concurrency() {
    //随机插入
    let map: CowBtreeMap<isize, isize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<isize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, None) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(None, true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(None, true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(None, true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(None, true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从头开始倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&0), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&0), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&0), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&0), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从指定关键字开始倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&1), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 1;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&10000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 10000;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&90000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 90000;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&99999), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从尾部开始倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&99999), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&99999), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&99999), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&99999), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 99999;
            for (key, value) in values {
                assert_eq!(*key, index);
                assert_eq!(*value, index);
                index -= 1;
            }
            assert_eq!(index, -1);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最小关键字开始倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&-1), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&-1), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&-1), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&-1), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 0;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    //从不存在的最大关键字开始倒序迭代键值对
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let join0 = thread::spawn(move || {
        let mut iterator = map0.values(Some(&100000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join1 = thread::spawn(move || {
        let mut iterator = map1.values(Some(&100000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join2 = thread::spawn(move || {
        let mut iterator = map2.values(Some(&100000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let join3 = thread::spawn(move || {
        let mut iterator = map3.values(Some(&100000), true);
        if let KVPairIterator::Descending(values) = iterator {
            let mut index = 100000;
            for (key, value) in values {
                index -= 1;
                assert_eq!(*key, index);
                assert_eq!(*value, index);
            }
            assert_eq!(index, 0);
        } else {
            panic!("Iterate values tree failed, reason: invalid iterator");
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(map.depth(), 8);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);
}

#[test]
fn test_insert_get_clear_tree_by_by_concurrency() {
    //顺序插入，顺序查询
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let map4 = map.clone();
    let map5 = map.clone();
    let map6 = map.clone();
    let map7 = map.clone();

    let join0 = thread::spawn(move || {
        for index in 0..25000 {
            if let Err(e) = map0.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        for index in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        for index in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        for index in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join4 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 0;
        for index in start..25000 {
            if let Some(val) = map4.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join5 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 25000;
        for index in start..50000 {
            if let Some(val) = map5.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join6 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 50000;
        for index in start..75000 {
            if let Some(val) = map6.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join7 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 75000;
        for index in start..100000 {
            if let Some(val) = map7.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();
    let _ = join4.join();
    let _ = join5.join();
    let _ = join6.join();
    let _ = join7.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //倒序插入，顺序查询
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let map4 = map.clone();
    let map5 = map.clone();
    let map6 = map.clone();
    let map7 = map.clone();

    let join0 = thread::spawn(move || {
        let mut index = 24999;
        for _ in 0..25000 {
            if let Err(e) = map0.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join1 = thread::spawn(move || {
        let mut index = 49999;
        for _ in 25000..50000 {
            if let Err(e) = map1.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join2 = thread::spawn(move || {
        let mut index = 74999;
        for _ in 50000..75000 {
            if let Err(e) = map2.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join3 = thread::spawn(move || {
        let mut index = 99999;
        for _ in 75000..100000 {
            if let Err(e) = map3.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
            index =  index
                .checked_sub(1)
                .unwrap_or(0);
        }
    });
    let join4 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 0;
        for index in start..25000 {
            if let Some(val) = map4.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join5 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 25000;
        for index in start..50000 {
            if let Some(val) = map5.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join6 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 50000;
        for index in start..75000 {
            if let Some(val) = map6.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join7 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 75000;
        for index in start..100000 {
            if let Some(val) = map7.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();
    let _ = join4.join();
    let _ = join5.join();
    let _ = join6.join();
    let _ = join7.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    //随机插入
    let map: CowBtreeMap<usize, usize> = CowBtreeMap::empty(5);
    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let map4 = map.clone();
    let map5 = map.clone();
    let map6 = map.clone();
    let map7 = map.clone();

    let join0 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (0..25000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map0.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join1 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (25000..50000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map1.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join2 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (50000..75000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map2.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join3 = thread::spawn(move || {
        let mut rng = SmallRng::from_entropy();
        let mut vec: Vec<usize> = (75000..100000).collect();
        vec.shuffle(&mut rng);

        for index in vec {
            if let Err(e) = map3.upsert(index, index, Some(1000)) {
                println!("Test insert tree failed, reason: {:?}", e);
                break;
            }
        }
    });
    let join4 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 0;
        for index in start..25000 {
            if let Some(val) = map4.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join5 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 25000;
        for index in start..50000 {
            if let Some(val) = map5.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join6 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 50000;
        for index in start..75000 {
            if let Some(val) = map6.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let join7 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 75000;
        for index in start..100000 {
            if let Some(val) = map7.get(&index) {
                if *val.value() == index {
                    count += 1;
                } else {
                    println!("Test insert tree failed, reason: value not match");
                    break;
                }
            } else {
                //从指定关键字开始重试读取
                start = index;
            }
        }
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();
    let _ = join4.join();
    let _ = join5.join();
    let _ = join6.join();
    let _ = join7.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 100000);
    assert_eq!(*map.min_key().unwrap().key(), 0);
    assert_eq!(*map.max_key().unwrap().key(), 99999);

    let map0 = map.clone();
    let map1 = map.clone();
    let map2 = map.clone();
    let map3 = map.clone();
    let map4 = map.clone();
    let map5 = map.clone();
    let map6 = map.clone();
    let map7 = map.clone();
    let join0 = thread::spawn(move || {
        if !map0.clear(Some(1000)) {
            println!("Test clear tree failed");
        }
    });
    let join1 = thread::spawn(move || {
        if !map1.clear(Some(1000)) {
            println!("Test clear tree failed");
        }
    });
    let join2 = thread::spawn(move || {
        if !map2.clear(Some(1000)) {
            println!("Test clear tree failed");
        }
    });
    let join3 = thread::spawn(move || {
        if !map3.clear(Some(1000)) {
            println!("Test clear tree failed");
        }
    });
    let join4 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 0;
        for index in start..25000 {
            if let Some(_) = map4.get(&index) {
                //从指定关键字开始重试读取
                start = index;
            } else {
                count += 1;
            }
        }
        assert_eq!(count, 25000);
    });
    let join5 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 25000;
        for index in start..50000 {
            if let Some(_) = map5.get(&index) {
                //从指定关键字开始重试读取
                start = index;
            } else {
                count += 1;
            }
        }
        assert_eq!(count, 25000);
    });
    let join6 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 50000;
        for index in start..75000 {
            if let Some(_) = map6.get(&index) {
                //从指定关键字开始重试读取
                start = index;
            } else {
                count += 1;
            }
        }
        assert_eq!(count, 25000);
    });
    let join7 = thread::spawn(move || {
        let mut count = 0;
        let mut start = 75000;
        for index in start..100000 {
            if let Some(_) = map7.get(&index) {
                //从指定关键字开始重试读取
                start = index;
            } else {
                count += 1;
            }
        }
        assert_eq!(count, 25000);
    });
    let _ = join0.join();
    let _ = join1.join();
    let _ = join2.join();
    let _ = join3.join();
    let _ = join4.join();
    let _ = join5.join();
    let _ = join6.join();
    let _ = join7.join();

    assert_eq!(map.b_factor(), 5);
    assert_eq!(map.len(), 0);
    assert_eq!(map.depth(), 0);
    assert_eq!(map.min_key().is_none(), true);
    assert_eq!(map.max_key().is_none(), true);
}


