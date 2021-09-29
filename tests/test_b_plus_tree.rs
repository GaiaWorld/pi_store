use rand::{Rng, SeedableRng, seq::SliceRandom, rngs::SmallRng};

use pi_store::free_lock::b_plus_tree::{CowBtreeMap, KeyRefGuard, KeyIterator, KVPairIterator};

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
    let mut index = 999;
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
            map.upsert(index, index * 1000, None);
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
            map.upsert(index, index, None);
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
        if let Ok(None) = map.upsert(index as isize, index as isize, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 2000..3000 {
        if let Ok(None) = map.upsert(index as isize, index as isize, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.keys(Some(&-1), false);
    if let KeyIterator::Ascending(mut keys) = iterator {
        let mut index = 0isize;
        for key in keys {
            assert_eq!(*key, index as isize);
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
            assert_eq!(*key, index as isize);
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

    //顺序插入，并从指定关键字开始顺序迭代关键字
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

    //顺序插入，并从尾开始顺序迭代关键字
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

    //顺序插入，并从不存在的最小关键字开始顺序迭代关键字
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
        if let Ok(None) = map.upsert(index as isize, index as isize, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    for index in 2000..3000 {
        if let Ok(None) = map.upsert(index as isize, index as isize, None) {
            continue;
        } else {
            panic!("Test insert tree failed, reason: invalid upsert result");
        }
    }
    let mut iterator = map.values(Some(&-1), false);
    if let KVPairIterator::Ascending(mut values) = iterator {
        let mut index = 0isize;
        for (key, value) in values {
            assert_eq!(*key, index as isize);
            assert_eq!(*value, index as isize);
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
            assert_eq!(*key, index as isize);
            assert_eq!(*value, index as isize);
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

