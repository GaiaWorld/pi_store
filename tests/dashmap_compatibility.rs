//! `dashmap 6.2` 的有界并发兼容专项。
//!
//! 本库在虚拟页管理器和块设备注册表中同时使用只读保护、可写保护、条目接口和迭代器。
//! 历史 `test_dashmap_bug` 会无限循环并睡眠约 31.7 年，只能用于人工诊断，不能充当自动回归。
//! 本目标把相同的读写竞争约束在独立子进程中，并由父进程施加 30 秒硬截止：即使依赖内部
//! 出现死锁，整套验收也会得到确定失败，而不是永久挂起。

use std::{
    env,
    process::Command,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use dashmap::{mapref::entry::Entry, DashMap};

const CHILD_ENV: &str = "PI_STORE_DASHMAP_COMPAT_CHILD";
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
const KEY_COUNT: usize = 1_024;
const WRITE_ROUNDS: usize = 32;
const READER_COUNT: usize = 2;
const WRITER_COUNT: usize = 2;

/// 验证 `dashmap 6.2.1` 与本库实际使用的 API 及受控并发方式兼容。
///
/// 父进程只负责隔离和截止；子进程才执行真正的共享映射读写。这样像在试验室外再加一道
/// 断路器：被测锁即使卡死，也只能卡住可终止的子进程，不能无限占住整个测试流水线。
#[test]
fn test_dashmap_6_2_compatibility() {
    if env::var_os(CHILD_ENV).is_some() {
        run_bounded_concurrent_api_matrix();
        return;
    }

    let executable = env::current_exe().expect("无法取得当前 DashMap 专项测试程序路径");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("test_dashmap_6_2_compatibility")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_ENV, "1")
        .spawn()
        .expect("无法启动隔离的 DashMap 兼容测试子进程");

    let deadline = Instant::now() + CHILD_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("无法查询 DashMap 测试子进程状态")
        {
            assert!(status.success(), "DashMap 兼容测试子进程失败: {status}");
            break;
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("DashMap 兼容测试超过 {CHILD_TIMEOUT:?}，疑似发生持续阻塞或死锁");
        }

        thread::sleep(Duration::from_millis(10));
    }
}

/// 在一个真实 `DashMap` 上覆盖本库使用的读、写、条目和迭代操作。
fn run_bounded_concurrent_api_matrix() {
    let map = Arc::new(DashMap::<usize, usize>::new());
    for key in 0..KEY_COUNT {
        assert_eq!(map.insert(key, 0), None);
    }
    assert_eq!(map.len(), KEY_COUNT);

    // 两个写线程各自拥有奇数或偶数键，因此最终值可以精确计算；两个读线程会同时持有一组
    // 只读保护，重现历史测试真正关心的“跨分片读保护与写保护竞争”，但总量始终有界。
    let start = Arc::new(Barrier::new(WRITER_COUNT + READER_COUNT + 1));
    let mut joins = Vec::with_capacity(WRITER_COUNT + READER_COUNT);

    for writer_id in 0..WRITER_COUNT {
        let map = map.clone();
        let start = start.clone();
        joins.push(thread::spawn(move || {
            start.wait();
            for round in 0..WRITE_ROUNDS {
                for key in (writer_id..KEY_COUNT).step_by(WRITER_COUNT) {
                    if round % 2 == 0 {
                        match map.entry(key) {
                            Entry::Occupied(mut occupied) => *occupied.get_mut() += 1,
                            Entry::Vacant(_) => panic!("初始化后的键不应在写入期间消失: {key}"),
                        }
                    } else {
                        *map.get_mut(&key).expect("初始化后的键必须存在") += 1;
                    }
                }
            }
        }));
    }

    for reader_id in 0..READER_COUNT {
        let map = map.clone();
        let start = start.clone();
        joins.push(thread::spawn(move || {
            const WINDOW: usize = 64;

            start.wait();
            for round in 0..WRITE_ROUNDS {
                let begin = (round * 31 + reader_id * 17) % (KEY_COUNT - WINDOW);
                let guards: Vec<_> = (begin..begin + WINDOW)
                    .map(|key| map.get(&key).expect("并发只更新值，不得删除键"))
                    .collect();

                assert!(guards.iter().all(|guard| **guard <= WRITE_ROUNDS));
                assert_eq!(map.len(), KEY_COUNT);
            }
        }));
    }

    start.wait();
    for join in joins {
        join.join().expect("DashMap 受控并发工作线程发生 panic");
    }

    for key in 0..KEY_COUNT {
        assert_eq!(*map.get(&key).expect("写入结束后键必须存在"), WRITE_ROUNDS);
    }

    // 顺序阶段覆盖生产代码还会使用的 Vacant/Occupied、迭代、包含判断和移除语义。
    for key in 0..32 {
        match map.entry(key) {
            Entry::Occupied(mut occupied) => *occupied.get_mut() += 1,
            Entry::Vacant(_) => panic!("既有键必须进入 Occupied 分支"),
        }

        match map.entry(KEY_COUNT + key) {
            Entry::Vacant(vacant) => {
                vacant.insert(key);
            }
            Entry::Occupied(_) => panic!("新键必须进入 Vacant 分支"),
        }
    }

    assert_eq!(map.len(), KEY_COUNT + 32);
    assert!((0..KEY_COUNT + 32).all(|key| map.contains_key(&key)));

    let iterated = map.iter().count();
    assert_eq!(iterated, KEY_COUNT + 32);

    for key in 0..32 {
        assert_eq!(map.remove(&(KEY_COUNT + key)), Some((KEY_COUNT + key, key)));
    }
    assert_eq!(map.len(), KEY_COUNT);
}
