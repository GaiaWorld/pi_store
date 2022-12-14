use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use fastcmp::Compare;

use pi_async::prelude::AsyncRuntime;
use pi_async::{
    lock::mutex_lock::Mutex,
    rt::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
};

use pi_hash::XHashMap;
use pi_store::log_store::log_file::{
    read_log_file, read_log_file_block, LogFile, LogMethod, PairLoader,
};
use std::io::ErrorKind;

#[test]
fn test_crc32fast() {
    let mut hasher = Hasher::new();
    hasher.update(&vec![1, 1, 1]);
    hasher.update(&vec![10, 10, 10]);
    hasher.update(&vec![255, 10, 255, 10, 255, 10]);
    let hash = hasher.finalize();
    let mut hasher = Hasher::new();
    hasher.update(&vec![1, 1, 1, 10, 10, 10, 255, 10, 255, 10, 255, 10]);

    assert_eq!(hash, hasher.finalize());
}

#[test]
fn test_fastcmp() {
    let vec0: Vec<u8> = vec![1, 1, 1];
    let vec1: Vec<u8> = vec![1, 1, 1];

    assert!(vec0.feq(&vec1));
}

struct Counter(AtomicUsize, Instant);

impl Drop for Counter {
    fn drop(&mut self) {
        println!(
            "!!!!!!drop counter, count: {:?}, time: {:?}",
            self.0.load(Ordering::Relaxed),
            Instant::now() - self.1
        );
    }
}

#[test]
fn test_log_append() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.commit(uid, true, false, None).await {
                            println!("!!!!!!append log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_remove() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::Remove, key.as_slice(), value);
                        if let Err(e) = log_copy.commit(uid, true, false, None).await {
                            println!("!!!!!!remove log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

struct TestCache {
    is_hidden_remove: bool,
    removed: XHashMap<Vec<u8>, ()>,
    map: BTreeMap<Vec<u8>, Option<String>>,
}

impl PairLoader for TestCache {
    fn is_require(&self, log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        !self.removed.contains_key(key) && !self.map.contains_key(key)
    }

    fn load(
        &mut self,
        log_file: Option<&PathBuf>,
        _method: LogMethod,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) {
        if let Some(value) = value {
            unsafe {
                self.map
                    .insert(key, Some(String::from_utf8_unchecked(value)));
            }
        } else {
            if self.is_hidden_remove {
                //忽略移除的键值对
                self.removed.insert(key, ());
            } else {
                self.map.insert(key, None);
            }
        }
    }
}

impl TestCache {
    pub fn new(is_hidden_remove: bool) -> Self {
        TestCache {
            is_hidden_remove,
            removed: XHashMap::default(),
            map: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[test]
fn test_log_files() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(), "./db/", 8000, 1024 * 1024, None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            }
            Ok(log) => {
                println!("只读文件列表:{:?}", log.all_readable_path());
                println!("第一次整理开始");
                let r = log.split().await;
                println!("第一次分裂文件 r:{:?}", r);
                println!("只读文件列表:{:?}", log.all_readable_path());
                let r = log.collect(1024 * 1024, 32 * 1024, false).await;
                println!("整理返回::{:?}", r);
                println!("只读文件列表:{:?}", log.all_readable_path());
                println!("第一次整理完成");
                println!("第二次整理开始");
                let r = log.split().await;
                println!("第二次分裂文件 r:{:?}", r);
                println!("只读文件列表:{:?}", log.all_readable_path());
                let r = log.collect(1024 * 1024, 32 * 1024, false).await;
                println!("整理返回::{:?}", r);
                println!("只读文件列表:{:?}", log.all_readable_path());
                println!("第二次整理完成");
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_load() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let mut cache = TestCache::new(true);
                let start = Instant::now();
                match log.load(&mut cache, None, 32 * 1024, true).await {
                    Err(e) => {
                        println!("!!!!!!load log failed, e: {:?}", e);
                    },
                    Ok(_) => {
                        println!("!!!!!!load log ok, len: {:?}, time: {:?}", cache.len(), Instant::now() - start);
                    },
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_collect() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(), "./log", 8000, 1024 * 1024, None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            }
            Ok(log) => {
                let start = Instant::now();
                match log.collect(1024 * 1024, 32 * 1024, false).await {
                    Err(e) => {
                        println!("!!!!!!load log failed, e: {:?}", e);
                    }
                    Ok((size, len)) => {
                        println!(
                            "!!!!!!load log ok, size: {:?}, len: {:?}, time: {:?}",
                            size,
                            len,
                            Instant::now() - start
                        );
                    }
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_append_delay_commit() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, false, 1).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_remove_delay_commit() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::Remove, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, false, 1).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_append_delay_commit_by_split() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, true, 1).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_remove_delay_commit_by_split() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::Remove, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, true, 1).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_split() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024,
                            None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let mut count = Arc::new(AtomicUsize::new(0));
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));

                let log_copy = log.clone();
                rt_copy.spawn(rt_copy.alloc(), async move {
                    let mut cache = TestCache::new(true);
                    let start = Instant::now();
                    match log_copy.load(&mut cache, None, 32 * 1024, true).await {
                        Err(e) => {
                            println!("!!!!!!load log failed, e: {:?}", e);
                        },
                        Ok(_) => {
                            println!("!!!!!!load log ok, len: {:?}, time: {:?}", cache.len(), Instant::now() - start);
                        },
                    }
                });

                thread::sleep(Duration::from_millis(5000));

                for index in 0..10000 {
                    let log_copy = log.clone();
                    let count_copy = count.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, false, 1).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);

                            if count_copy.fetch_add(1, Ordering::Relaxed) == 999 {
                                match log_copy.split().await {
                                    Err(e) => {
                                        println!("!!!!!!split log failed, e: {:?}", e);
                                    },
                                    Ok(log_index) => {
                                        println!("!!!!!!split log ok, log index: {}", log_index);
                                    },
                                }
                                count_copy.store(0, Ordering::SeqCst);
                            }
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_collect_logs() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(), "./log", 8000, 1024 * 1024, None).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            }
            Ok(log) => {
                let log_paths = vec![PathBuf::from("./log/000001"), PathBuf::from("./log/000002")];

                let start = Instant::now();
                match log
                    .collect_logs(vec![], log_paths, 1024 * 1024, 32 * 1024, true)
                    .await
                {
                    Err(e) => {
                        println!("!!!!!!collect logs failed, e: {:?}", e);
                    }
                    Ok((size, len)) => {
                        println!(
                            "!!!!!!collect logs ok, size: {:?}, len: {:?}, time: {:?}",
                            size,
                            len,
                            Instant::now() - start
                        );
                    }
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}
