use std::thread;
use std::fs::{copy, remove_file};
use std::path::Path;
use std::time::Duration;

use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntimeBuilder,
                      startup_global_time_loop};

use pi_store::devices::{BlockDevice, BlockLocation, DeviceStatus, simple_device::{Binary, SimpleDevice}};

use pi_blocks_allocator::device::{BuddyBlocksDeviceBuilder, BuddyBlocksDevice};
#[test]
fn test_buddy_blocks_device() {
    env_logger::init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        //清理测试环境
        let _r = remove_file("./buddy_blocks/000.dat");
        let _r = remove_file("./buddy_blocks/000.dat.0");
        let _r = remove_file("./buddy_blocks/000.dat.1");

        let mut builder = BuddyBlocksDeviceBuilder::new("./buddy_blocks");
        let device = builder
            .build(rt_copy.clone())
            .await
            .unwrap();
        assert!(device.is_full_free());
        assert!(device.is_freed(&BlockLocation::empty()));
        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
        assert_eq!(device.avail_size().unwrap(), device.capacity().unwrap());
        assert_eq!(device.used_size(), 0);
        assert_eq!(device.block_unit_len(), 4096);
        assert_eq!(device.max_block_size(), 4096 * 4096 * 8);
        assert_eq!(device.get_url().unwrap().to_string().as_str(),
                   "file:///E:/rust/pi_mastser/crate%20master/pi_store/buddy_blocks/");
        assert_eq!(device.get_status() as u8, 1);

        //测试分配
        let mut blocks = Vec::with_capacity(10);
        for size in vec![1, 4096, 4097, 8192, 8193, 10000, 20000, 65536, 65537, 4096 * 4096 * 8, 4096 * 4096 * 8 + 1] {
            let block = device.alloc_block(size).await;

            match size {
                1 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 1));
                    assert_eq!(device.used_size(), 1 * 4096);
                },
                4096 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 2));
                    assert_eq!(device.used_size(), 2 * 4096);
                },
                4097 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 4));
                    assert_eq!(device.used_size(), 4 * 4096);
                },
                8192 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 6));
                    assert_eq!(device.used_size(), 6 * 4096);
                },
                8193 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 10));
                    assert_eq!(device.used_size(), 10 * 4096);
                },
                10000 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 14));
                    assert_eq!(device.used_size(), 14 * 4096);
                },
                20000 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 22));
                    assert_eq!(device.used_size(), 22 * 4096);
                },
                65536 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 38));
                    assert_eq!(device.used_size(), 38 * 4096);
                },
                65537 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 70));
                    assert_eq!(device.used_size(), 70 * 4096);
                },
                134217728 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 70));
                    assert_eq!(device.used_size(), 134504448);
                },
                134217729 => {
                    assert!(block.is_empty());
                    assert!(device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 70));
                    assert_eq!(device.used_size(), 134504448);
                },
                _ => unimplemented!(),
            }

            blocks.push(block);
        }

        //测试读写
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .write(block, &format!("This is {:?}", index).into_bytes())
                    .await;
                assert!(r.is_ok());
            }
        }
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .read(block)
                    .await;
                assert!(r.is_ok());
                assert_eq!(String::from_utf8_lossy(&r.unwrap()[..9]).to_string(), format!("This is {:?}", index));
                println!("read {:?} ok", index);
            }
        }

        //测试释放前复制块文件
        rt_copy.timeout(6000).await;
        let from = "./buddy_blocks/000.dat";
        let to = "./buddy_blocks/000.dat.0";
        let r = copy(from, to);
        assert!(r.is_ok());

        //测试释放
        for (index, block) in blocks.into_iter().enumerate() {
            if !device.is_freed(&block) {
                println!("free block: {:?}", block);
                let r = device.free_block(&block).await;
                assert!(r);
                assert!(device.is_freed(&block));

                match index {
                    0 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 69));
                        assert_eq!(device.used_size(), 134504448 - 1 * 4096);
                    },
                    1 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 68));
                        assert_eq!(device.used_size(), 134504448 - 2 * 4096);
                    },
                    2 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 66));
                        assert_eq!(device.used_size(), 134504448 - 4 * 4096);
                    },
                    3 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 64));
                        assert_eq!(device.used_size(), 134504448 - 6 * 4096);
                    },
                    4 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 60));
                        assert_eq!(device.used_size(), 134504448 - 10 * 4096);
                    },
                    5 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 56));
                        assert_eq!(device.used_size(), 134504448 - 14 * 4096);
                    },
                    6 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 48));
                        assert_eq!(device.used_size(), 134504448 - 22 * 4096);
                    },
                    7 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 32));
                        assert_eq!(device.used_size(), 134504448 - 38 * 4096);
                    },
                    8 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 0));
                        assert_eq!(device.used_size(), 134504448 - 70 * 4096);
                    },
                    9 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.used_size(), 134504448 - 70 * 4096 - 4096 * 4096 * 8);
                    },
                    _ => unimplemented!(),
                }
            }
        }

        println!("=======================");

        //再测试分配
        let mut blocks = Vec::with_capacity(10);
        for size in vec![1, 4096, 4097, 8192, 8193, 10000, 20000, 65536, 65537, 4096 * 4096 * 8, 4096 * 4096 * 8 + 1] {
            let block = device.alloc_block(size).await;

            match size {
                1 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 1));
                    assert_eq!(device.used_size(), 1 * 4096);
                },
                4096 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 2));
                    assert_eq!(device.used_size(), 2 * 4096);
                },
                4097 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 4));
                    assert_eq!(device.used_size(), 4 * 4096);
                },
                8192 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 6));
                    assert_eq!(device.used_size(), 6 * 4096);
                },
                8193 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 10));
                    assert_eq!(device.used_size(), 10 * 4096);
                },
                10000 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 14));
                    assert_eq!(device.used_size(), 14 * 4096);
                },
                20000 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 22));
                    assert_eq!(device.used_size(), 22 * 4096);
                },
                65536 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 38));
                    assert_eq!(device.used_size(), 38 * 4096);
                },
                65537 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 * 2 - 70));
                    assert_eq!(device.used_size(), 70 * 4096);
                },
                134217728 => {
                    assert!(!block.is_empty());
                    assert!(!device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 70));
                    assert_eq!(device.used_size(), 134504448);
                },
                134217729 => {
                    assert!(block.is_empty());
                    assert!(device.is_freed(&block));
                    assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                    assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 70));
                    assert_eq!(device.used_size(), 134504448);
                },
                _ => unimplemented!(),
            }

            blocks.push(block);
        }

        //再测试读写
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .write(block, &format!("This is {:?}", index).into_bytes())
                    .await;
                assert!(r.is_ok());
            }
        }
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .read(block)
                    .await;
                assert!(r.is_ok());
                assert_eq!(String::from_utf8_lossy(&r.unwrap()[..9]).to_string(), format!("This is {:?}", index));
                println!("read {:?} ok", index);
            }
        }

        //再测试释放前复制块文件
        rt_copy.timeout(6000).await;
        let from = "./buddy_blocks/000.dat";
        let to = "./buddy_blocks/000.dat.1";
        let r = copy(from, to);
        assert!(r.is_ok());

        //再测试释放
        for (index, block) in blocks.into_iter().enumerate() {
            if !device.is_freed(&block) {
                println!("free block: {:?}", block);
                let r = device.free_block(&block).await;
                assert!(r);
                assert!(device.is_freed(&block));

                match index {
                    0 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 69));
                        assert_eq!(device.used_size(), 134504448 - 1 * 4096);
                    },
                    1 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 68));
                        assert_eq!(device.used_size(), 134504448 - 2 * 4096);
                    },
                    2 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 66));
                        assert_eq!(device.used_size(), 134504448 - 4 * 4096);
                    },
                    3 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 64));
                        assert_eq!(device.used_size(), 134504448 - 6 * 4096);
                    },
                    4 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 60));
                        assert_eq!(device.used_size(), 134504448 - 10 * 4096);
                    },
                    5 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 56));
                        assert_eq!(device.used_size(), 134504448 - 14 * 4096);
                    },
                    6 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 48));
                        assert_eq!(device.used_size(), 134504448 - 22 * 4096);
                    },
                    7 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 32));
                        assert_eq!(device.used_size(), 134504448 - 38 * 4096);
                    },
                    8 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * (4096 * 8 - 0));
                        assert_eq!(device.used_size(), 134504448 - 70 * 4096);
                    },
                    9 => {
                        assert_eq!(device.capacity().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.avail_size().unwrap(), 4096 * 4096 * 8 * 2);
                        assert_eq!(device.used_size(), 134504448 - 70 * 4096 - 4096 * 4096 * 8);
                    },
                    _ => unimplemented!(),
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_load_buddy_blocks_device_after_crashed() {
    env_logger::init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let mut builder = BuddyBlocksDeviceBuilder::new("./buddy_blocks1");
        builder = builder.set_collect_interval(Duration::from_millis(1000000000));
        let device = builder
            .build(rt_copy.clone())
            .await
            .unwrap();
        assert_eq!(device.max_block_size(), 4096 * 4096 * 8);
        assert_eq!(device.get_url().unwrap().to_string().as_str(),
                   "file:///E:/rust/pi_mastser/crate%20master/pi_store/buddy_blocks1/");
        assert_eq!(device.get_status() as u8, 1);

        let metadata = device.get_metadata().await;
        println!("device metadata: {:#?}", metadata);

        //测试分配
        let mut blocks = Vec::with_capacity(10);
        for size in vec![1, 4096, 4097, 8192, 8193, 10000, 20000, 65536, 65537, 4096 * 4096 * 8, 4096 * 4096 * 8 + 1] {
            let block = device.alloc_block(size).await;
            blocks.push(block);
        }

        //测试读写
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .write(block, &format!("This is {:?}", index).into_bytes())
                    .await;
                assert!(r.is_ok());
            }
        }
        for (index, block) in blocks.iter().enumerate() {
            if !device.is_freed(&block) {
                let r = device
                    .read(block)
                    .await;
                assert!(r.is_ok());
                assert_eq!(String::from_utf8_lossy(&r.unwrap()[..9]).to_string(), format!("This is {:?}", index));
                println!("read {:?} ok", index);
            }
        }

        //关闭进程前提交脏块组和脏块索引
        let r = device.mamually_commit_block_groups().await;
        assert!(r.is_ok());
        let r = device.mamually_commit_block_indexes().await;
        assert!(r.is_ok());

        let metadata = device.get_metadata().await;
        println!("device metadata: {:#?}", metadata);

        std::process::exit(0);
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_simple_device() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        match SimpleDevice::open(rt_copy, "./device/0.simple", None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => {
                println!("!!!!!!device: {:?}", device.get_status());

                let location = device.alloc_block(50).await;
                println!("!!!!!!location: {:?}", location);
                match device.write(&location,
                                   &Binary::new("Hello Simple device".to_string().into_bytes())).await {
                    Err(e) => panic!("Write block failed, reason: {:?}", e),
                    Ok(len) => {
                        println!("!!!!!!write ok, len: {}", len);
                    }
                }

                match device.read(&location).await {
                    Err(e) => panic!("Read block failed, reason: {:?}", e),
                    Ok(block) => {
                        println!("read writed data: {:?}", String::from_utf8_lossy(block.as_ref()))
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 先执行一次test_simple_device，再执行
#[test]
fn test_simple_device_collect() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        match SimpleDevice::open(rt_copy, "./device/0.simple", None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => {
                println!("!!!!!!device: {:?}", device.get_status());


                match device.collect_alloced_blocks(&[BlockLocation::from(1)]).await {
                    Err(e) => println!("Collect alloced failed, reason: {:?}", e),
                    Ok(size) => {
                        println!("collect alloced ok, size: {}", size);
                    }
                }

                let location = device.alloc_block(50).await;
                println!("!!!!!!location: {:?}", location);
                match device.write(&location,
                                   &Binary::new("Hello Simple device".to_string().into_bytes())).await {
                    Err(e) => panic!("Write block failed, reason: {:?}", e),
                    Ok(len) => {
                        println!("!!!!!!write ok, len: {}", len);
                    }
                }

                match device.read(&location).await {
                    Err(e) => panic!("Read block failed, reason: {:?}", e),
                    Ok(block) => {
                        println!("read writed data: {:?}", String::from_utf8_lossy(block.as_ref()))
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}