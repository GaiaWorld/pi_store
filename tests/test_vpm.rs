use std::thread;
use std::sync::Arc;
use std::default::Default;
use std::io::ErrorKind;
use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;
use futures::future::{FutureExt, BoxFuture};
use dashmap::DashMap;
use bytes::BufMut;
use crossbeam_channel::internal::SelectHandle;
use futures::task::SpawnExt;

use pi_assets::{asset::{Asset, Size, Garbageer, GarbageGuard},
                mgr::AssetMgr,
                allocator::Allocator};
use pi_async_rt::rt::{AsyncRuntime,
                      AsyncValueNonBlocking,
                      multi_thread::MultiTaskRuntimeBuilder,
                      startup_global_time_loop};

use pi_store::{vpm::{VirtualPageWriteDelta, VirtualPageBuf, PageId,
                     VirtualPageWriteCmd,
                     page_cache::{SharedPageRelease,
                                  VirtualPageLFUCache,
                                  SharedPageBuffer,
                                  init_global_virtual_page_lfu_cache_allocator,
                                  startup_auto_collect,
                                  register_release_handler},
                     page_table::VirtualPageTable,
                     page_pool::{VirtualPageCachingStrategy,
                                 PageBuffer},
                     page_manager::{VirtualPageManagerBuilder, VirtualPageManager}},
               devices::simple_device::{Binary, SimpleDevice}};
use pi_blocks_allocator::device::{BuddyBlocksDeviceBuilder, BuddyBlocksDevice};

// Dashmap5.x后出现的Bug
#[test]
fn test_dashmap_bug() {
    let map_1 = Arc::new(DashMap::<i32, String>::default());
    let map_2 = map_1.clone();

    for i in 0..1000 {
        map_1.insert(i, "foobar".to_string());
    }

    let _writer = thread::spawn(move || loop {
        println!("writer iteration");
        for i in 0..1000 {
            let mut item = map_1.get_mut(&i).unwrap();
            *item = "foobaz".to_string();
        }
    });

    let _reader = thread::spawn(move || loop {
        println!("reader iteration");
        for i in 0..1000 {
            let j = i32::min(i + 100, 1000);
            let _v: Vec<_> = (i..j).map(|k| map_2.get(&k)).collect();
        }
    });

    thread::sleep(Duration::from_secs(1000000000));
}

#[test]
fn test_async_value() {
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let rt_clone = rt_copy.clone();
        let value = AsyncValueNonBlocking::new();
        let value_copy = value.clone();
        rt_copy.spawn(async move {
            let value_clone = value_copy.clone();
            rt_clone.spawn(async move {
                let r = value_clone.await;
                println!("!!!!!!1");
            });

            value_copy.set(());
        });
        value.await;
        println!("!!!!!!0");
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[derive(Debug, Clone)]
pub struct TestBin(Arc<Vec<u8>>);

impl Asset for TestBin {
    type Key = u64;
}

impl Size for TestBin {
    fn size(&self) -> usize {
        self.0.len()
    }
}

pub struct TestCallback;

impl Garbageer<TestBin> for TestCallback {
    fn garbage_ref(&self, k: &u64, v: &TestBin, _timeout: u64, _guard: GarbageGuard<TestBin>) {
        println!("!!!!!!gc ok, k: {:?}, v: {:?}", k, v);
    }
}

#[test]
fn test_asserts() {
    let rt = MultiTaskRuntimeBuilder::default().build();

    let mgr = AssetMgr::new(TestCallback,
                                     true,
                                     10 * 1024 * 1024,
                                     5000);

    let mut all = Allocator::new(100 * 1024 * 1024);
    all.register(mgr.clone(), 1024 * 1024, 10 * 1024 * 1024);
    all.auto_collect(rt.clone(), 5000);

    rt.spawn(async move {
        for index in 0..10 {
            if let Ok(buf) = mgr.insert(index, TestBin(Arc::new(vec![index as u8]))) {
                println!("!!!!!!load ok, index: {:?}, buf: {:?}", index, buf.0.as_slice());
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_virtual_page_table() {
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let page_table =
            VirtualPageTable::new(rt_copy.clone(),
                                  "./page_table",
                                  1,
                                  0,
                                  32 * 1024 * 1024,
                                  8192,
                                  true,
                                  1)
                .await;
        let current_page_uid = page_table.current_page_uid() as u128;
        println!("!!!!!!current page uid: {}", current_page_uid);
        for page_id in 1..current_page_uid {
            assert!(page_table.addressing(&page_id.into()).is_some());
        }

        let location = current_page_uid - 1;
        let (sender, receiver) = unbounded();
        let sender0 = sender.clone();
        let sender1 = sender.clone();
        let sender2 = sender.clone();
        let sender3 = sender.clone();
        let sender4 = sender.clone();
        let sender5 = sender.clone();
        let sender6 = sender.clone();
        let sender7 = sender.clone();

        let now = Instant::now();
        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location..location + 1000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender0.send(None);
                    return;
                }
            }

            sender0.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 1000..location + 2000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender1.send(None);
                    return;
                }
            }

            sender1.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 2000..location + 3000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender2.send(None);
                    return;
                }
            }

            sender2.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 3000..location + 4000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender3.send(None);
                    return;
                }
            }

            sender3.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 4000..location + 5000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender4.send(None);
                    return;
                }
            }

            sender4.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 5000..location + 6000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender5.send(None);
                    return;
                }
            }

            sender5.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 6000..location + 7000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender6.send(None);
                    return;
                }
            }

            sender6.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(async move {
            for index in location + 7000..location + 8000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register((page_id as u128).into(), index as u64) {
                    sender7.send(None);
                    return;
                }
            }

            sender7.send(Some(()));
        });

        let mut count = 0;
        let mut err_count = 0;
        loop {
            match receiver.recv() {
                Err(e) => {
                    panic!("Test virtual page table failed, reason: {:?}", e);
                },
                Ok(Some(_)) => {
                    count += 1;
                },
                Ok(None) => {
                    err_count += 1;
                },
            }

            if count + err_count == 8 {
                break;
            }
        }

        println!("!!!!!!swap in finish, count: {}, err_count: {}, time: {:?}", count, err_count, now.elapsed());

        let now = Instant::now();
        if let Err(e) = page_table.flush().await {
            panic!("Test virtual page failed, reason: {:?}", e);
        }
        println!("!!!!!!flush ok, time: {:?}", now.elapsed());
    });

    thread::sleep(Duration::from_millis(1000000000));
}

pub struct TestWriteDelta {
    index:          u64,
    page_id:        PageId,
    copy_page_id:   PageId,
    delta_type:     usize,
}

impl VirtualPageWriteDelta for TestWriteDelta {
    type Content = Vec<u8>;

    fn size(&self) -> usize {
        //返回足够大的写增量大小，整理时会大概率被同步
        0xffff
    }

    fn get_cmd_index(&self) -> u64 {
        self.index
    }

    fn set_cmd_index(&mut self, cmd_index: u64) {
        self.index = cmd_index;
    }

    fn get_origin_page_id(&self) -> PageId {
        self.page_id.clone()
    }

    fn get_copied_page_id(&self) -> PageId {
        self.copy_page_id.clone()
    }

    fn get_type(&self) -> usize {
        self.delta_type
    }

    fn inner(self) -> Self::Content {
        match self.get_type() {
            1 => {
                ("Hello ".to_string() + self.copy_page_id.page_uid().to_string().as_str()).into_bytes()
            },
            _ => {
                ("This is super block ".to_string() + self.copy_page_id.page_uid().to_string().as_str()).into_bytes()
            }
        }
    }
}

impl TestWriteDelta {
    pub fn new(page_id: PageId,
               copy_page_id: PageId) -> Self {
        if copy_page_id.is_normal() {
            TestWriteDelta {
                index: 0,
                page_id,
                copy_page_id,
                delta_type: 1,
            }
        } else if copy_page_id.is_internal() {
            TestWriteDelta {
                index: 0,
                page_id,
                copy_page_id,
                delta_type: 2
            }
        } else {
            unimplemented!()
        }
    }
}

#[derive(Clone)]
pub struct TestPageBuf {
    page_id:        PageId,
    copy_page_id:   PageId,
    page_type:      usize,
    buf:            Vec<u8>,
}

impl VirtualPageBuf for TestPageBuf {
    type Content = Vec<u8>;
    type Delta = TestWriteDelta;
    type Output = Vec<u8>;
    type Bin = Vec<u8>;

    fn with_page_type(origin_page_id: PageId,
                      copied_page_id: PageId,
                      _page_type: Option<usize>) -> Self {
        TestPageBuf {
            page_id: origin_page_id,
            copy_page_id: copied_page_id,
            page_type: 1,
            buf: Vec::default(),
        }
    }

    fn get_original_page_id(&self) -> PageId {
        self.page_id.clone()
    }

    fn get_copied_page_id(&self) -> PageId {
        self.copy_page_id.clone()
    }

    fn is_missing_pages(&self) -> bool {
        self.buf.is_empty()
    }

    fn get_page_type(&self) -> usize {
        self.page_type
    }

    fn page_size(&self) -> usize {
        self.buf.len()
    }

    fn read_page(&self) -> Self::Output {
        self.buf.clone()
    }

    fn write_page_delta(&mut self, delta: Self::Delta) -> Result<(), String> {
        self
            .buf
            .put_slice(delta.inner().as_ref());

        Ok(())
    }

    fn deserialize_page<Input>(&mut self, bin: Input)
        where Input: AsRef<[u8]> + Send + Sized + 'static {
        self.buf.clear();
        self.buf.put_slice(bin.as_ref());
    }

    fn serialize_page(self) -> Self::Bin {
        self.buf
    }
}

pub struct TestPageBufRelease<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Default + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>(VirtualPageManager<C, O, B, D, P>);

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Default + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> SharedPageRelease<C, O, B, D, P> for TestPageBufRelease<C, O, B, D, P> {
    fn release(&self,
               page_id: u128,
               buffer: Arc<PageBuffer<C, O, B, D, P>>,
               guard: GarbageGuard<SharedPageBuffer<C, O, B, D, P>>) -> BoxFuture<'static, ()> {
        let manager = self.0.clone();
        async move {
            println!("======> Release shared page cache, page_id: {:?}", page_id);
            manager
                .sync_page_buffer(buffer, true)
                .await
                .unwrap();
            drop(guard);
        }.boxed()
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Default + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> TestPageBufRelease<C, O, B, D, P> {
    pub fn new(manager: VirtualPageManager<C, O, B, D, P>) -> Self {
        TestPageBufRelease(manager)
    }
}

// 执行后可以执行test_virtual_page_manager_load_all
#[test]
fn test_virtual_page_manager_init () {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                        10 * 1024 * 1024,
                                                                                                        1024,
                                                                                                        10 * 1024 * 1024,
                                                                                                        5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //分配新的页面，并写入分配的页面
        for index in 0..10 {
            //初始化写指令
            let mut cmd = VirtualPageWriteCmd::new();

            //为写指令增加5个增量
            for _ in 0..5 {
                let page_id = page_manager
                    .alloc_page(1, 16);
                cmd.append(TestWriteDelta::new(page_id.clone(),
                                               page_id.clone()));
            }

            //为写指令增加1个后续增量，后续增量写入分配的内部页
            let super_page_id = page_manager.alloc_page(0, 32);
            cmd.follow_up(TestWriteDelta::new(super_page_id.clone(),
                                              super_page_id.clone()));

            match page_manager.write_through(cmd, Some(1000), true).await {
                Err(e) => {
                    panic!("Write through failed, cmd index: {}, reason: {:?}", index, e);
                },
                Ok(r) => {
                    println!("!!!!!!Write through ok, cmd index: {}", *r);
                },
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 可以在执行test_virtual_page_manager_init后再执行
#[test]
fn test_virtual_page_manager_load_all() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                           10 * 1024 * 1024,
                                                                                                           1024,
                                                                                                           10 * 1024 * 1024,
                                                                                                           5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        match page_manager.load_all(true).await {
            Err(e) => {
                println!("!!!!!!loaded failed, reason: {:?}", e);
            },
            Ok(page_ids) => {
                for page_id in page_ids {
                    if page_id.is_normal() {
                        match page_manager.read(None, &page_id, true).await {
                            Err(e) => {
                                println!("!!!!!!load failed, page_id: {:?}, reason: {:?}", page_id, e);
                            },
                            Ok(None) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                            },
                            Ok(Some(output)) => {
                                count += 1;
                                println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                         page_id,
                                         String::from_utf8_lossy(output.as_ref()));
                            },
                        }
                    } else if page_id.is_internal() {
                        if let Some(output) = page_manager.read_internal(&page_id) {
                            count += 1;
                            println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                     page_id,
                                     String::from_utf8_lossy(output.as_ref()));
                        }
                    } else {
                        unimplemented!()
                    }
                }
                println!("!!!!!!loaded finish, count: {}", count);
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 加载已有页面，并追加新的页面
#[test]
fn test_virtual_page_manager_load_append() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                           10 * 1024 * 1024,
                                                                                                           1024,
                                                                                                           10 * 1024 * 1024,
                                                                                                           5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        match page_manager.load_all(true).await {
            Err(e) => {
                println!("!!!!!!loaded failed, reason: {:?}", e);
            },
            Ok(page_ids) => {
                for page_id in page_ids {
                    if page_id.is_normal() {
                        match page_manager.read(None, &page_id, true).await {
                            Err(e) => {
                                println!("!!!!!!load failed, page_id: {:?}, reason: {:?}", page_id, e);
                            },
                            Ok(None) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                            },
                            Ok(Some(output)) => {
                                count += 1;
                                println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                         page_id,
                                         String::from_utf8_lossy(output.as_ref()));
                            },
                        }
                    } else if page_id.is_internal() {
                        if let Some(output) = page_manager.read_internal(&page_id) {
                            count += 1;
                            println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                     page_id,
                                     String::from_utf8_lossy(output.as_ref()));
                        }
                    } else {
                        unimplemented!()
                    }
                }
                println!("!!!!!!loaded finish, count: {}", count);

                //初始化写指令
                let mut cmd = VirtualPageWriteCmd::new();

                //为写指令增加1个增量
                let page_id = page_manager
                    .alloc_page(1, 16) ;
                cmd.append(TestWriteDelta::new(page_id.clone(),
                                               page_id.clone()));

                //为写指令增加1个后续增量，后续增量写入分配的内部页
                let super_page_id = page_manager.alloc_page(0, 32);
                cmd.follow_up(TestWriteDelta::new(super_page_id.clone(),
                                                  super_page_id.clone()));

                match page_manager.write_through(cmd, Some(1000), true).await {
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        println!("Write through failed, reason: {:?}", e);
                    },
                    Err(e) => {
                        panic!("Write through failed, reason: {:?}", e);
                    },
                    Ok(r) => {
                        println!("!!!!!!Write through ok, cmd index: {}", *r);
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 加载已有页面，并更新已有页面
#[test]
fn test_virtual_page_manager_load_update() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                           10 * 1024 * 1024,
                                                                                                           1024,
                                                                                                           10 * 1024 * 1024,
                                                                                                           5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        match page_manager.load_all(true).await {
            Err(e) => {
                println!("!!!!!!loaded failed, reason: {:?}", e);
            },
            Ok(mut page_ids) => {
                page_ids.sort();
                for page_id in &page_ids {
                    if page_id.is_normal() {
                        match page_manager.read(None, page_id, true).await {
                            Err(e) => {
                                println!("!!!!!!load failed, page_id: {:?}, reason: {:?}", page_id, e);
                            },
                            Ok(None) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                            },
                            Ok(Some(output)) => {
                                count += 1;
                                println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                         page_id,
                                         String::from_utf8_lossy(output.as_ref()));
                            },
                        }
                    } else if page_id.is_internal() {
                        if let Some(output) = page_manager.read_internal(page_id) {
                            count += 1;
                            println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                     page_id,
                                     String::from_utf8_lossy(output.as_ref()));
                        }
                    } else {
                        unimplemented!()
                    }
                }
                println!("!!!!!!loaded finish, count: {}", count);

                let mut page_id = PageId::empty();
                let mut follow_up_page_id = PageId::empty();
                if count == 0 {
                    //当前没有页面，则追加页面
                    page_id = page_manager
                        .alloc_page(1, 16) ;
                    follow_up_page_id = page_manager
                        .alloc_page(0, 32);
                } else {
                    //当前有页面
                    page_id = page_ids[0].clone();
                    follow_up_page_id = page_ids[1].clone();
                }

                //初始化写指令
                let mut cmd = VirtualPageWriteCmd::new();

                //为写指令增加1个增量
                cmd.append(TestWriteDelta::new(page_id.clone(),
                                               page_id.clone()));

                //为写指令增加1个后续增量，后续增量写入分配的内部页
                cmd.follow_up(TestWriteDelta::new(follow_up_page_id.clone(),
                                                  follow_up_page_id.clone()));

                match page_manager.write_through(cmd, Some(1000), true).await {
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        println!("Write through failed, reason: {:?}", e);
                    },
                    Err(e) => {
                        panic!("Write through failed, reason: {:?}", e);
                    },
                    Ok(r) => {
                        println!("!!!!!!Write through ok, cmd index: {}", *r);
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 加载已有页面，并写时复制的方式更新已有页面
// 初始化时使用空页面进行写时复制的更新
#[test]
fn test_virtual_page_manager_load_copy_on_write() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                           10 * 1024 * 1024,
                                                                                                           1024,
                                                                                                           10 * 1024 * 1024,
                                                                                                           5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        match page_manager.load_all(true).await {
            Err(e) => {
                println!("!!!!!!loaded failed, reason: {:?}", e);
            },
            Ok(mut page_ids) => {
                let mut current_page_id = PageId::empty();

                page_ids.sort();
                for page_id in page_ids {
                    if page_id.is_normal() {
                        match page_manager.read(None, &page_id, true).await {
                            Err(e) => {
                                println!("!!!!!!load failed, page_id: {:?}, reason: {:?}", page_id, e);
                            },
                            Ok(None) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                            },
                            Ok(Some(output)) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                         page_id,
                                         String::from_utf8_lossy(output.as_ref()));
                                count += 1;
                                current_page_id = page_id;
                            },
                        }
                    } else if page_id.is_internal() {
                        if let Some(output) = page_manager.read_internal(&page_id) {
                            println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                     page_id,
                                     String::from_utf8_lossy(output.as_ref()));
                            count += 1;
                        } else {
                            println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                        }
                    } else {
                        unimplemented!()
                    }
                }
                println!("!!!!!!loaded finish, count: {}", count);

                //初始化写指令
                let mut cmd = VirtualPageWriteCmd::new();

                //为写指令增加1个增量
                let new_page_id = page_manager
                    .alloc_page(1, 16) ;
                cmd.append(TestWriteDelta::new(current_page_id.clone(),
                                               new_page_id.clone()));

                //为写指令增加1个后续增量，后续增量写入分配的内部页
                let new_follow_up_page_id = page_manager
                    .alloc_page(0, 32);
                cmd.follow_up(TestWriteDelta::new(new_follow_up_page_id.clone(),
                                                  new_follow_up_page_id.clone()));

                match page_manager.write_through(cmd, Some(1000), true).await {
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        println!("Write through failed, reason: {:?}", e);
                    },
                    Err(e) => {
                        panic!("Write through failed, reason: {:?}", e);
                    },
                    Ok(r) => {
                        println!("!!!!!!Write through ok, cmd index: {}", *r);
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 加载已有页面，并写时复制的方式更新已有页面，并释放更新后的原始页面
// 初始化时使用空页面进行写时复制的更新
#[test]
fn test_virtual_page_manager_load_and_copy_on_write_and_free() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                           10 * 1024 * 1024,
                                                                                                           1024,
                                                                                                           10 * 1024 * 1024,
                                                                                                           5000);

    let rt_copy = rt.clone();
    rt.spawn(async move {
        let device = BuddyBlocksDeviceBuilder::new("./device")
            .build(rt_copy.clone())
            .await
            .unwrap();

        let cache = VirtualPageLFUCache::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new();
        let page_manager = VirtualPageManagerBuilder::new(1,
                                                          rt_copy.clone(),
                                                          "./page_table",
                                                          cache)
            .set_init_page_uid(1)
            .set_table_log_file_limit(32 * 1024 * 1024)
            .set_table_load_buf_len(8192)
            .set_pool_buffer_delta_limit(8192)
            .set_table_delay_timeout(1)
            .build()
            .await;
        page_manager.startup_collecting();

        let r = page_manager.join_device(1, Arc::new(device));
        assert!(r);
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::<Vec<u8>, Vec<u8>, Vec<u8>, TestWriteDelta, TestPageBuf>::new(page_manager.clone())));
        startup_auto_collect(rt_copy.clone(), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        match page_manager.load_all(true).await {
            Err(e) => {
                println!("!!!!!!loaded failed, reason: {:?}", e);
            },
            Ok(mut page_ids) => {
                let mut current_page_id = PageId::empty();
                let mut current_follow_up_page_id = PageId::empty();

                page_ids.sort();
                for page_id in page_ids {
                    if page_id.is_normal() {
                        match page_manager.read(None, &page_id, true).await {
                            Err(e) => {
                                println!("!!!!!!load failed, page_id: {:?}, reason: {:?}", page_id, e);
                            },
                            Ok(None) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                            },
                            Ok(Some(output)) => {
                                println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                         page_id,
                                         String::from_utf8_lossy(output.as_ref()));
                                count += 1;
                                current_page_id = page_id;
                            },
                        }
                    } else if page_id.is_internal() {
                        if let Some(output) = page_manager.read_internal(&page_id) {
                            println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                                     page_id,
                                     String::from_utf8_lossy(output.as_ref()));
                            count += 1;
                            current_follow_up_page_id = page_id;
                        } else {
                            println!("!!!!!!load ok, page_id: {:?}, data: None", page_id);
                        }
                    } else {
                        unimplemented!()
                    }
                }
                println!("!!!!!!loaded finish, count: {}", count);

                //初始化写指令
                let mut cmd = VirtualPageWriteCmd::new();

                //为写指令增加1个增量
                let new_page_id = page_manager
                    .alloc_page(1, 16) ;
                cmd.append(TestWriteDelta::new(current_page_id.clone(),
                                               new_page_id.clone()));

                //为写指令增加1个后续增量，后续增量写入分配的内部页
                let new_follow_up_page_id = page_manager
                    .alloc_page(0, 32);
                cmd.follow_up(TestWriteDelta::new(new_follow_up_page_id.clone(),
                                                  new_follow_up_page_id.clone()));

                match page_manager.write_through(cmd, Some(1000), true).await {
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        println!("Write through failed, reason: {:?}", e);
                    },
                    Err(e) => {
                        panic!("Write through failed, reason: {:?}", e);
                    },
                    Ok(r) => {
                        println!("!!!!!!Write through ok, cmd index: {}", *r);

                        if !current_page_id.is_empty()
                            && !current_follow_up_page_id.is_empty() {
                            //原始页面不是空页面，则立即释放原始页面
                            let r0 = page_manager.free_page(current_page_id.clone());
                            let r1 = page_manager.free_page(current_follow_up_page_id.clone());
                            if r0 && r1 {
                                println!("!!!!!!Free page ok, current_page_id: {:?}, current_follow_up_page_id: {:?}",
                                         current_page_id,
                                         current_follow_up_page_id);
                            } else {
                                println!("!!!!!!Free page failed, current_page_id: {:?}/{:?}, current_follow_up_page_id: {:?}/{:?}, ",
                                         current_page_id,
                                         r0,
                                         current_follow_up_page_id,
                                         r1);
                            }
                        }
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}