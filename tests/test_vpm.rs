use std::io;
use std::mem;
use std::thread;
use std::sync::Arc;
use std::default::Default;
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use std::collections::hash_map::Values;

use parking_lot::RwLock;
use crossbeam_channel::unbounded;
use futures::future::{FutureExt, BoxFuture};
use dashmap::DashMap;
use bytes::BufMut;
use crossbeam_channel::internal::SelectHandle;

use pi_hash::XHashMap;
use pi_sinfo::EnumType::Bin;
use pi_assets::{asset::{Asset, Garbageer, GarbageGuard},
                mgr::AssetMgr,
                allocator::Allocator};
use pi_async_rt::rt::{AsyncRuntime, multi_thread::MultiTaskRuntimeBuilder};

use pi_store::{vpm::{VirtualPageWriteDelta, VirtualPageBuf, PageId,
                     VirtualPageWriteCmd,
                     page_cache::{SharedPageRelease,
                                  VirtualPageLFUCache,
                                  VirtualPageLFUCacheDirtyIterator,
                                  SharedPageBuffer,
                                  init_global_virtual_page_lfu_cache_allocator,
                                  startup_auto_collect,
                                  register_release_handler},
                     page_table::VirtualPageTable,
                     page_pool::{VirtualPageCachingStrategy,
                                 PageBuffer},
                     page_manager::VirtualPageManagerBuilder},
               devices::simple_device::{Binary, SimpleDevice}};
use pi_store::vpm::page_manager::VirtualPageManager;

// Dashmap5.x后出现的Bug
#[test]
fn test_dashmap_bug() {
    let map_1 = Arc::new(DashMap::<i32, String>::default());
    let map_2 = map_1.clone();

    for i in 0..1000 {
        map_1.insert(i, "foobar".to_string());
    }

    let _writer = std::thread::spawn(move || loop {
        println!("writer iteration");
        for i in 0..1000 {
            let mut item = map_1.get_mut(&i).unwrap();
            *item = "foobaz".to_string();
        }
    });

    let _reader = std::thread::spawn(move || loop {
        println!("reader iteration");
        for i in 0..1000 {
            let j = i32::min(i + 100, 1000);
            let _v: Vec<_> = (i..j).map(|k| map_2.get(&k)).collect();
        }
    });

    std::thread::sleep(Duration::from_secs(1000000000));
}

#[derive(Debug, Clone)]
pub struct TestBin(Arc<Vec<u8>>);

impl Asset for TestBin {
    type Key = u64;

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
    all.auto_collect(AsyncRuntime::Multi(rt.clone()), 5000);

    rt.spawn(rt.alloc(), async move {
        for index in 0..10 {
            if let Some(buf) = mgr.insert(index, TestBin(Arc::new(vec![index as u8]))) {
                println!("!!!!!!load ok, index: {:?}, buf: {:?}", index, buf.0.as_slice());
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_virtual_page_table() {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let page_table = VirtualPageTable::new(rt_copy.clone(), "./page_table", 1, 32 * 1024 * 1024, 8192, true, 1).await;
        let current_page_uid = page_table.current_page_uid() as u128;
        println!("!!!!!!current page uid: {}", current_page_uid);
        for page_id in 1..current_page_uid {
            if page_table.addressing(&page_id).is_none() {
                panic!("Test load virtual page table failed, page_id: {}", page_id);
            }
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
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location..location + 1000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender0.send(None);
                    return;
                }
            }

            sender0.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 1000..location + 2000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender1.send(None);
                    return;
                }
            }

            sender1.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 2000..location + 3000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender2.send(None);
                    return;
                }
            }

            sender2.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 3000..location + 4000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender3.send(None);
                    return;
                }
            }

            sender3.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 4000..location + 5000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender4.send(None);
                    return;
                }
            }

            sender4.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 5000..location + 6000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender5.send(None);
                    return;
                }
            }

            sender5.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 6000..location + 7000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
                    sender6.send(None);
                    return;
                }
            }

            sender6.send(Some(()));
        });

        let page_table_copy = page_table.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            for index in location + 7000..location + 8000 {
                let page_id = page_table_copy.alloc_page_uid();
                if let Some(_) = page_table_copy.register(page_id as u128, index as u64) {
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
}

impl VirtualPageWriteDelta for TestWriteDelta {
    type Content = Binary;

    fn size(&self) -> usize {
        //返回足够大的写增量大小，整理时会大概率被同步
        0xffffffff
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
        1
    }

    fn inner(self) -> Self::Content {
        Binary::new(("Hello ".to_string() + self.index.to_string().as_str()).as_bytes().to_vec())
    }
}

impl TestWriteDelta {
    pub fn new(page_id: PageId,
               copy_page_id: PageId) -> Self {
        TestWriteDelta {
            index: 0,
            page_id,
            copy_page_id,
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
    type Content = Binary;
    type Delta = TestWriteDelta;
    type Output = Binary;
    type Bin = Binary;

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
        1
    }

    fn read_page(&self) -> Self::Output {
        Binary::new(self.buf.clone())
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
        Binary::new(self.buf)
    }
}

pub struct TestPageBufRelease<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>(VirtualPageManager<C, O, B, D, P>);

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> SharedPageRelease<C, O, B, D, P> for TestPageBufRelease<C, O, B, D, P> {
    fn release(&self,
               _page_id: u128,
               buffer: Arc<PageBuffer<C, O, B, D, P>>,
               guard: GarbageGuard<SharedPageBuffer<C, O, B, D, P>>) -> BoxFuture<'static, ()> {
        let manager = self.0.clone();
        async move {
            manager
                .sync_page_buffer(buffer)
                .await
                .unwrap();
            mem::drop(guard);
        }.boxed()
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> TestPageBufRelease<C, O, B, D, P> {
    pub fn new(manager: VirtualPageManager<C, O, B, D, P>) -> Self {
        TestPageBufRelease(manager)
    }
}

#[test]
fn test_virtual_page_manager_init() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                        10 * 1024 * 1024,
                                                                                                        1024,
                                                                                                        10 * 1024 * 1024,
                                                                                                        5000);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let device = match SimpleDevice::open(rt_copy.clone(),
                                              "./device/0.simple",
                                              None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => device,
        };

        let cache = VirtualPageLFUCache::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>::new();
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
        if !page_manager.join_device(1, Arc::new(device)) {
            panic!("Join device failed");
        }
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::new(page_manager.clone())));
        startup_auto_collect(AsyncRuntime::Multi(rt_copy.clone()), 5000);

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

            //为写指令增加1个后续增量
            let page_id = page_manager.alloc_page(1, 32);
            cmd.follow_up(TestWriteDelta::new(page_id.clone(),
                                              page_id.clone()));

            match page_manager.write_through(cmd, Some(1000)).await {
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

#[test]
fn test_virtual_page_manager_load_all() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                        10 * 1024 * 1024,
                                                                                                        1024,
                                                                                                        10 * 1024 * 1024,
                                                                                                        5000);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let device = match SimpleDevice::open(rt_copy.clone(),
                                              "./device/0.simple",
                                              None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => device,
        };

        let cache = VirtualPageLFUCache::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>::new();
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
        if !page_manager.join_device(1, Arc::new(device)) {
            panic!("Join device failed");
        }
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::new(page_manager.clone())));
        startup_auto_collect(AsyncRuntime::Multi(rt_copy.clone()), 5000);

        //加载虚拟页表中的所有虚拟页
        let mut count = 0;
        if let Ok(page_ids) = page_manager.load_all().await {
            for page_id in page_ids {
                if let Ok(Some(output)) = page_manager.read(None, &page_id).await {
                    count += 1;
                    println!("!!!!!!load ok, page_id: {:?}, data: {:?}",
                             page_id,
                             String::from_utf8_lossy(output.as_ref()));
                }
            }
        }
        println!("!!!!!!loaded finish, count: {}", count);
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_virtual_page_manager_load_write() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    init_global_virtual_page_lfu_cache_allocator::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>(rt.clone(),
                                                                                                        10 * 1024 * 1024,
                                                                                                        0,
                                                                                                        10 * 1024 * 1024,
                                                                                                        5000);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let device = match SimpleDevice::open(rt_copy.clone(),
                                              "./device/0.simple",
                                              None).await {
            Err(e) => panic!("Open simple device failed, reason: {:?}", e),
            Ok(device) => device,
        };

        let cache = VirtualPageLFUCache::<Binary, Binary, Binary, TestWriteDelta, TestPageBuf>::new();
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
        if !page_manager.join_device(1, Arc::new(device)) {
            panic!("Join device failed");
        }
        register_release_handler(1,
                                 Arc::new(TestPageBufRelease::new(page_manager.clone())));
        startup_auto_collect(AsyncRuntime::Multi(rt_copy.clone()), 5000);

        //加载虚拟页表中的所有虚拟页
        if let Ok(page_ids) = page_manager.load_all().await {
            for page_id in page_ids {
                let mut cmd = VirtualPageWriteCmd::new();

                //为写指令增加1个增量
                let new_page_id = page_manager.alloc_page(1, 128);
                cmd.append(TestWriteDelta::new(new_page_id.clone(),
                                               new_page_id.clone()));

                //为写指令增加1个后续增量
                cmd.follow_up(TestWriteDelta::new(page_id.clone(),
                                                  page_id.clone()));

                let page_manager_copy = page_manager.clone();
                rt_copy.spawn(rt_copy.alloc(), async move {
                    match page_manager_copy.write_through(cmd, None).await {
                        Err(e) => {
                            panic!("Write through failed, reason: {:?}", e);
                        },
                        Ok(r) => {
                            println!("!!!!!!Write through ok, cmd index: {}", *r);
                        },
                    }
                });
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}
