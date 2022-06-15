use std::ops::Deref;
use std::marker::PhantomData;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use std::result::Result as GenResult;
use std::cmp::Ordering as CmpOrdering;
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering}};

use futures::future::{FutureExt, BoxFuture};
use dashmap::{DashMap, iter::Iter};
use bytes::{Buf, BufMut};
use log::{warn, debug};

use pi_async::{lock::{spin_lock::SpinLock, mutex_lock::Mutex},
               rt::{AsyncTaskPoolExt, AsyncTaskPool, AsyncRuntime, multi_thread::{MultiTaskRuntimeBuilder as PiMultiTaskRuntimeBuilder, MultiTaskRuntime}}};
use pi_assets::{asset::{Asset, Garbageer, GarbageGuard},
                mgr::{AssetMgr, LoadResult},
                allocator::Allocator};
use pi_share::Share;
use pi_hash::XHashMap;


use crate::vpm::{VirtualPageWriteDelta, VirtualPageBuf, page_pool::{VirtualPageCachingStrategy, PageBuffer}, page_manager::VirtualPageManager, PageId};

// 全局虚拟页LFU缓存分配器
lazy_static! {
    static ref SHARED_PAGE_BUFFER_RELEASE_CALLBACK: SpinLock<Option<usize>> = SpinLock::new(None);
    static ref IS_INITED: SpinLock<bool> = SpinLock::new(false);
    static ref GLOBAL_VIRTUAL_PAGE_LFU_CACHE_MGR: SpinLock<Option<usize>> = SpinLock::new(None);
    static ref GLOBAL_VIRTUAL_PAGE_LFU_CACHE_ALLOCATOR: SpinLock<Option<Allocator>> = SpinLock::new(None);
}

/// 初始化全局虚拟页LFU缓存分配器
pub fn init_global_virtual_page_lfu_cache_allocator<C, O, B, D, P>(rt: MultiTaskRuntime<()>,
                                                                   capacity: usize,
                                                                   min_capacity: usize,
                                                                   max_capacity: usize,
                                                                   timeout: usize) -> bool
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O> {
    let mut locked = IS_INITED.lock();

    if !*locked {
        //未初始化，则初始化，并返回成功
        let mut allocator = Allocator::new(capacity);

        let callback = SharedPageBufferReleaseCallback::<C, O, B, D, P>::new(rt);
        *SHARED_PAGE_BUFFER_RELEASE_CALLBACK.lock() = Some(Box::into_raw(Box::new(callback.clone())) as usize);

        let mgr = AssetMgr::new(callback,
                                true,
                                capacity,
                                timeout);
        *GLOBAL_VIRTUAL_PAGE_LFU_CACHE_MGR.lock() = Some(Arc::into_raw(mgr.clone()) as usize);

        allocator.register(mgr, min_capacity, max_capacity);
        *GLOBAL_VIRTUAL_PAGE_LFU_CACHE_ALLOCATOR.lock() = Some(allocator);
        *locked = true;
        true
    } else {
        //已初始化，则返回失败
        false
    }
}

/// 启动全局虚拟页LFU缓存分配器的自动整理，需要异步运行时和整理间隔时间，单位为毫秒
/// 创建了所有全局虚拟页LFU缓存后，再启动全局虚拟页LFU缓存分配器的自动整理
pub fn startup_auto_collect<P, RT>(rt: RT,
                                   interval: usize) -> bool
    where P: AsyncTaskPoolExt<()> + AsyncTaskPool<(), Pool = P>,
          RT: AsyncRuntime<(), Pool = P>, {
    let locked = IS_INITED.lock();
    if !*locked {
        //未初始化，则立即返回失败
        return false;
    }

    if let Some(allocator) = GLOBAL_VIRTUAL_PAGE_LFU_CACHE_ALLOCATOR.lock().take() {
        //未启动全局虚拟页LFU缓存分配器的自动整理，则立即启动
        allocator.auto_collect(rt, interval);
    }

    true
}

/// 注册虚拟页缓冲的释放处理器
/// 只有初始化全局虚拟页LFU缓存分配器后，才允许注册
pub fn register_release_handler<C, O, B, D, P>(uid: u32,
                                               handler: Arc<dyn SharedPageRelease<C, O, B, D, P>>) -> Option<Arc<dyn SharedPageRelease<C, O, B, D, P>>>
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O> {
    if let Some(ptr) = *SHARED_PAGE_BUFFER_RELEASE_CALLBACK.lock() {
        //虚拟页的共享页缓冲释放回调存在
        let boxed = unsafe {
            Box::from_raw(ptr as *mut SharedPageBufferReleaseCallback<C, O, B, D, P>)
        };

        let result = boxed.register_handler(uid, handler);
        Box::into_raw(boxed); //避免提前释放

        result
    } else {
        //虚拟页的共享页缓冲释放回调不存在
        panic!("Register release handler failed, uid: {}, reason: handler not exist", uid);
    }
}

///
/// 虚拟页的共享页缓冲释放
///
pub trait SharedPageRelease<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>: Send + Sync + 'static {
    /// 安全的异步释放指定的虚拟页的共享页缓冲
    fn release(&self,
               page_id: u128,
               buffer: Arc<PageBuffer<C, O, B, D, P>>,
               guard: GarbageGuard<SharedPageBuffer<C, O, B, D, P>>) -> BoxFuture<'static, ()>;
}

///
/// 虚拟页的共享页缓冲
///
#[derive(Clone)]
pub struct SharedPageBuffer<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>(Arc<PageBuffer<C, O, B, D, P>>);

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Deref for SharedPageBuffer<C, O, B, D, P> {
    type Target = Arc<PageBuffer<C, O, B, D, P>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Asset for SharedPageBuffer<C, O, B, D, P> {
    type Key = u128;

    fn size(&self) -> usize {
        self.0.buf_size()
    }
}

///
/// 虚拟页的共享页缓冲释放回调
///
pub struct SharedPageBufferReleaseCallback<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>(Arc<InnerSharedPageBufferReleaseCallback<C, O, B, D, P>>);

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Clone for SharedPageBufferReleaseCallback<C, O, B, D, P> {
    fn clone(&self) -> Self {
        SharedPageBufferReleaseCallback(self.0.clone())
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Garbageer<SharedPageBuffer<C, O, B, D, P>> for SharedPageBufferReleaseCallback<C, O, B, D, P> {
    fn garbage_ref(&self,
                   k: &u128,
                   v: &SharedPageBuffer<C, O, B, D, P>,
                   _timeout: u64,
                   guard: GarbageGuard<SharedPageBuffer<C, O, B, D, P>>) {
        let uid = *k;
        let buffer = (*v).clone();
        if let Some(ptr) = *SHARED_PAGE_BUFFER_RELEASE_CALLBACK.lock() {
            //虚拟页的共享页缓冲释放回调存在
            self.0.rt.spawn(self.0.rt.alloc(), async move {
                if buffer.deltas_len() > 0 {
                    //待释放的虚拟页是脏页，则需要立即强制同步
                    let boxed = unsafe {
                        Box::from_raw(ptr as *mut SharedPageBufferReleaseCallback<C, O, B, D, P>)
                    };

                    let page_id = PageId::new(uid);
                    if let Some(handler) = boxed.get_handler(&page_id.owner_uid()) {
                        //待释放的虚拟页存在对应的释放处理器
                        handler.release(uid, buffer, guard).await;
                        Box::into_raw(boxed); //避免提前释放
                    } else {
                        //待释放的虚拟页不存在对应的释放处理器
                        Box::into_raw(boxed); //避免提前释放
                        panic!("Release shared page buffer failed, manager_id: {:?}, page_id: {:?}, reason: handler not exist", page_id.owner_uid(), page_id);
                    }
                }
            });
        } else {
            //虚拟页的共享页缓冲释放回调不存在
            warn!("Garbage ref warning, page_id: {}, reason: release callback not exist", uid);
        }
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> SharedPageBufferReleaseCallback<C, O, B, D, P> {
    /// 构建虚拟页的共享页缓冲释放回调
    pub fn new(rt: MultiTaskRuntime<()>) -> Self {
        let inner = InnerSharedPageBufferReleaseCallback {
            rt,
            handlers: SpinLock::new(XHashMap::default()),
        };

        SharedPageBufferReleaseCallback(Arc::new(inner))
    }

    /// 获取指定虚拟页管理器唯一id的虚拟页共享缓冲释放处理器
    pub fn get_handler(&self, uid: &u32) -> Option<Arc<dyn SharedPageRelease<C, O, B, D, P>>> {
        if let Some(handler) = self.0.handlers.lock().get(uid) {
            Some(handler.clone())
        } else {
            None
        }
    }

    /// 注册指定虚拟页管理器唯一id的虚拟页共享缓冲释放处理器
    pub fn register_handler(&self,
                            uid: u32,
                            handler: Arc<dyn SharedPageRelease<C, O, B, D, P>>) -> Option<Arc<dyn SharedPageRelease<C, O, B, D, P>>> {
        self
            .0
            .handlers
            .lock()
            .insert(uid, handler)
    }
}

struct InnerSharedPageBufferReleaseCallback<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> {
    rt:         MultiTaskRuntime<()>,                                               //异步运行时
    handlers:   SpinLock<XHashMap<u32, Arc<dyn SharedPageRelease<C, O, B, D, P>>>>, //虚拟页共享缓冲释放处理器表
}

///
/// 虚拟页LFU缓存
///
pub struct VirtualPageLFUCache<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>(Arc<InnerVirtualPageLFUCache<C, O, B, D, P>>);

unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Send for VirtualPageLFUCache<C, O, B, D, P> {}
unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Sync for VirtualPageLFUCache<C, O, B, D, P> {}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Clone for VirtualPageLFUCache<C, O, B, D, P> {
    fn clone(&self) -> Self {
        VirtualPageLFUCache(self.0.clone())
    }
}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> VirtualPageCachingStrategy<C, O, B, D, P> for VirtualPageLFUCache<C, O, B, D, P> {
    type Iter = VirtualPageLFUCacheDirtyIterator<C, O, B, D, P>;

    fn is_full(&self) -> bool {
        self.0.mgr.get_capacity() <= self.0.mgr.size()
    }

    fn contains(&self, page_id: &u128) -> bool {
        self.0.mgr.contains_key(page_id)
    }

    fn len(&self) -> usize {
        self.0.mgr.len()
    }

    fn size(&self) -> u64 {
        self.0.mgr.size() as u64
    }

    fn adjust_size(&self, page_id: &u128, size: isize) {
        if let Some(handle) = self.0.mgr.get(page_id) {
            handle.adjust_size(size);
        }
    }

    fn get(&self, page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>> {
        if let Some(handle) = self.0.mgr.get(page_id) {
            Some((*handle).clone())
        } else {
            None
        }
    }

    fn load(&self,
            page_id: u128,
            loading: BoxFuture<'static, Result<PageBuffer<C, O, B, D, P>>>,
            loaded: Box<dyn Fn(&PageBuffer<C, O, B, D, P>) + Send + 'static>)
            -> BoxFuture<'static, Result<Arc<PageBuffer<C, O, B, D, P>>>> {
        let mgr = self.0.mgr.clone();
        async move {
            match AssetMgr::load(&mgr, &page_id) {
                LoadResult::Ok(handle) => {
                    //指定的页缓冲已加载
                    Ok((*handle).clone())
                },
                LoadResult::Wait(wait) => {
                    //指定的页缓冲正在加载
                    match wait.await {
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            //指定的页不存在
                            Err(e)
                        },
                        Err(e) => {
                            //加载指定的页错误
                            Err(e)
                        },
                        Ok(handle) => {
                            //指定的页加载成功
                            Ok((*handle).clone())
                        },
                    }
                },
                LoadResult::Receiver(receiver) => {
                    //指定的页缓冲还未开始加载，且需要立即异步加载
                    match loading.await {
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            //指定的页不存在
                            receiver.receive(page_id, Err(Error::new(e.kind(), format!("Load page buffer failed, page_id: {:?}, reason: {:?}", page_id, e)))).await; //通知异步加载失败
                            Err(e)
                        },
                        Err(e) => {
                            //异步加载指定的页缓冲失败
                            receiver.receive(page_id, Err(Error::new(e.kind(), format!("Async load page buffer failed, page_id: {:?}, reason: {:?}", page_id, e)))).await; //通知异步加载失败
                            Err(e)
                        },
                        Ok(buffer) => {
                            //异步加载指定的页缓冲成功
                            loaded(&buffer); //加载后处理

                            //通知异步加载成功
                            match receiver.receive(page_id, Ok(SharedPageBuffer(Arc::new(buffer)))).await {
                                Err(e) => {
                                    Err(Error::new(ErrorKind::Other, format!("Async load page buffer failed, page_id: {:?}, reason: {:?} receive", page_id, e)))
                                },
                                Ok(handle) => {
                                    Ok((*handle).clone())
                                },
                            }
                        },
                    }
                },
            }
        }.boxed()
    }

    fn insert(&self, page_id: u128, buffer: PageBuffer<C, O, B, D, P>)
              -> Option<Arc<PageBuffer<C, O, B, D, P>>> {
        let _ = self
            .0
            .mgr
            .insert(page_id, SharedPageBuffer(Arc::new(buffer))); //不握住Handler，保证即时回收
        None
    }

    /// 虚拟页LFU缓存忽略外部移除缓存
    fn remove(&self, _page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>> {
        None
    }

    /// 虚拟页LFU缓存忽略外部移除所有缓存
    fn clear(&self) -> usize {
        0
    }

    /// 获取脏页的共享缓冲迭代器
    fn iter(&self) -> Self::Iter {
        let mut keys: Vec<u128> = Vec::new();
        self.0.mgr.cache_iter(&mut keys,
                              |keys: &mut Vec<u128>, key: &u128, val: &SharedPageBuffer<C, O, B, D, P>, time: u64| {
                                  if val.0.deltas_len() > 0 {
                                      //当前页缓冲是脏页，则记录页缓冲的页唯一id
                                      keys.push(*key);
                                  }
                              });

        VirtualPageLFUCacheDirtyIterator {
            mgr: self.0.mgr.clone(),
            keys,
            marker: PhantomData,
        }
    }
}

/*
* 虚拟页LFU缓存同步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> VirtualPageLFUCache<C, O, B, D, P> {
    /// 构建虚拟页LFU缓存
    pub fn new() -> Self {
        if let Some(ptr) = *GLOBAL_VIRTUAL_PAGE_LFU_CACHE_MGR.lock() {
            //全局虚拟页LFU缓存资源管理器已初始化
            let mgr = unsafe {
                Arc::from_raw(ptr as *const AssetMgr<SharedPageBuffer<C, O, B, D, P>, SharedPageBufferReleaseCallback<C, O, B, D, P>>)
            };

            let inner = InnerVirtualPageLFUCache {
                mgr: mgr.clone(),
            };
            Arc::into_raw(mgr); //避免提前释放

            VirtualPageLFUCache(Arc::new(inner))
        } else {
            //全局虚拟页LFU缓存分配器未初始化，则立即抛出异常
            panic!("Create VirtualPageLFUCache failed, reason: uninitialized global cache allocator");
        }
    }

    /// 判断缓存当前是否足够空闲
    pub fn is_enough_empty(&self) -> bool {
        self.0.mgr.size() as f64 / self.0.mgr.get_capacity() as f64 <= 0.75
    }
}

// 内部虚拟页LFU缓存
struct InnerVirtualPageLFUCache<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> {
    //资源管理器
    mgr:    Share<AssetMgr<SharedPageBuffer<C, O, B, D, P>, SharedPageBufferReleaseCallback<C, O, B, D, P>>>,
}

/// 虚拟页LFU缓存的脏页迭代器
pub struct VirtualPageLFUCacheDirtyIterator<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> {
    //资源管理器
    mgr:    Share<AssetMgr<SharedPageBuffer<C, O, B, D, P>, SharedPageBufferReleaseCallback<C, O, B, D, P>>>,
    //脏页唯一id列表
    keys:   Vec<u128>,
    marker: PhantomData<(C, O, B, D, P)>,
}

unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Send for VirtualPageLFUCacheDirtyIterator<C, O, B, D, P> {}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> Iterator for VirtualPageLFUCacheDirtyIterator<C, O, B, D, P> {
    type Item = Arc<PageBuffer<C, O, B, D, P>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(key) = self.keys.pop() {
            if let Some(handle) = self.mgr.get(&key) {
                Some((*handle).clone())
            } else {
                //指定虚拟页唯一id的页缓冲不存在，则立即返回空
                None
            }
        } else {
            //已迭代完成，则立即返回空
            None
        }
    }
}