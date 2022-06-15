use std::marker::PhantomData;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use std::result::Result as GenResult;
use std::cmp::Ordering as CmpOrdering;
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering}};
use std::sync::atomic::AtomicBool;

use futures::future::{FutureExt, BoxFuture};
use dashmap::{DashMap, iter::Iter};
use bytes::{Buf, BufMut};
use log::debug;

use pi_async::{lock::{spin_lock::SpinLock, mutex_lock::{Mutex, MutexGuard}},
              rt::multi_thread::MultiTaskRuntime};

use crate::vpm::{VirtualPageWriteDelta, VirtualPageBuf};

///
/// 页缓冲已初始化
///
pub const PAGE_INITED: u8 = 0;

///
/// 页缓冲正在同步脏页
///
pub const PAGE_FLUSHING: u8 = 1;

///
/// 页缓冲正在同步脏页
///
pub const PAGE_SYNCING: u8 = 2;

///
/// 页缓冲已同步脏页完成
///
pub const PAGE_SYNCED: u8 = 3;

///
/// 页面缓冲正在整理
///
pub const PAGE_COLLECTING: u8 = 4;

///
/// 虚拟页缓存策略，用于缓存虚拟页的基页和写增量，并负责虚拟页的缓存管理
///
pub trait VirtualPageCachingStrategy<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
>: Clone + Send + Sync + 'static {
    type Iter: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static;

    /// 判断虚拟页缓存是否已满
    fn is_full(&self) -> bool;

    /// 判断指定的页id的页缓冲是否存在
    fn contains(&self, page_id: &u128) -> bool;

    /// 获取虚拟页缓存的虚拟页缓冲数量
    fn len(&self) -> usize;

    /// 获取虚拟页缓存大小
    fn size(&self) -> u64;

    /// 调整虚拟页缓存大小
    fn adjust_size(&self, page_id: &u128, size: isize);

    /// 获取指定页id的页缓冲的只读引用
    fn get(&self, page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>>;

    /// 获取指定页id的页缓冲的只读引用，如果指定的页缓冲不存在，则执行异步加载过程，并返回结果
    fn load(&self,
            page_id: u128,
            loading: BoxFuture<'static, Result<PageBuffer<C, O, B, D, P>>>,
            loaded: Box<dyn Fn(&PageBuffer<C, O, B, D, P>) + Send + 'static>)
        -> BoxFuture<'static, Result<Arc<PageBuffer<C, O, B, D, P>>>>;

    /// 插入一个指定页id的页缓冲
    fn insert(&self, page_id: u128, buffer: PageBuffer<C, O, B, D, P>)
        -> Option<Arc<PageBuffer<C, O, B, D, P>>>;

    /// 移除一个指定页id的页缓冲
    fn remove(&self, page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>>;

    /// 清空所有页缓冲，返回被移除的页缓冲数量
    fn clear(&self) -> usize;

    /// 获取页缓冲的迭代器
    fn iter(&self) -> Self::Iter;
}

///
/// 虚拟页缓冲池
///
pub struct VirtualPageBufferPool<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
>(Arc<InnerVirtualPageBufPool<C, O, B, D, P, I, M>>);

unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> Send for VirtualPageBufferPool<C, O, B, D, P, I, M> {}
unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> Sync for VirtualPageBufferPool<C, O, B, D, P, I, M> {}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> Clone for VirtualPageBufferPool<C, O, B, D, P, I, M> {
    fn clone(&self) -> Self {
        VirtualPageBufferPool(self.0.clone())
    }
}

/*
* 虚拟页缓冲池同步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> VirtualPageBufferPool<C, O, B, D, P, I, M> {
    /// 构建指定异步运行时，虚拟页缓存和页的写增量缓冲大小限制的虚拟页缓冲池
    pub fn new(rt: MultiTaskRuntime<()>,
               cache: M,
               limit: usize) -> Self {
        let inner = InnerVirtualPageBufPool {
            rt,
            cache,
            limit,
            marker: PhantomData,
        };

        VirtualPageBufferPool(Arc::new(inner))
    }

    /// 判断指定页id的页缓冲是否存在
    pub fn contains(&self, page_id: &u128) -> bool {
        self.0.cache.contains(page_id)
    }

    /// 判断指定页id的页缓冲中的页是否是脏页
    pub fn is_dirty(&self, page_id: &u128) -> bool {
        self.deltas_len(page_id) > 0
    }

    /// 获取页缓冲的写增量大小限制
    pub fn get_limit(&self) -> usize {
        self.0.limit
    }

    /// 获取所有缓冲页的数量
    pub fn len(&self) -> usize {
        self.0.cache.len()
    }

    /// 获取所有缓冲页的大小
    pub fn size(&self) -> u64 {
        self.0.cache.size()
    }

    /// 获取指定页id的页缓冲的写增量列表长度
    pub fn deltas_len(&self, page_id: &u128) -> usize {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.deltas.lock().len()
        } else {
            0
        }
    }

    /// 获取指定页id的页缓冲的写增量大小
    pub fn deltas_size(&self, page_id: &u128) -> usize {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.current.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// 获取指定页id的页缓冲的写增量大小的限制
    pub fn deltas_size_limit(&self, page_id: &u128) -> usize {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.limit
        } else {
            0
        }
    }

    /// 获取指定页id的页缓冲的脏页过期时间
    pub fn dirty_expired(&self, page_id: &u128) -> u64 {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.dirty_expired.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// 获取指定页id的页缓冲的过期时间
    pub fn page_expired(&self, page_id: &u128) -> u64 {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.expired.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// 调整指定页id的虚拟页缓冲的大小
    pub fn adjust_page_buffer_size(&self, page_id: &u128, size: isize) {
        self.0.cache.adjust_size(page_id, size);
    }

    /// 获取指定页id的虚拟页缓冲的只读引用
    #[inline]
    pub fn get_page_buffer(&self, page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>> {
        self.0.cache.get(page_id)
    }

    /// 将指定页id的页缓冲加入虚拟页缓冲池
    pub fn join_page(&self,
                     page_id: u128,
                     buffer: PageBuffer<C, O, B, D, P>) {
        self.0.cache.insert(page_id, buffer);
    }

    /// 读指定页id的页缓冲的基础页的内容，并更新页缓冲的过期时间
    pub fn read_base_page(&self,
                          page_id: &u128,
                          expired: u64) -> Option<O> {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.expired.store(expired, Ordering::Relaxed);
            Some(buf
                .page
                .lock()
                .read_page())
        } else {
            None
        }
    }

    /// 将指定页id的写增量加入页缓冲的写增量列表，并更新页缓冲的脏页过期时间和页缓冲的过期时间
    pub fn join_delta(&self,
                      page_id: &u128,
                      delta: D,
                      dirty_expired: u64,
                      expired: u64) -> bool {
        if let Some(buf) = self.get_page_buffer(page_id) {
            buf.deltas.lock().push_back(delta);
            buf.dirty_expired.store(dirty_expired, Ordering::Relaxed);
            buf.expired.store(expired, Ordering::Relaxed);

            true
        } else {
            false
        }
    }

    /// 从虚拟页缓冲池中移除指定页id的页缓冲
    pub fn remove_page(&self, page_id: &u128) -> Option<Arc<PageBuffer<C, O, B, D, P>>> {
        self.0.cache.remove(page_id)
    }

    /// 获取虚拟页缓冲池的缓存迭代器
    pub fn iter(&self) -> I {
        self.0.cache.iter()
    }
}

/*
* 虚拟页缓冲池异步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> VirtualPageBufferPool<C, O, B, D, P, I, M> {
    /// 获取指定页id的虚拟页缓冲的只读引用，如果指定的虚拟页缓冲不存在，则异步加载后返回
    #[inline]
    pub async fn load_page_buffer(&self,
                                  page_id: u128,
                                  loading: BoxFuture<'static, Result<PageBuffer<C, O, B, D, P>>>,
                                  loaded: Box<dyn Fn(&PageBuffer<C, O, B, D, P>) + Send + 'static>)
        -> Result<Arc<PageBuffer<C, O, B, D, P>>> {
        self.0.cache.load(page_id, loading, loaded).await
    }
}

// 内部虚拟页缓冲池
struct InnerVirtualPageBufPool<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
> {
    rt:         MultiTaskRuntime<()>,                   //运行时
    cache:      M,                                      //页缓冲的缓存
    limit:      usize,                                  //页的写增量缓冲大小限制，单位B
    marker:     PhantomData<(C, O, B, D, P, I, M)>,
}

// 页缓冲
pub struct PageBuffer<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> {
    page:           SpinLock<P>,            //基页
    deltas:         SpinLock<VecDeque<D>>,  //页的写增量缓冲
    status:         Mutex<u8>,              //页缓冲状态
    current:        AtomicUsize,            //页的写增量缓冲大小，单位B
    limit:          usize,                  //页的写增量缓冲大小，单位B
    dirty_expired:  AtomicU64,              //脏页的过期时间，为0表示永不过期，单位ms
    expired:        AtomicU64,              //页的过期时间，为0表示永不过期，单位ms
}

/*
* 页缓冲同步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static, 
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> PageBuffer<C, O, B, D, P> {
    /// 构建页缓冲
    pub fn new(page: P, limit: usize) -> Self {
        PageBuffer {
            page: SpinLock::new(page),
            deltas: SpinLock::new(VecDeque::new()),
            status: Mutex::new(PAGE_INITED),
            current: AtomicUsize::new(0),
            limit,
            dirty_expired: AtomicU64::new(0),
            expired: AtomicU64::new(0),
        }
    }

    /// 获取页缓冲的基页的只读引用
    #[inline]
    pub fn base_page(&self) -> &SpinLock<P> {
        &self.page
    }

    /// 线程安全的设置页缓冲的基页
    #[inline]
    pub fn set_base_page(&self, new_base_page: P) {
        *self.page.lock() = new_base_page;
    }

    /// 复制页缓冲的基页
    #[inline]
    pub fn copy_base_page(&self) -> P {
        self
            .page
            .lock()
            .clone()
    }

    /// 获取页缓冲的写增量缓冲长度
    #[inline]
    pub fn deltas_len(&self) -> usize {
        self.deltas.lock().len()
    }

    /// 获取页缓冲的写增量缓冲大小
    #[inline]
    pub fn deltas_size(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// 获取页缓冲的写增量的限制大小，单位B
    #[inline]
    pub fn deltas_limit(&self) -> usize {
        self.limit
    }

    /// 获取页缓冲的写增量缓冲
    #[inline]
    pub fn get_deltas(&self) -> &SpinLock<VecDeque<D>> {
        &self.deltas
    }

    /// 追加指定写增量到当前页缓冲的写增量缓冲中
    #[inline]
    pub fn append_delta(&self, delta: D) {
        self
            .current
            .fetch_add(delta.size(), Ordering::Relaxed); //增加页的写增量缓冲大小

        self
            .deltas
            .lock()
            .push_back(delta);
    }

    /// 获取页缓冲的大小，包括基页大小和写增量大小，单位B
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.page.lock().page_size() + self.deltas_size()
    }

    /// 获取页缓冲的脏页过期时间，单位ms
    #[inline]
    pub fn get_dirty_expired(&self) -> u64 {
        self.dirty_expired.load(Ordering::Relaxed)
    }

    /// 设置页缓冲的脏页过期时间，单位ms
    #[inline]
    pub fn set_dirty_expired(&self, dirty_expired: u64) {
        self.dirty_expired.store(dirty_expired, Ordering::Relaxed);
    }

    /// 获取页缓冲的过期时间，单位ms
    #[inline]
    pub fn get_expired(&self) -> u64 {
        self.expired.load(Ordering::Relaxed)
    }

    /// 设置页缓冲的过期时间，单位ms
    pub fn set_expired(&self, expired: u64) {
        self.expired.store(expired, Ordering::Relaxed);
    }
}

/*
* 页缓冲异步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
> PageBuffer<C, O, B, D, P> {
    /// 获取页缓冲的状态
    #[inline]
    pub async fn get_status(&self) -> u8 {
        *self.status.lock().await
    }

    /// 设置页缓冲的状态为开始刷新，成功返回状态守护者
    #[inline]
    pub async fn start_flush(&self) -> PageBufferStatusGuard {
        let mut locked = self.status.lock().await;
        match *locked {
            PAGE_INITED => {
                *locked = PAGE_FLUSHING;
                PageBufferStatusGuard(locked)
            },
            current => {
                //设置失败，则立即抛出异常
                panic!("Start flush failed, current: {}, reason: invalid status", current);
            },
        }
    }

    /// 设置页缓冲的状态为开始同步，成功返回空状态守护者
    #[inline]
    pub async fn start_sync(&self) -> PageBufferStatusGuard {
        let mut locked = self.status.lock().await;
        match *locked {
            PAGE_INITED => {
                *locked = PAGE_SYNCING;
                PageBufferStatusGuard(locked)
            },
            current => {
                //设置失败，则返回当前页缓冲的状态
                panic!("Start sync failed, current: {}, reason: invalid status", current);
            },
        }
    }
}

///
/// 页缓冲状态守护者
///
pub struct PageBufferStatusGuard(MutexGuard<u8>);

impl Drop for PageBufferStatusGuard {
    fn drop(&mut self) {
        //重置页缓冲的状态
        *self.0 = PAGE_INITED;
    }
}




