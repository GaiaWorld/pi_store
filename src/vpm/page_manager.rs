use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::collections::btree_map::{Entry as BtreeMapEntry, BTreeMap};
use std::time::Instant;
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use futures::FutureExt;
use async_lock::Mutex;
use dashmap::{DashMap, mapref::entry::Entry};
use bytes::BufMut;
use log::debug;

use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntime};
use pi_guid::Guid;

use crate::vpm::{VirtualPageWriteDelta, VirtualPageBuf, PageId, VirtualPageWriteCmd, WriteIndex,
                 page_cache::{VirtualPageLFUCache, VirtualPageLFUCacheDirtyIterator},
                 page_table::VirtualPageTable,
                 page_pool::{VirtualPageCachingStrategy, VirtualPageBufferPool, PageBuffer}};
use crate::devices::{EMPTY_BLOCK,
                     EMPTY_BLOCK_LOCATION,
                     DeviceDetail,
                     DeviceValueType,
                     DeviceDetailMap,
                     DeviceStatus,
                     BlockDevice,
                     BlockLocation};

///
/// 默认的初始虚拟页唯一id
///
const DEFAULT_INIT_PAGE_UID: u64 = 1;

///
/// 默认的虚拟页表的日志文件大小限制，单位B
///
const DEFAULT_TABLE_LOG_FILE_LIMIT: usize = 32 * 1024 * 1024;

///
/// 默认的虚拟页表的加载缓冲区大小，单位B
///
const DEFAULT_TABLE_LOAD_BUF_LEN: u64 = 1024 * 1024;

///
/// 默认的页缓冲的缓存整理时间间隔，单位ms
///
const DEFAULT_TABLE_DELAY_TIMEOUT: usize = 1;

///
/// 默认的页写增量缓冲大小限制，单位B
///
const DEFAULT_PAGE_DELTAS_SIZE_LIMIT: usize = 4096;

///
/// 默认的页缓冲的定时刷新时间间隔，单位ms
///
const DEFAULT_FLUSH_INTERVAL: usize = 6000;

///
/// 默认的页缓冲脏页的过期时间，单位ms
///
const DEFAULT_DIRTY_EXPIRED: u64 = u64::MAX;

///
/// 默认的页缓冲的过期时间，单位ms
///
const DEFAULT_EXPIRED: u64 = 5000;

///
/// 虚拟页管理器构建器
///
pub struct VirtualPageManagerBuilder<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static = VirtualPageLFUCacheDirtyIterator<C, O, B, D, P>,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> = VirtualPageLFUCache<C, O, B, D, P>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static = Guid,
    BS: Debug + Clone + Hash + Send + Sync + 'static = DeviceStatus,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BD: DeviceDetail<Key = BK, Val = BV> = DeviceDetailMap,
> {
    uid:                    u32,                                                          //虚拟页管理器唯一id
    rt:                     MultiTaskRuntime<()>,                                           //运行时
    table_path:             PathBuf,                                                        //虚拟页的路径
    init_page_uid:          u64,                                                            //初始虚拟页唯一id
    table_log_file_limit:   usize,                                                          //虚拟页表的日志文件大小限制，单位B
    table_load_buf_len:     u64,                                                            //虚拟页表的加载缓冲区大小，单位B
    table_delay_timeout:    usize,                                                          //虚拟页表的延迟同步间隔时长，单位ms
    cache:                  M,                                                              //页缓冲的缓存
    limit:                  usize,                                                          //页缓冲的写增量缓冲大小限制，单位B
    sync_interval:          usize,                                                          //页缓冲的定时同步时间间隔，单位ms
    marker:                 PhantomData<(C, O, B, D, P, I, M, BU, BS, BK, BV, BD)>,
}

/*
* 虚拟页管理器构建器同步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> VirtualPageManagerBuilder<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
    /// 构建一个虚拟页管理器构建器
    pub fn new<Pa: AsRef<Path>>(uid: u32,
                                rt: MultiTaskRuntime<()>,
                                path: Pa,
                                cache: M) -> Self {
        let table_path = path.as_ref().to_path_buf();

        VirtualPageManagerBuilder {
            uid,
            rt,
            table_path,
            init_page_uid: DEFAULT_INIT_PAGE_UID,
            table_log_file_limit: DEFAULT_TABLE_LOG_FILE_LIMIT,
            table_load_buf_len: DEFAULT_TABLE_LOAD_BUF_LEN,
            table_delay_timeout: DEFAULT_TABLE_DELAY_TIMEOUT,
            cache,
            limit: DEFAULT_TABLE_LOG_FILE_LIMIT,
            sync_interval: DEFAULT_FLUSH_INTERVAL,
            marker: PhantomData,
        }
    }

    /// 设置初始虚拟页唯一id
    pub fn set_init_page_uid(mut self, init_page_uid: u64) -> Self {
        self.init_page_uid = init_page_uid;
        self
    }

    /// 设置虚拟页表的日志文件大小限制
    pub fn set_table_log_file_limit(mut self, table_log_file_limit: usize) -> Self {
        self.table_log_file_limit = table_log_file_limit;
        self
    }

    /// 设置虚拟页表的加载缓冲区大小
    pub fn set_table_load_buf_len(mut self, table_load_buf_len: u64) -> Self {
        self.table_load_buf_len = table_load_buf_len;
        self
    }

    /// 设置页缓冲的缓存整理间隔
    pub fn set_table_delay_timeout(mut self, table_delay_timeout: usize) -> Self {
        self.table_delay_timeout = table_delay_timeout;
        self
    }

    /// 设置页缓冲的写增量大小限制
    pub fn set_pool_buffer_delta_limit(mut self, set_pool_buffer_delta_limit: usize) -> Self {
        self.limit = set_pool_buffer_delta_limit;
        self
    }

    /// 设置页面缓冲的定时同步时间间隔
    pub fn set_sync_interval(mut self, sync_interval: usize) -> Self {
        self.sync_interval = sync_interval;
        self
    }
}

/*
* 虚拟页管理器构建器异步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> VirtualPageManagerBuilder<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
    /// 构建一个虚拟页管理器
    pub async fn build(mut self) -> VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
        let uid = self.uid;
        let rt = self.rt;
        let table = VirtualPageTable::new(rt.clone(),
                                          self.table_path,
                                          self.init_page_uid,
                                          self.table_log_file_limit,
                                          self.table_load_buf_len,
                                          true,
                                          self.table_delay_timeout).await;

        let pool = VirtualPageBufferPool::new(rt.clone(),
                                              self.cache,
                                              self.limit);
        let reserved = Arc::new(DashMap::new());
        let devices = Arc::new(DashMap::new());
        let sync_interval = self.sync_interval;
        let write_cmd_index = AtomicU64::new(1);
        let write_cmd_buffer = Arc::new(Mutex::new(BTreeMap::new()));

        let inner = InnerVirtualPageManager {
            uid,
            rt,
            table,
            pool,
            reserved,
            devices,
            sync_interval,
            write_cmd_index,
            write_cmd_buffer,
        };

        VirtualPageManager(Arc::new(inner))
    }
}

///
/// 虚拟页管理器
/// 虚拟页管理器支持单个虚拟页的原子操作
/// 虚拟页管理器不支持虚拟页的事务操作，如果需要事务特性，需要外部提供
///
pub struct VirtualPageManager<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static = VirtualPageLFUCacheDirtyIterator<C, O, B, D, P>,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> = VirtualPageLFUCache<C, O, B, D, P>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static = Guid,
    BS: Debug + Clone + Hash + Send + Sync + 'static = DeviceStatus,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BD: DeviceDetail<Key = BK, Val = BV> = DeviceDetailMap,
>(Arc<InnerVirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD>>);

unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> Send for VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {}
unsafe impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> Sync for VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {}

impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> Clone for VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
    fn clone(&self) -> Self {
        VirtualPageManager(self.0.clone())
    }
}

/*
* 虚拟页管理器同步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
    /// 获取虚拟页管理器唯一id
    pub fn get_uid(&self) -> u32 {
        self.0.uid
    }

    /// 获取所有当前已换入的虚拟页数量
    pub fn len(&self) -> usize {
        self.0.pool.len()
    }

    /// 获取所有当前已换入的虚拟页的大小
    pub fn size(&self) -> u64 {
        self.0.pool.size()
    }

    /// 检查指定位置的块设备是否存在
    pub fn contains_device(&self, offset: u32) -> bool {
        if offset == 0 {
            //默认0位置的块设备就是虚拟页管理器
            true
        } else {
            self.0.devices.contains_key(&offset)
        }
    }

    /// 获取所有块设备的位置
    pub fn devices(&self) -> Vec<u32> {
        let mut offsets = Vec::with_capacity(self.0.devices.len());
        offsets.push(0); //默认0位置的块设备就是虚拟页管理器

        let mut iterator = self.0.devices.iter();
        while let Some(entry) = iterator.next() {
            offsets.push(*entry.key());
        }

        offsets
    }

    /// 为当前虚拟页管理器增加指定位置的块设备，如果指定位置块设备存在，则返回增加块设备失败
    pub fn join_device(&self,
                       offset: u32,
                       device: Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = B>>) -> bool {
        if offset == 0 {
            //不允许使用0位置来增加块设备
            return false;
        }

        match self.0.devices.entry(offset) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                //指定位置块设备不存在，则增加指定的块设备到指定位置
                v.insert(device);
                true
            }
        }
    }

    /// 移除指定位置的块设备，如果指定位置有块设备且完全空闲，则移除并返回块设备
    pub fn remove_device(&self, offset: u32)
        -> Option<Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = B>>> {
        if offset == 0 {
            //不允许移除0位置的块设备
            return None;
        }

        match self.0.devices.entry(offset) {
            Entry::Vacant(_) => None,
            Entry::Occupied(mut o) => {
                let device = o.get_mut();
                if device.is_full_free() {
                    //当前块设备完全空闲，则移除当前块设备
                    Some(o.remove())
                } else {
                    //当前块设备非完全空闲，则不允许移除当前块设备
                    None
                }
            },
        }
    }

    /// 同步非阻塞的分配指定大小的页，在指定块设备上为页保留指定大小的分配块空间，并延迟到访问页时再提交分配，
    /// 返回非空页id，分配0大小的页则返回空页id，
    /// 分配虚拟页唯一id失败则立即抛出异常
    pub fn alloc_page(&self,
                      offset: u32,
                      size: usize) -> PageId {
        if size == 0 {
            //分配大小为0的页，则立即返回空页
            return PageId::empty();
        }

        let page_id = create_page_id(self.0.uid,
                                     offset,
                                     self.0.table.alloc_page_uid());
        if let Some(_) = register_page(&self.0.table,
                                       page_id.clone(),
                                       EMPTY_BLOCK_LOCATION) {
            //不允许注册已存在的虚拟页
            panic!("Lazy alloc page failed, page_id: {:?}, reason: conflict page", page_id);
        }

        if let Some(_) = register_reserved_page(&self.0.reserved,
                                                page_id.clone(),
                                                size) {
            //不允许注册已存在的保留的虚拟页
            panic!("Lazy alloc page failed, page_id: {:?}, reason: conflict reserved page", page_id);
        }

        page_id
    }

    /// 同步非阻塞的释放指定的虚拟页，将虚拟页表中指定页id对应的块位置设置为空块，并通知对应的块设备释放指定的块位置
    /// 实际的释放，将延迟到虚拟页表整理和块设备整理时
    pub fn free_page(&self, page_id: PageId) -> bool {
        if page_id.is_empty() || !self.0.table.contains_page(page_id.as_ref()) {
            //忽略空页或不存在的页的释放
            return true;
        }

        //同步标记指定虚拟页已释放
        let location = if let Some(old_location) = self.0.table.addressing(page_id.as_ref()) {
            //指定页id的块位置存在，则将块位置设置为空块
            if let Err(_) = self.0.table.update_location(page_id.as_ref(),
                                                         old_location,
                                                         EMPTY_BLOCK) {
                //释放失败
                return false;
            }

            BlockLocation::new(old_location)
        } else {
            //指定的虚拟页不存在
            EMPTY_BLOCK_LOCATION
        };

        if location.is_empty() {
            //忽略空块的释放
            return true;
        }

        //异步释放指定块设备的指定块
        let offset = page_id.device_offset();
        let devices = self.0.devices.clone();
        let _ = self.0.rt.spawn(async move {
            if let Some(device) = devices.get(&offset) {
                //指定位置的块设备存在，则释放指定的块
                device.free_block(&location).await;
            }
        });

        //TODO...

        true
    }
}

/*
* 虚拟页管理器异步方法
*/
impl<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
    BS: Debug + Clone + Hash + Send + Sync + 'static,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
    BD: DeviceDetail<Key = BK, Val = BV>,
> VirtualPageManager<C, O, B, D, P, I, M, BU, BS, BK, BV, BD> {
    /// 异步阻塞的分配指定大小的页，在指定块设备上立即为页分配块空间，
    /// 提交分配成功后再返回非空页id，分配0大小的页则返回空页id，
    /// 分配虚拟页唯一id失败则立即抛出异常，提交分配失败也会立即抛出异常
    pub async fn immediate_alloc_page(&self,
                                      offset: u32,
                                      size: usize) -> PageId {
        if size == 0 {
            //分配大小为0的页，则立即返回空页
            return PageId::empty();
        }

        //分配虚拟页，并提交分配
        let page_id = create_page_id(self.0.uid,
                                     offset,
                                     self.0.table.alloc_page_uid());
        commit_alloced(&self.0.table,
                       &self.0.reserved,
                       &self.0.devices,
                       page_id.clone(),
                       Some(size)).await;

        page_id
    }

    /// 异步加载虚拟页管理器的虚拟页表中的所有虚拟页
    pub async fn load_all(&self) -> Result<Vec<PageId>> {
        let mut iterator = self.0.table.iter();

        let mut page_ids = Vec::with_capacity(self.0.table.len());
        while let Some((id, _location)) = iterator.next() {
            let page_id = PageId::new(id);
            if !page_id.is_empty() {
                //指定的虚拟页不是空页
                let offset = page_id.device_offset();
                if !self.0.pool.contains(page_id.as_ref()) {
                    //指定的虚拟页的页缓冲不存在，则立即创建对应的页缓冲
                    let base_page = P::with_page_type(page_id.clone(),
                                                      page_id.clone(),
                                                      None); //创建一个虚拟页的基页
                    let buffer = PageBuffer::new(base_page, self.0.pool.get_limit());
                    self.0.pool.join_page(*page_id.as_ref(), buffer); //在虚拟页缓冲池中加入页缓冲
                }

                if let Some(buffer) = self.0.pool.get_page_buffer(page_id.as_ref()) {
                    if buffer
                        .base_page()
                        .lock()
                        .is_missing_pages() {
                        //页缓冲缺页，则立即加载基页数据
                        if let Some(location) = self.0.table.addressing(page_id.as_ref()) {
                            match read_block(&self.0.rt,
                                             &self.0.devices,
                                             offset,
                                             &BlockLocation::new(location)).await {
                                Err(e) => {
                                    //读块数据失败，则立即返回错误原因
                                    return Err(Error::new(ErrorKind::Other, format!("Load page failed, page_id: {:?}, reason: {:?}", page_id, e)));
                                },
                                Ok(bin) => {
                                    //读块数据成功
                                    let mut locked = buffer
                                        .base_page()
                                        .lock();

                                    locked.deserialize_page(bin); //将块数据反序列化为基页数据
                                },
                            }
                        } else {
                            //提交分配后，指定页面id必须有对应的块位置
                            panic!("Load page failed, page_id: {:?}, device: {}, reason: block location missing",
                                   page_id,
                                   offset);
                        }
                    }
                }
            }

            page_ids.push(page_id);
        }

        Ok(page_ids)
    }

    /// 异步的读指定虚拟页
    /// 读指定虚拟页时如果虚拟页对应的页缓冲不存在，则立即加载对应的基页数据
    /// 如果当前虚拟页的页缓冲存在且为脏页，则需要立即对将写增量写入虚拟页的页缓冲的基页，保证读到最新的虚拟页数据
    pub async fn read(&self,
                      page_type: Option<usize>,
                      page_id: &PageId) -> Result<Option<O>> {
        if page_id.is_empty() || !self.0.table.contains_page(&page_id) {
            //空页或指定的虚拟页不存在，则忽略读指定的虚拟页
            return Ok(None);
        }

        let offset = page_id.device_offset();
        if !self.0.pool.contains(page_id.as_ref()) {
            //指定的虚拟页的页缓冲不存在，则立即创建对应的页缓冲
            let base_page = P::with_page_type(page_id.clone(),
                                              page_id.clone(),
                                              page_type); //创建一个虚拟页的基页
            let buffer = PageBuffer::new(base_page, self.0.pool.get_limit());
            self.0.pool.join_page(*page_id.as_ref(), buffer); //在虚拟页缓冲池中加入页缓冲
        }

        let page_id_copy = page_id.clone();
        let limit = self.0.pool.get_limit();
        let loading = async move {
            //创建一个对应的虚拟页的基页
            let base_page = P::with_page_type(page_id_copy.clone(),
                                              page_id_copy,
                                              page_type);
            Ok(PageBuffer::new(base_page, limit))
        }.boxed();
        let loaded = Box::new(move |buffer: &PageBuffer<C, O, B, D, P>| {
            //设置创建的虚拟页的属性
            buffer.set_dirty_expired(DEFAULT_DIRTY_EXPIRED);
            buffer.set_expired(DEFAULT_EXPIRED);
        });
        let buffer = match self
            .0
            .pool
            .load_page_buffer(*page_id.as_ref(), loading, loaded)
            .await {
            Err(e) => {
                //指定虚拟页的页缓冲不存在
                return Err(e);
            },
            Ok(buffer) => {
                //指定虚拟页的页缓冲存在
                buffer
            },
        };

        if buffer
            .base_page()
            .lock()
            .is_missing_pages() {
            //页缓冲缺页，则立即加载基页数据
            if let Some(location) = self.0.table.addressing(page_id.as_ref()) {
                //基页数据存在
                match read_block(&self.0.rt,
                                 &self.0.devices,
                                 offset,
                                 &BlockLocation::new(location)).await {
                    Err(e) => {
                        //读块数据失败，则立即返回错误原因
                        Err(Error::new(ErrorKind::Other,
                                       format!("Read page failed, page_id: {:?}, reason: {:?}", page_id, e)))
                    },
                    Ok(bin) => {
                        //读块数据成功
                        let mut locked = buffer
                            .base_page()
                            .lock();

                        locked.deserialize_page(bin); //将块数据反序列化为基页数据
                        let result = locked.read_page();

                        Ok(Some(result))
                    },
                }
            } else {
                //基页数据不存在
                Ok(None)
            }
        } else {
            //页缓冲不缺页
            if buffer.deltas_len() > 0 {
                //虚拟页是脏页，则合并所有脏页缓冲的写增量到基页
                let mut copyed_base_page = buffer.copy_base_page();
                let mut deltas = buffer.get_deltas().lock();
                while let Some(delta) = deltas.pop_front() {
                    let delta_cmd_index = delta.get_cmd_index();
                    let delta_type = delta.get_type();

                    if let Err(e) = copyed_base_page.write_page_delta(delta) {
                        //写入增量到基页失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Read page failed, page_id: {:?}, cmd_index: {}, type: {}, reason: {:?}", page_id, delta_cmd_index, delta_type, e)));
                    }
                }
                buffer.set_base_page(copyed_base_page); //将合并后的复制得基页替换当前脏页的基页
            }

            //返回合并后的基页数据
            let result = buffer
                .base_page()
                .lock().read_page();
            Ok(Some(result))
        }
    }

    /// 异步的写指定虚拟页，并返回写指令编号
    /// 写指定虚拟页时如果虚拟页对应的页缓冲不存在，将在刷新虚拟页时创建，但并不加载对应的基页数据
    /// 将写指令写入写指令缓冲后立即返回
    /// 如果当前写指令缓冲忙，则会异步阻塞写指定虚拟页，直到写指令缓冲空闲后，再唤醒并继续当前写指定虚拟页操作
    /// 如果写失败，则由外部负责重试，修复或忽略
    /// 不允许在没有执行任何读操作的情况下，对一个存在但还未加载到虚拟页缓冲池的虚拟页执行任何写操作
    /// 只允许对一个不存在且未加载到虚拟页缓冲池的虚拟页，在未执行任何读操作前执行写操作
    pub async fn write_back(&self, mut cmd: VirtualPageWriteCmd<C, D>) -> WriteIndex {
        //分配并设置写指令编号
        let index = self
            .0
            .write_cmd_index
            .fetch_add(1, Ordering::Relaxed);
        cmd.set_index(index);

        self
            .0
            .write_cmd_buffer
            .lock()
            .await
            .insert(index, cmd.clone());

        WriteIndex::new(index)
    }

    /// 异步的写虚拟页，可以指定是否在指定时间后强制同步脏页到块设备，成功返回写指令编号
    /// 写指定虚拟页时如果虚拟页对应的页缓冲不存在，将在刷新虚拟页时创建，但并不加载对应的基页数据
    /// 将写指令写入写指令缓冲后，立即强制刷新指定写编号的写指令，并在刷新写指令后，异步等待写指令中的所有写增量同步到块设备成功后再返回
    /// 如果当前写指令缓冲忙，则会异步阻塞写指定虚拟页，直到写指令缓冲空闲后，再唤醒并继续当前写指定虚拟页操作
    /// 如果写失败，则由外部负责重试，修复或忽略
    /// 不允许在没有执行任何读操作的情况下，对一个存在但还未加载到虚拟页缓冲池的虚拟页执行任何写操作
    /// 只允许对一个不存在且未加载到虚拟页缓冲池的虚拟页，在未执行任何读操作前执行写操作
    pub async fn write_through(&self,
                               mut cmd: VirtualPageWriteCmd<C, D>,
                               force_sync: Option<usize>) -> Result<WriteIndex> {
        let index = self.write_back(cmd.clone()).await;

        if let Some(delay_timeout) = force_sync {
            //在指定时间后，强制同步脏页到块设备
            self.0.rt.timeout(delay_timeout).await; //休眠指定时间

            let page_ids = self.flush_page(index.clone(),
                                           DEFAULT_DIRTY_EXPIRED,
                                           DEFAULT_EXPIRED).await; //强制刷新指定写编号的写指令的写增量
            if let Err(e) = self.sync_page(page_ids).await {
                //强制同步所有脏页失败，则立即返回错误原因
                Err(Error::new(ErrorKind::Other, format!("Page write through failed, index: {:?}, reason: {:?}", index, e)))
            } else {
                //强制同步所有脏页成功，则强制刷新写编号的写指令的后续写增量
                let page_ids = self.flush_followup_page(index.clone(),
                                                        DEFAULT_DIRTY_EXPIRED,
                                                        DEFAULT_EXPIRED).await;
                if let Err(e) = self.sync_page(page_ids).await {
                    //强制同步所有后续脏页失败，则立即返回错误原因
                    Err(Error::new(ErrorKind::Other, format!("Page write through failed, index: {:?}, reason: {:?}", index, e)))
                } else {
                    //强制同步所有后续脏页成功
                    Ok(index)
                }
            }
        } else {
            //异步等待虚拟页管理器自动同步脏页到块设备
            let page_ids = self.flush_page(index.clone(),
                                           DEFAULT_DIRTY_EXPIRED,
                                           DEFAULT_EXPIRED).await; //强制刷新指定写编号的写指令的写增量
            if let Err(e) = cmd.wait_write_sync().await {
                //写指令的写增量同步失败
                Err(Error::new(ErrorKind::Other, format!("Page write through failed, index: {:?}, pages: {:?}, reason: {:?}", index, page_ids, e)))
            } else {
                let page_ids = self.flush_followup_page(index.clone(),
                                                        DEFAULT_DIRTY_EXPIRED,
                                                        DEFAULT_EXPIRED).await;
                if let Err(e) = cmd.wait_write_follow_up_sync().await {
                    //写指令的后续写增量同步失败
                    Err(Error::new(ErrorKind::Other, format!("Page write follow up through failed, index: {:?}, pages: {:?}, reason: {:?}", index, page_ids, e)))
                } else {
                    //写指令执行成功
                    Ok(index)
                }
            }
        }
    }

    /// 异步的强制刷新写指令缓冲中的指定写编号的写指令的写增量，并返回刷新写增量后影响的虚拟页的唯一id列表
    /// 将写指令缓冲中的指定写指令的所有写增量分别写入对应页缓冲的写增量缓冲
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制刷新，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制刷新操作
    pub async fn flush_page(&self,
                            index: WriteIndex,
                            dirty_expired: u64,
                            expired: u64) -> Vec<PageId> {
        let mut page_ids = Vec::new();
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        if let Some(cmd) = self
            .0
            .write_cmd_buffer
            .lock()
            .await
            .get(index.as_ref()) {
            //指定写指令编号的写指令存在
            flush_write_to_pages(&table,
                                 &pool,
                                 cmd,
                                 dirty_expired,
                                 expired,
                                 &mut page_ids).await;
        }

        let mut result = Vec::new();
        for page_id in page_ids {
            result.push(PageId::new(page_id));
        }
        result
    }

    /// 异步的强制刷新写指令缓冲中的所有写指令的写增量，并返回刷新后写增量后影响的虚拟页的唯一id列表
    /// 将写指令缓冲中的指定写指令的所有写增量分别写入对应页缓冲的写增量缓冲
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制刷新，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制刷新操作
    pub async fn flush_all_pages(&self,
                                 dirty_expired: u64,
                                 expired: u64) -> Vec<PageId> {
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();
        let page_ids = flush_all_write_to_pages(&table,
                                                &pool,
                                                &write_cmd_buffer,
                                                dirty_expired,
                                                expired).await;

        let mut result = Vec::new();
        for page_id in page_ids {
            result.push(PageId::new(page_id));
        }
        result
    }

    /// 异步的强制刷新写指令缓冲中的指定写编号的写指令的后续写增量，并返回刷新后续写增量影响的虚拟页的唯一id列表
    /// 将写指令缓冲中的指定写指令的所有后续写增量分别写入对应页缓冲的写增量缓冲
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制刷新，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制刷新操作
    pub async fn flush_followup_page(&self,
                                     index: WriteIndex,
                                     dirty_expired: u64,
                                     expired: u64) -> Vec<PageId> {
        let mut page_ids = Vec::new();
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        if let Some(cmd) = self
            .0
            .write_cmd_buffer
            .lock()
            .await
            .get(index.as_ref()) {
            //指定写指令编号的写指令存在
            flush_followup_write_to_pages(&table,
                                          &pool,
                                          cmd,
                                          dirty_expired,
                                          expired,
                                          &mut page_ids).await;
        }

        let mut result = Vec::new();
        for page_id in page_ids {
            result.push(PageId::new(page_id));
        }
        result
    }

    /// 异步的强制刷新写指令缓冲中的所有写指令的后续写增量，并返回刷新后续写增量影响的虚拟页的唯一id列表
    /// 将写指令缓冲中的指定写指令的所有后续写增量分别写入对应页缓冲的写增量缓冲
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制刷新，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制刷新操作
    pub async fn flush_all_followup_pages(&self,
                                          dirty_expired: u64,
                                          expired: u64) -> Vec<PageId> {
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();
        let page_ids = flush_all_followup_write_to_pages(&table,
                                                         &pool,
                                                         &write_cmd_buffer,
                                                         dirty_expired,
                                                         expired).await;

        let mut result = Vec::new();
        for page_id in page_ids {
            result.push(PageId::new(page_id));
        }
        result
    }

    /// 异步的强制同步指定虚拟页，成功返回同步的页缓冲大小
    /// 同步脏页时，虚拟页对应的基页没有数据，则会加载基页数据后再同步
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制同步，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制同步操作
    pub async fn sync_page(&self, page_ids: Vec<PageId>) -> Result<usize> {
        let rt = self.0.rt.clone();
        let table = self.0.table.clone();
        let reserved = self.0.reserved.clone();
        let devices = self.0.devices.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();

        //强制同步指定的虚拟页
        let mut size = 0;
        for page_id in page_ids {
            if let Some(buffer) = self.0.pool.get_page_buffer(page_id.as_ref()) {
                //指定虚拟页的页缓冲存在
                let sync_guard = buffer.start_sync().await;

                let buffer_size = match sync_dirty_page(&rt,
                                                        &table,
                                                        &reserved,
                                                        &devices,
                                                        &write_cmd_buffer,
                                                        buffer.as_ref()).await {
                    Err(e) => {
                        return Err(Error::new(ErrorKind::Other, format!("Sync page failed, page_id: {:?}, reason: {:?}", page_id, e)));
                    },
                    Ok(size) => {
                        drop(sync_guard);
                        size
                    },
                };
                size += buffer_size;
            }
        }

        Ok(size)
    }

    /// 异步的强制同步所有虚拟页，成功返回同步的虚拟页的数量和虚拟页的大小
    /// 同步脏页时，虚拟页对应的基页没有数据，则会加载基页数据后再同步
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制同步，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制同步操作
    pub async fn sync_all_pages(&self) -> Result<(usize, usize)> {
        let rt = self.0.rt.clone();
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        let reserved = self.0.reserved.clone();
        let devices = self.0.devices.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();

        //强制同步所有的虚拟页
        sync_all_dirty_pages(&rt,
                             &table,
                             &pool,
                             &reserved,
                             &devices,
                             &write_cmd_buffer).await
    }

    /// 异步阻塞的强制刷新并同步虚拟页管理器的虚拟页
    /// 合并写增量的虚拟页，并将合并后的虚拟页写入块设备
    pub async fn force_flush_sync(&self,
                                  dirty_expired: u64,
                                  expired: u64) {
        let rt = self.0.rt.clone();
        let table = self.0.table.clone();
        let pool = self.0.pool.clone();
        let reserved = self.0.reserved.clone();
        let devices = self.0.devices.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();

        //刷新虚拟页管理器的写指令缓冲的所有写指令的写增量，到虚拟页缓冲池中对应页缓冲的写增量缓冲中
        let flush_write_start_time = Instant::now();
        let page_ids = flush_all_write_to_pages(&table,
                                                &pool,
                                                &write_cmd_buffer,
                                                dirty_expired,
                                                expired).await;
        debug!("Force flush pages finish, pages_count: {}, time: {:?}",
                    page_ids.len(),
                    flush_write_start_time.elapsed());
        //非强制同步虚拟页缓冲池中的所有脏页缓冲
        let sync_start_time = Instant::now();
        match sync_all_dirty_pages(&rt,
                                   &table,
                                   &pool,
                                   &reserved,
                                   &devices,
                                   &write_cmd_buffer).await {
            Err(e) => {
                //同步脏页失败
                error!("{:?}", e);
            },
            Ok((pages_count, pages_size)) => {
                //同步脏页成功
                debug!("Force sync dirty pages ok, pages_count: {}, pages_size: {}, time: {:?}",
                            pages_count,
                            pages_size,
                            sync_start_time.elapsed());
            },
        }

        //刷新虚拟页管理器的写指令缓冲的所有写指令的后续写增量，到虚拟页缓冲池中对应页缓冲的写增量缓冲中
        let flush_followup_write_start_time = Instant::now();
        let page_ids = flush_all_followup_write_to_pages(&table,
                                                         &pool,
                                                         &write_cmd_buffer,
                                                         dirty_expired,
                                                         expired).await;
        //刷新后续写增量成功，则立即强制同步虚拟页缓冲池中的所有被后续写增量影响的脏页缓冲
        let pages_count = page_ids.len();
        debug!("Force flush pages finish, pages_count: {}, time: {:?}",
                            pages_count,
                            flush_followup_write_start_time.elapsed());

        let mut pages_size = 0;
        let sync_followup_start_time = Instant::now();
        for page_id in page_ids {
            if let Some(buffer) = pool.get_page_buffer(&page_id) {
                match sync_dirty_page(&rt,
                                      &table,
                                      &reserved,
                                      &devices,
                                      &write_cmd_buffer,
                                      buffer.as_ref()).await {
                    Err(e) => {
                        //强制同步后续写增量影响的脏页失败
                        error!("{:?}", e);
                    },
                    Ok(buffer_size) => {
                        //强制同步后续写增量影响的脏页成功
                        pages_size += buffer_size;
                    },
                }
            }
        }

        debug!("Force sync followup dirty pages ok, pages_count: {}, pages_size: {}, time: {:?}",
                            pages_count,
                            pages_size,
                            sync_followup_start_time.elapsed());
    }

    /// 内部使用的异步的强制同步指定虚拟页，成功返回同步的页缓冲大小
    /// 同步脏页时，虚拟页对应的基页没有数据，则会加载基页数据后再同步
    /// 如果当前虚拟页管理器正在刷新，正在同步脏页或正在整理，则会异步阻塞强制同步，直到刷新，同步脏页或整理完成后，再唤醒并继续当前强制同步操作
    pub async fn sync_page_buffer(&self, buffer: Arc<PageBuffer<C, O, B, D, P>>) -> Result<usize> {
        let rt = self.0.rt.clone();
        let table = self.0.table.clone();
        let reserved = self.0.reserved.clone();
        let devices = self.0.devices.clone();
        let write_cmd_buffer = self.0.write_cmd_buffer.clone();
        let page_id = buffer.base_page().lock().get_original_page_id();

        //强制同步指定的虚拟页
        let sync_guard = buffer.start_sync().await;

        let buffer_size = match sync_dirty_page(&rt,
                                                &table,
                                                &reserved,
                                                &devices,
                                                &write_cmd_buffer,
                                                buffer.as_ref()).await {
            Err(e) => {
                return Err(Error::new(ErrorKind::Other, format!("Sync page buffer failed, page_id: {:?}, reason: {:?}", page_id, e)));
            },
            Ok(size) => {
                drop(sync_guard);
                size
            },
        };

        Ok(buffer_size)
    }
}

// 内部虚拟页管理器
struct InnerVirtualPageManager<
    C: Send + 'static,
    O: Send + 'static,
    B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
    D: VirtualPageWriteDelta<Content = C>,
    P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
    I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static = VirtualPageLFUCacheDirtyIterator<C, O, B, D, P>,
    M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> = VirtualPageLFUCache<C, O, B, D, P>,
    BU: Debug + Clone + Hash + Ord + Send + Sync + 'static = Guid,
    BS: Debug + Clone + Hash + Send + Sync + 'static = DeviceStatus,
    BK: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BV: Debug + Clone + Hash + Eq + Send + Sync + 'static = DeviceValueType,
    BD: DeviceDetail<Key = BK, Val = BV> = DeviceDetailMap,
> {
    //虚拟页管理器唯一id
    uid:                u32,
    //运行时
    rt:                 MultiTaskRuntime<()>,
    //虚拟页表
    table:              VirtualPageTable,
    //虚拟页缓冲池
    pool:               VirtualPageBufferPool<C, O, B, D, P, I, M>,
    //保留的虚拟页表，不需要持久化
    reserved:           Arc<DashMap<u128, usize>>,
    //块设备列表
    devices:            Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = B>>>>,
    //定时同步的间隔时长，单位ms
    sync_interval:      usize,
    //当前写指令编号
    write_cmd_index:    AtomicU64,
    //写指令缓冲
    write_cmd_buffer:   Arc<Mutex<BTreeMap<u64, VirtualPageWriteCmd<C, D>>>>,
}

// 根据虚拟页管理器、块设备和页唯一id生成虚拟页id
#[inline]
fn create_page_id(uid: u32,
                  offset: u32,
                  page_uid: u64) -> PageId {
    PageId(((uid as u128) << 96) | ((offset as u128) << 64) | page_uid as u128)
}

// 在虚拟页表中注册指定虚拟页，包括页面唯一id和块位置
// 如果块位置为空，则表示注册的虚拟页为保留的虚拟页，即还未提交分配的虚拟页
// 成功返回空，失败则返回已存在的虚拟页的块位置
#[inline]
fn register_page(table: &VirtualPageTable,
                 page_id: PageId,
                 location: BlockLocation) -> Option<BlockLocation> {
    if let Some(old) = table.register(*page_id, *location) {
        //指定页id的虚拟页已存在，则返回已存在的虚拟页的块位置
        Some(BlockLocation::new(old))
    } else {
        //指定页id的虚拟页不存在
        None
    }
}

// 在保留的虚拟页表中注册指定虚拟页
// 保留的虚拟页并没有分配对应的块，会在提供分配后再分配对应的块
// 成功返回空，失败则返回已存在的保留的虚拟页的大小
#[inline]
fn register_reserved_page(reserved: &DashMap<u128, usize>,
                          page_id: PageId,
                          size: usize) -> Option<usize> {
    if let Some(old) = reserved.insert(*page_id, size) {
        //指定页id的保留的虚拟页已存在，则返回已存在的保留的虚拟页的大小
        Some(old)
    } else {
        //指定页id的保留的虚拟页不存在
        None
    }
}

// 提交已分配的虚拟页，会从指定的块设备上分配指定虚拟页大小的块空间
// 提交分配成功，则会移除保留的虚拟页，并返回提交分配后的块内部位置
// 提交分配失败，则会忽略保留的虚拟页，并返回提交分配后的块内部位置
// 一般会在立即分配或在同步保留的虚拟页时调用
#[inline]
async fn commit_alloced<BU, BS, BK, BV, BD, BF>(table: &VirtualPageTable,
                                                reserved: &DashMap<u128, usize>,
                                                devices: &DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = BF>>>,
                                                page_id: PageId,
                                                size: Option<usize>) -> u64
    where BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV>,
          BF: AsRef<[u8]> + Clone + Send + Sync + 'static {
    let offset = page_id.device_offset(); //获取虚拟页所在的块设备，在虚拟页管理器中的位置
    if let Some(device) = devices.get(&offset) {
        //指定虚拟页所在的块设备存在
        if let Some(size) = size {
            //立即为指定虚拟页分配块，并注册到虚拟页表
            let location = device
                .alloc_block(size)
                .await; //分配指定大小的块

            if location.is_empty() {
                //分配块失败，则立即抛出异常
                panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {:?}, reason: out of space");
            }

            //注册指定页id和块位置的虚拟页
            if let Some(_) = register_page(table, page_id.clone(), location.clone()) {
                //不允许注册已存在的虚拟页
                panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {:?}, reason: conflict page",
                       page_id,
                       offset,
                       size);
            }

            //注册并分配虚拟页成功
            debug!("Immediate Commit alloced ok, page_id: {:?}, device: {}, size: {:?}",
                page_id,
                offset,
                size);

            *location
        } else {
            //为指定的保留的虚拟页分配块
            if let Some((_, size)) = reserved.remove(page_id.as_ref()) {
                //指定的保留的虚拟页存在
                let location = device
                    .alloc_block(size)
                    .await; //分配指定大小的块

                if location.is_empty() {
                    //分配块失败，则立即抛出异常
                    panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {:?}, reason: out of space");
                }

                //原子的更新指定页id对应的块位置
                match table.update_location(page_id.as_ref(),
                                            EMPTY_BLOCK,
                                            *location) {
                    Err(e) => {
                        //更新失败
                        panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {}, reason: {:?}",
                               page_id,
                               offset,
                               size,
                               e);
                    },
                    Ok(None) => {
                        //指定的虚拟页不存在
                        panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {}, reason: page not exist",
                               page_id,
                               offset,
                               size);
                    },
                    Ok(Some(_)) => {
                        //更新成功
                        debug!("Commit alloced ok, page_id: {:?}, device: {}, size: {}",
                            page_id,
                            offset,
                            size);
                    },
                }

                *location
            } else {
                //指定的保留的虚拟页不存在，则立即抛出异常
                panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {:?}, reason: reserved page not exist",
                       page_id,
                       offset,
                       size);
            }
        }
    } else {
        //指定虚拟页所在的块设备不存在
        panic!("Commit alloced failed, page_id: {:?}, device: {}, size: {:?}, reason: devices missing",
               page_id,
               offset,
               size);
    }
}

/// 异步迭代写指令缓冲中的所有写指令，并将写指令中的写增量加入对应页缓冲的写增量缓冲中，返回本次刷新的写增量影响的虚拟页的唯一id列表
async fn flush_all_write_to_pages<C, O, B, D, P, I, M>(table: &VirtualPageTable,
                                                       pool: &VirtualPageBufferPool<C, O, B, D, P, I, M>,
                                                       write_cmd_buffer: &Arc<Mutex<BTreeMap<u64, VirtualPageWriteCmd<C, D>>>>,
                                                       dirty_expired: u64,
                                                       expired: u64) -> Vec<u128>
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
          M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> {
    let locked = write_cmd_buffer
        .lock()
        .await;
    let mut iterator = locked.iter(); //获取以编号从小到大的顺序进行迭代的写指令缓冲迭代器

    let mut page_ids = Vec::new();
    while let Some((_index, cmd)) = iterator.next() {
        flush_write_to_pages(table,
                             pool,
                             cmd,
                             dirty_expired,
                             expired,
                             &mut page_ids).await;
    }

    page_ids.dedup(); //移除重复的虚拟页唯一id
    page_ids
}

// 将指定写指令中的写增量加入对应页缓冲的写增量缓冲中，返回写增量的数量
async fn flush_write_to_pages<C, O, B, D, P, I, M>(table: &VirtualPageTable,
                                                   pool: &VirtualPageBufferPool<C, O, B, D, P, I, M>,
                                                   cmd: &VirtualPageWriteCmd<C, D>,
                                                   dirty_expired: u64,
                                                   expired: u64,
                                                   page_ids: &mut Vec<u128>)
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
          M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> {
    while let Some(delta) = cmd.pop_front() {
        let copied_page_id = delta.get_copied_page_id();
        if copied_page_id.is_empty() || !table.contains_page(&copied_page_id) {
            //忽略空页或不存在的页的刷新，并继续刷新下一个页缓冲
            continue;
        }
        let page_type = delta.get_type();

        let origin_page_id_copy = delta.get_origin_page_id();
        let copied_page_id_copy = copied_page_id.clone();
        let limit = pool.get_limit();
        let loading = async move {
            //创建一个写增量对应的虚拟页的基页
            let base_page = P::with_page_type(origin_page_id_copy,
                                              copied_page_id_copy,
                                              Some(page_type));
            Ok(PageBuffer::new(base_page, limit))
        }.boxed();
        let loaded = Box::new(move |buffer: &PageBuffer<C, O, B, D, P>| {
            //设置创建的虚拟页的属性
            buffer.set_dirty_expired(dirty_expired);
            buffer.set_expired(expired);
        });
        match pool
            .load_page_buffer(*copied_page_id.as_ref(), loading, loaded)
            .await {
            Err(e) => {
                //指定虚拟页的页缓冲不存在
                error!("Flush write to pages failed, reason: {:?}", e);
            },
            Ok(buffer) => {
                //后续写增量指定的虚拟页存在
                let flush_guard = buffer.start_flush().await;

                let delta_size = delta.size();
                buffer.append_delta(delta);
                pool.adjust_page_buffer_size(copied_page_id.as_ref(), delta_size as isize);
                drop(flush_guard); //关闭页缓冲的刷新状态

                page_ids.push(*copied_page_id);
            },
        }
    }
}

/// 异步迭代写指令缓冲中的所有写指令，并将写指令中的后续写增量加入对应页缓冲的写增量缓冲中，返回本次刷新后续写增量所影响的虚拟页的唯一id列表
async fn flush_all_followup_write_to_pages<C, O, B, D, P, I, M>(table: &VirtualPageTable,
                                                                pool: &VirtualPageBufferPool<C, O, B, D, P, I, M>,
                                                                write_cmd_buffer: &Arc<Mutex<BTreeMap<u64, VirtualPageWriteCmd<C, D>>>>,
                                                                dirty_expired: u64,
                                                                expired: u64) -> Vec<u128>
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
          M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> {
    let mut locked = write_cmd_buffer
        .lock()
        .await;
    let mut iterator = locked.iter(); //获取以编号从小到大的顺序进行迭代的写指令缓冲迭代器

    let mut indexs = Vec::new();
    let mut page_ids = Vec::new();
    while let Some((index, cmd)) = iterator.next() {
        flush_followup_write_to_pages(table,
                                      pool,
                                      cmd,
                                      dirty_expired,
                                      expired,
                                      &mut page_ids).await;
        indexs.push(*index);
    }

    page_ids.dedup(); //移除重复的虚拟页唯一id
    page_ids
}

// 将指定写指令中的后续写增量加入对应页缓冲的写增量缓冲中，返回指定写指令的后续写增量所影响的虚拟页的唯一id列表
async fn flush_followup_write_to_pages<C, O, B, D, P, I, M>(table: &VirtualPageTable,
                                                            pool: &VirtualPageBufferPool<C, O, B, D, P, I, M>,
                                                            cmd: &VirtualPageWriteCmd<C, D>,
                                                            dirty_expired: u64,
                                                            expired: u64,
                                                            page_ids: &mut Vec<u128>)
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
          M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I> {
    if !cmd.is_synced_deltas() {
        //当前写指令的所有写增量还未同步完成，则忽略当前写指令的后续写增量的刷新
        return;
    }

    while let Some(delta) = cmd.pop_front_from_followup() {
        let copied_page_id = delta.get_copied_page_id();
        if copied_page_id.is_empty() || !table.contains_page(&copied_page_id) {
            //忽略空页或不存在的页的刷新，并继续刷新下一个页缓冲
            continue;
        }
        let page_type = delta.get_type();

        let origin_page_id_copy = delta.get_origin_page_id();
        let copied_page_id_copy = copied_page_id.clone();
        let limit = pool.get_limit();
        let loading = async move {
            //创建一个写增量对应的虚拟页的基页
            let base_page = P::with_page_type(origin_page_id_copy,
                                              copied_page_id_copy,
                                              Some(page_type));
            Ok(PageBuffer::new(base_page, limit))
        }.boxed();
        let loaded = Box::new(move |buffer: &PageBuffer<C, O, B, D, P>| {
            //设置创建的虚拟页的属性
            buffer.set_dirty_expired(dirty_expired);
            buffer.set_expired(expired);
        });
        match pool
            .load_page_buffer(*copied_page_id.as_ref(), loading, loaded)
            .await {
            Err(e) => {
                //指定虚拟页的页缓冲不存在
                error!("Flush followup write to pages failed, reason: {:?}", e);
            },
            Ok(buffer) => {
                //后续写增量指定的虚拟页存在
                let flush_guard = buffer.start_flush().await;

                let delta_size = delta.size();
                buffer.append_delta(delta);
                pool.adjust_page_buffer_size(copied_page_id.as_ref(), delta_size as isize);
                drop(flush_guard); //关闭页缓冲的刷新状态

                page_ids.push(*copied_page_id);
            },
        }
    }
}

/// 异步迭代同步虚拟页面缓冲池中的所有页缓冲，并同步其中的脏页缓冲，返回本次同步的页缓冲数量和页缓冲大小
#[inline]
async fn sync_all_dirty_pages<C, O, B, D, P, I, M, BU, BS, BK, BV, BD>(rt: &MultiTaskRuntime<()>,
                                                                       table: &VirtualPageTable,
                                                                       pool: &VirtualPageBufferPool<C, O, B, D, P, I, M>,
                                                                       reserved: &Arc<DashMap<u128, usize>>,
                                                                       devices: &Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = B>>>>,
                                                                       write_cmd_buffer: &Arc<Mutex<BTreeMap<u64, VirtualPageWriteCmd<C, D>>>>) -> Result<(usize, usize)>
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          I: Iterator<Item = Arc<PageBuffer<C, O, B, D, P>>> + Send + 'static,
          M: VirtualPageCachingStrategy<C, O, B, D, P, Iter = I>,
          BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV> {
    let mut iterator = pool.iter();

    let mut count = 0;
    let mut size = 0;
    while let Some(buffer) = iterator.next() {
        if buffer.deltas_len() == 0 {
            //当前页缓冲不是脏页，则忽略当前页缓冲的同步，并继续同步下一个页缓冲
            continue;
        }

        let sync_guard = buffer.start_sync().await;

        match sync_dirty_page(rt,
                              table,
                              reserved,
                              devices,
                              write_cmd_buffer,
                              buffer.as_ref()).await {
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                //分配块失败或写块数据失败，则忽略当前页缓冲的同步，并继续同步下一个页缓冲
                drop(sync_guard);
                continue;
            },
            Err(e) => {
                //同步指定页缓冲失败，则立即返回错误原因
                return Err(e);
            },
            Ok(buffer_size) => {
                //同步指定的脏页缓冲成功
                drop(sync_guard);
                count += 1; //增加本次同步的页缓冲数量
                size += buffer_size; //增加本次同步的页缓冲大小
            },
        }
    }

    Ok((count, size))
}

/// 异步的同步虚拟页缓冲池中的指定缓冲页，如果指定缓冲页是脏页则同步脏页缓冲，返回本次同步的页缓冲大小
/// 调用者必须保证调用指定的页缓冲的start_sync方法，并在返回成功后再调用sync_dirty_page方法对指定的页缓冲进行同步
#[inline]
async fn sync_dirty_page<C, O, B, D, P, BU, BS, BK, BV, BD>(rt: &MultiTaskRuntime<()>,
                                                            table: &VirtualPageTable,
                                                            reserved: &Arc<DashMap<u128, usize>>,
                                                            devices: &Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = B>>>>,
                                                            write_cmd_buffer: &Arc<Mutex<BTreeMap<u64, VirtualPageWriteCmd<C, D>>>>,
                                                            buffer: &PageBuffer<C, O, B, D, P>) -> Result<usize>
    where C: Send + 'static,
          O: Send + 'static,
          B: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static,
          D: VirtualPageWriteDelta<Content = C>,
          P: VirtualPageBuf<Content = C, Delta = D, Bin = B, Output = O>,
          BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV> {
    if buffer.deltas_len() == 0 {
        //当前页缓冲不是脏页，则忽略当前页缓冲的同步
        return Ok(0);
    }
    let buffer_size = buffer.buf_size();

    //当前脏页缓冲需要同步，则复制当前页缓冲的基页，并将当前页缓冲的写增量按顺序写入复制的基页中
    {
        let mut copyed_base_page = buffer.copy_base_page();
        let origin_page_id = copyed_base_page.get_original_page_id();
        let copied_page_id = copyed_base_page.get_copied_page_id();
        if copied_page_id.is_empty() || !table.contains_page(&copied_page_id) {
            //忽略空页或不存在的页的同步
            return Ok(0);
        }
        let offset = copied_page_id.device_offset();

        let mut old_location = EMPTY_BLOCK; //默认旧的块位置为空块
        let mut new_location = EMPTY_BLOCK_LOCATION; //默认新的块位置为空块
        if reserved.contains_key(copied_page_id.as_ref()) {
            //复制的脏页的基页是保留的虚拟页，则为保留的虚拟页提交分配
            //不需要为刚提交分配的虚拟页换入缺页的虚拟页，因为刚提交的保留的虚拟页一定会出现基页缺页，需要写增量对其进行初始化
            new_location = BlockLocation::new(commit_alloced(table,
                                                             reserved,
                                                             devices,
                                                             copied_page_id.clone(),
                                                             None).await);
            old_location = *new_location; //为已提交分配的保留的虚拟页，设置旧的块位置
        } else {
            //复制的脏页的基页是已提交的虚拟页
            if copyed_base_page.is_missing_pages() {
                //复制的脏页的基页缺页，则需要先换入缺页的虚拟页
                if let Some(location) = table.addressing(origin_page_id.as_ref()) {
                    //如果当前缺页的复制的脏页的基页对应的不是空块，则加载对应的原始虚拟页所指定的块位置的块数据，并将块数据反序列化为复制的脏页的基页
                    match read_block(rt,
                                     devices,
                                     offset,
                                     &BlockLocation::new(location)).await {
                        Err(e) => {
                            //读块数据失败，则立即通知对应的写指令，并立即返回错误原因
                            let mut deltas = buffer.get_deltas().lock();
                            while let Some(delta) = deltas.pop_front() {
                                let delta_cmd_index = delta.get_cmd_index();
                                let delta_type = delta.get_type();

                                if let Some(write_cmd) = write_cmd_buffer
                                    .lock()
                                    .await
                                    .get(&delta_cmd_index) {
                                    let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, type: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, delta_type, e)))).await;
                                }
                            }

                            return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, reason: {:?}", copied_page_id, e)));
                        },
                        Ok(bin) => {
                            //读块数据成功
                            copyed_base_page.deserialize_page(bin); //将块数据反序列化为复制的脏页的基页
                            old_location = location; //设置旧的块位置
                        },
                    }
                } else {
                    //提交分配后，指定页面id必须有对应的块位置
                    panic!("Flush drity page failed, page_id: {:?}, device: {}, size: {}, reason: block location missing",
                           copied_page_id,
                           offset,
                           buffer_size);
                }
            } else {
                //复制的脏页的基页不缺页，则不需要先换入缺页的虚拟页
                if let Some(location) = table.addressing(origin_page_id.as_ref()) {
                    //设置旧的块位置
                    old_location = location;
                } else {
                    //提交分配后，指定页面id必须有对应的块位置
                    panic!("Flush drity page failed, page_id: {:?}, device: {}, size: {}, reason: block location missing",
                           copied_page_id,
                           offset,
                           buffer_size);
                }
            }
        }

        //复制的脏页的基页已就绪，则合并所有脏页缓冲的写增量到基页
        let mut cmd_indexs = BTreeMap::new();
        let mut deltas = buffer.get_deltas().lock();
        while let Some(delta) = deltas.pop_front() {
            let delta_cmd_index = delta.get_cmd_index();
            let delta_type = delta.get_type();

            if let Err(e) = copyed_base_page.write_page_delta(delta) {
                //写入增量到基页失败，则立即通知对应的写指令，并立即返回错误原因
                if let Some(write_cmd) = write_cmd_buffer
                    .lock()
                    .await
                    .get(&delta_cmd_index) {
                    let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, type: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, delta_type, e)))).await;
                }

                return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, type: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, delta_type, e)));
            } else {
                //记录当前写缓冲的写增量所在的写指令编号和写增量的数量
                match cmd_indexs.entry(delta_cmd_index) {
                    BtreeMapEntry::Occupied(mut o) => {
                        *o.get_mut() += 1;
                    },
                    BtreeMapEntry::Vacant(v) => {
                        v.insert(1);
                    },
                }
            }
        }

        let copyed_base_page_clone = copyed_base_page.clone();
        let mut copyed_base_page_bin = copyed_base_page.serialize_page();
        let copyed_base_page_size = copyed_base_page_bin.as_mut().len();
        if new_location.is_empty() {
            //当前虚拟页是已存储到块设备上的块数据的映射，则需要根据复制的脏页的基页的合并后大小，分配指定块设备的新块
            match alloc_block(devices, offset, copyed_base_page_size).await {
                Err(e) => {
                    //分配块失败，则立即通知对应的写指令，并立即返回错误原因
                    for (delta_cmd_index, _sync_count) in cmd_indexs {
                        if let Some(write_cmd) = write_cmd_buffer
                            .lock()
                            .await
                            .get(&delta_cmd_index) {
                            //对应写指令编号的写指令存在
                            let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, e)))).await;
                        }
                    }

                    return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, reason: {:?}", copied_page_id, e)));
                },
                Ok(location) => {
                    //分配块成功
                    new_location = location;
                },
            }
        }

        //将合并后的复制的脏页的基页写入分配的块中
        match write_block(rt,
                          devices,
                          offset,
                          &new_location,
                          copyed_base_page_bin).await {
            Err(e) => {
                //写块数据失败，则立即通知对应的写指令，并立即返回错误原因
                for (delta_cmd_index, _sync_count) in cmd_indexs {
                    if let Some(write_cmd) = write_cmd_buffer
                        .lock()
                        .await
                        .get(&delta_cmd_index) {
                        //对应写指令编号的写指令存在
                        let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, e)))).await;
                    }
                }

                return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, reason: {:?}", copied_page_id, e)));
            },
            Ok(_) => {
                //在新的块位置上写块数据成功，则立即更新虚拟页表中指定虚拟页对应的块位置
                if let Err(current_location) = table.update_location(copied_page_id.as_ref(), old_location, *new_location) {
                    //更新虚拟页表中指定虚拟页对应的块位置失败，则立即通知对应的写指令，并立即返回错误原因
                    for (delta_cmd_index, _sync_count) in cmd_indexs {
                        if let Some(write_cmd) = write_cmd_buffer
                            .lock()
                            .await
                            .get(&delta_cmd_index) {
                            //对应写指令编号的写指令存在
                            let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, current_location: {}, old_location: {}, new_location: {}, reason: update location failed", copied_page_id, offset, delta_cmd_index, current_location, old_location, *new_location)))).await;
                        }
                    }

                    return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, current_location: {}, old_location: {}, new_location: {}, reason: update location failed", copied_page_id, current_location, old_location, *new_location)));
                }

                //强制刷新虚拟页表，以持久化更新虚拟页表的结果
                if let Err(e) = table.flush().await {
                    //强制刷新虚拟页表失败，则立即通知对应的写指令，并立即返回错误原因
                    for (delta_cmd_index, _sync_count) in cmd_indexs {
                        if let Some(write_cmd) = write_cmd_buffer
                            .lock()
                            .await
                            .get(&delta_cmd_index) {
                            //对应写指令编号的写指令存在
                            let _ = write_cmd.callback_by_sync(Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, device: {}, cmd_index: {}, old_location: {}, new_location: {}, reason: {:?}", copied_page_id, offset, delta_cmd_index, old_location, *new_location, e)))).await;
                        }
                    }

                    return Err(Error::new(ErrorKind::Other, format!("Sync dirty page failed, page_id: {:?}, old_location: {}, new_location: {}, reason: {:?}", copied_page_id, old_location, *new_location, e)));
                }

                //将合并后的复制得基页替换当前脏页的基页
                buffer.set_base_page(copyed_base_page_clone);

                //更新虚拟页表中指定虚拟页对应的块位置成功，则表示写虚拟页成功，则立即通知对应的写指令
                for (delta_cmd_index, sync_count) in cmd_indexs {
                    let mut is_remove_cmd = false; //初始化是否移除写指令的标记

                    if let Some(write_cmd) = write_cmd_buffer
                        .lock()
                        .await
                        .get(&delta_cmd_index) {
                        //对应写指令编号的写指令存在，调用当前写指令的写增量的成功回调
                        let _ = write_cmd.callback_by_sync(Ok(sync_count)).await;

                        if write_cmd.deltas_len() == 0 && write_cmd.followup_len() == 0 {
                            is_remove_cmd = true;
                        }
                    }

                    if is_remove_cmd {
                        //写指令的所有写增量和所有后续写增量已同步成功，则将写指令从写缓冲中移除
                        let _ = write_cmd_buffer
                            .lock()
                            .await
                            .remove(&delta_cmd_index);
                    }
                }
            },
        }
    }

    Ok(buffer_size)
}

// 异步从指定块设备上分配指定大小的块
async fn alloc_block<BU, BS, BK, BV, BD, BF>(devices: &Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = BF>>>>,
                                             offset: u32,
                                             size: usize) -> Result<BlockLocation>
    where BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV>,
          BF: AsRef<[u8]> + Clone + Send + Sync + 'static {
    if let Some(entry) = devices.get(&offset) {
        let device = entry.value();
        Ok(device.alloc_block(size).await)
    } else {
        //指定虚拟页所在的块设备不存在
        Err(Error::new(ErrorKind::Other, format!("Alloc block device space failed, device: {}, size: {}, reason: devices missing",
                                                 offset,
                                                 size)))
    }
}

// 异步读取指定块设备的指定块位置的数据，成功返回读取到的块数据
#[inline]
async fn read_block<BU, BS, BK, BV, BD, BF>(rt: &MultiTaskRuntime<()>,
                                            devices: &Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = BF>>>>,
                                            offset: u32,
                                            location: &BlockLocation) -> Result<BF>
    where BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV>,
          BF: AsRef<[u8]> + Clone + Send + Sync + 'static {
    if location.is_empty() {
        //读空块，则立即返回读错误
        return Err(Error::new(ErrorKind::Other, format!("Read block device failed, device: {}, location: {:?}, reason: empty block", offset, location)));
    }

    if let Some(entry) = devices.get(&offset) {
        let device = entry.value();

        loop {
            match device.read(location).await {
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    //块设备读取数据已阻塞，则休眠后，继续从设备中读取数据
                    rt.timeout(0).await;
                },
                Err(e) => {
                    //块设备读取数据错误，则立即返回错误原历
                    return Err(Error::new(ErrorKind::Other, format!("Read block device failed, device: {}, location: {:?}, reason: {:?}", offset, location, e)));
                },
                Ok(result) => {
                    //块设备读取数据成功
                    return Ok(result);
                }
            }
        }
    } else {
        //指定虚拟页所在的块设备不存在
        Err(Error::new(ErrorKind::Other, format!("Read block device failed, device: {}, location: {:?}, reason: devices missing", offset, location)))
    }
}

// 异步向指定块设备的指定块位置写入数据，成功返回写入的数据大小
#[inline]
async fn write_block<BU, BS, BK, BV, BD, BF>(rt: &MultiTaskRuntime<()>,
                                             devices: &Arc<DashMap<u32, Arc<dyn BlockDevice<Uid = BU, Status = BS, DetailKey = BK, DetailVal = BV, Detail = BD, Buf = BF>>>>,
                                             offset: u32,
                                             location: &BlockLocation,
                                             buf: BF) -> Result<usize>
    where BU: Debug + Clone + Hash + Ord + Send + Sync + 'static,
          BS: Debug + Clone + Hash + Send + Sync + 'static,
          BK: Debug + Clone + Hash + Eq + Send + Sync + 'static ,
          BV: Debug + Clone + Hash + Eq + Send + Sync + 'static,
          BD: DeviceDetail<Key = BK, Val = BV>,
          BF: AsRef<[u8]> + Clone + Send + Sync + 'static {
    let size = buf.as_ref().len();
    if location.is_empty() {
        //写空块，则立即返回写错误
        return Err(Error::new(ErrorKind::Other, format!("Write block device failed, device: {}, location: {:?}, size: {}, reason: empty block", offset, location, size)));
    }

    if let Some(entry) = devices.get(&offset) {
        let device = entry.value();

        loop {
            match device.write(location, &buf).await {
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    //块设备读取数据已阻塞，则休眠后，继续从设备中读取数据
                    rt.timeout(0).await;
                },
                Err(e) => {
                    //块设备读取数据错误，则立即返回错误原历
                    return Err(Error::new(ErrorKind::Other, format!("Write block device failed, device: {}, location: {:?}, size: {}, reason: {:?}", offset, location, size, e)));
                },
                Ok(result) => {
                    //块设备读取数据成功
                    return Ok(result);
                }
            }
        }
    } else {
        //指定虚拟页所在的块设备不存在
        Err(Error::new(ErrorKind::Other, format!("Write block device failed, device: {}, location: {:?}, size: {}, reason: devices missing", offset, location, size)))
    }
}



