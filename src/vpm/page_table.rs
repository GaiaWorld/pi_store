use std::time::Instant;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::result::Result as GenResult;
use std::collections::hash_map::Entry as HashMapEntry;
use std::io::Result;
use std::sync::{Arc,
                atomic::{AtomicU64, Ordering}};

use dashmap::{DashMap, iter::Iter};
use bytes::{Buf, BufMut};
use log::{debug, error};

use pi_hash::XHashMap;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::multi_thread::MultiTaskRuntime};

use crate::{log_store::log_file::{PairLoader, LogMethod, LogFile},
            vpm::{EMPTY_PAGE, PageId}};

///
/// 超级页的页ID
///
const SUPER_PAGE_ID: u128 = 0;

///
/// 可分配的最大页面唯一id
///
const MAX_PAGE_UID: u64 = u64::MAX;

///
/// 默认的提交日志的块大小，为了防止自动生成新的可写文件，所以默认为最大，单位B
///
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const DEFAULT_COMMIT_LOG_BLOCK_SIZE: usize = 16 * 1024 * 1024 * 1024;
#[cfg(any(target_arch = "x86", target_arch = "arm"))]
const DEFAULT_COMMIT_LOG_BLOCK_SIZE: usize = usize::MAX;

///
/// 虚拟页表迭代器条目
///
#[derive(Debug, Clone)]
pub enum VirtualPageTableIteratorItem {
    Reserved(u128, Vec<u8>),    //保留页的值
    Internal(u128, Vec<u8>),    //内部页的值
    Normal(u128, u64),          //普通页的值
}

impl VirtualPageTableIteratorItem {
    /// 判断是否是保留页
    pub fn is_reserved(&self) -> bool {
        if let VirtualPageTableIteratorItem::Reserved(_, _) = self {
            true
        } else {
            false
        }
    }

    /// 判断是否是内部页
    pub fn is_internal(&self) -> bool {
        if let VirtualPageTableIteratorItem::Internal(_, _) = self {
            true
        } else {
            false
        }
    }

    /// 判断是否是普通页
    pub fn is_normal(&self) -> bool {
        if let VirtualPageTableIteratorItem::Normal(_, _) = self {
            true
        } else {
            false
        }
    }

    /// 获取页唯一ID的数值
    pub fn page_id(&self) -> u128 {
        match self {
            VirtualPageTableIteratorItem::Reserved(pid, _) => *pid,
            VirtualPageTableIteratorItem::Internal(pid, _) => *pid,
            VirtualPageTableIteratorItem::Normal(pid, _) => *pid,
        }
    }
}

///
/// 虚拟页表迭代器
///
pub struct VirtualPageTableIterator{
    internal_iterator:  VirtualPageTableInternalPgaeIterator,   //内部页迭代器
    normal_iterator:    VirtualPageTableNormalPgaeIterator,     //普通页迭代器
    is_finish:  bool,   //是否已迭代完成
}

unsafe impl Send for VirtualPageTableIterator {}

impl Iterator for VirtualPageTableIterator {
    type Item = VirtualPageTableIteratorItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finish {
            //已迭代完成，则立即返回空
            return None;
        }

        if !self.internal_iterator.is_finish {
            //迭代内部页
            if let Some(val) = self.internal_iterator.next() {
                return Some(VirtualPageTableIteratorItem::Internal(val.0, val.1));
            }
        }

        if !self.normal_iterator.is_finish {
            //迭代普通页
            if let Some(val) = self.normal_iterator.next() {
                return Some(VirtualPageTableIteratorItem::Normal(val.0, val.1));
            }
        }

        //已迭代完成，则释放迭代器
        self.is_finish = true;
        None
    }
}

///
/// 虚拟页表的普通页迭代器
///
pub struct VirtualPageTableNormalPgaeIterator{
    inner:      usize,  //迭代器指针
    is_finish:  bool,   //是否已迭代完成
}

unsafe impl Send for VirtualPageTableNormalPgaeIterator {}

impl Iterator for VirtualPageTableNormalPgaeIterator {
    type Item = (u128, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finish {
            //已迭代完成，则立即返回空
            return None;
        }

        //获取迭代器
        let mut iterator = unsafe {
            Box::from_raw(self.inner as *mut Iter<'_, u128, AtomicU64>)
        };

        if let Some(val) = iterator.next() {
            Box::into_raw(iterator); //还未迭代完成，则需要避免迭代器被提前释放
            Some((*val.key(), val.value().load(Ordering::Acquire)))
        } else {
            //已迭代完成，则释放迭代器
            self.is_finish = true;
            None
        }
    }
}

///
/// 虚拟页表的内部页迭代器
///
pub struct VirtualPageTableInternalPgaeIterator{
    inner:      usize,  //迭代器指针
    is_finish:  bool,   //是否已迭代完成
}

unsafe impl Send for VirtualPageTableInternalPgaeIterator {}

impl Iterator for VirtualPageTableInternalPgaeIterator {
    type Item = (u128, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finish {
            //已迭代完成，则立即返回空
            return None;
        }

        //获取迭代器
        let mut iterator = unsafe {
            Box::from_raw(self.inner as *mut Iter<'_, u128, Vec<u8>>)
        };

        if let Some(val) = iterator.next() {
            Box::into_raw(iterator); //还未迭代完成，则需要避免迭代器被提前释放
            Some((*val.key(), val.value().clone()))
        } else {
            //已迭代完成，则释放迭代器
            self.is_finish = true;
            None
        }
    }
}

///
/// 虚拟页表
///
#[derive(Clone)]
pub struct VirtualPageTable(Arc<InnerVirtualPageTable>);

unsafe impl Send for VirtualPageTable {}
unsafe impl Sync for VirtualPageTable {}

/*
* 虚拟页表同步方法
*/
impl VirtualPageTable {
    /// 获取虚拟页表中虚拟页的数量
    pub fn len(&self) -> usize {
        self.0.map.len()
    }

    /// 判断指定页面id的虚拟页是否存在
    pub fn contains_page(&self, page_id: &u128) -> bool {
        self.0.map.contains_key(page_id)
    }

    /// 获取当前内部页面唯一id
    pub fn current_internal_page_uid(&self) -> u64 {
        self
            .0
            .internal_page_uid_allocator
            .load(Ordering::Acquire)
    }

    /// 分配内部页面唯一ID
    pub fn alloc_internal_page_uid(&self) -> u64 {
        let locked = self.0.wait_flush.lock();
        let inner_page_uid = self.0.internal_page_uid_allocator.fetch_add(1, Ordering::Release);
        drop(locked);

        if inner_page_uid == MAX_PAGE_UID {
            //达到可分配的页面唯一id限制，则立即抛出异常
            panic!("Alloc inner page uid failed, reason: out of limit");
        }

        inner_page_uid
    }

    /// 读取指定内部页的值
    pub fn read_internal_page(&self, page_id: &PageId) -> Option<Vec<u8>> {
        if page_id.is_normal() {
            //忽略读取普通页的值
            return None;
        }

        if let Some(item) = self.0.internal_map.get(&*page_id) {
            Some(item.value().clone())
        } else {
            None
        }
    }

    /// 将值写入指定内部页
    /// 持久化需要等待手动或自动刷新虚拟页表
    pub fn write_internal_page(&self,
                               page_id: PageId,
                               value: &[u8]) -> Option<Vec<u8>> {
        if page_id.is_normal() {
            //忽略写入普通页的值
            return None;
        }

        let r = self
            .0
            .internal_map
            .insert(*page_id, value.to_vec());

        //持久化到虚拟页表对应的日志文件中
        let log_uid = self
            .0
            .file
            .append(LogMethod::PlainAppend,
                    (*page_id).to_le_bytes().as_slice(),
                    value); //写入虚拟页表文件的缓冲区
        self.0.wait_flush.lock().push(log_uid); //将等待刷新的日志id写入等待刷新列表中

        r
    }

    /// 移除指定内部页的值
    pub fn remove_internal_page(&self, page_id: &PageId) -> Option<Vec<u8>> {
        if page_id.is_normal() {
            //忽略移除普通页的值
            return None;
        }

        if let Some((_page_id, value)) = self.0.internal_map.remove(&*page_id) {
            Some(value)
        } else {
            None
        }
    }

    /// 获取当前页面唯一id
    pub fn current_page_uid(&self) -> u64 {
        self
            .0
            .page_uid_allocator
            .load(Ordering::Acquire)
    }

    /// 分配页面唯一id
    pub fn alloc_page_uid(&self) -> u64 {
        let locked = self.0.wait_flush.lock();
        let page_uid = self.0.page_uid_allocator.fetch_add(1, Ordering::Release);
        drop(locked);

        if page_uid == MAX_PAGE_UID {
            //达到可分配的页面唯一id限制，则立即抛出异常
            panic!("Alloc page uid failed, reason: out of limit");
        }

        page_uid
    }

    /// 获取指定页面id所指定的块设备中的块位置
    pub fn addressing(&self, page_id: &PageId) -> Option<u64> {
        if !page_id.is_normal() {
            //忽略获取保留页和内部页的块位置
            return None;
        }

        if let Some(location) = self.0.map.get(&*page_id) {
            Some(location.value().load(Ordering::Acquire))
        } else {
            None
        }
    }

    /// 将指定普通页面注册到虚拟页表，如果指定页面id的页面存在，则返回旧页面的块位置
    pub fn register(&self, page_id: PageId, location: u64) -> Option<u64> {
        if !page_id.is_normal() {
            //忽略保留页和内部页的注册
            return None;
        }

        if let Some(old) = self.0.map.insert(*page_id, AtomicU64::new(location)) {
            let log_uid = self
                .0
                .file
                .append(LogMethod::PlainAppend,
                        (*page_id).to_le_bytes().as_slice(),
                        location.to_le_bytes().as_slice()); //写入虚拟页表文件的缓冲区
            self.0.wait_flush.lock().push(log_uid); //将等待刷新的日志id写入等待刷新列表中

            Some(old.into_inner())
        } else {
            let log_uid = self
                .0
                .file
                .append(LogMethod::PlainAppend,
                        (*page_id).to_le_bytes().as_slice(),
                        location.to_le_bytes().as_slice()); //写入虚拟页表文件的缓冲区
            self.0.wait_flush.lock().push(log_uid); //将等待刷新的日志id写入等待刷新列表中

            None
        }
    }

    /// 更新指定普通页面id的块位置，如果指定页面id的页面存在，且更新成功，则返回旧页面的块位置
    pub fn update_location(&self,
                           page_id: &PageId,
                           old_location: u64,
                           new_location: u64) -> GenResult<Option<u64>, u64> {
        if !page_id.is_normal() {
            //忽略保留页和内部页的更新
            return Ok(None);
        }

        if let Some(location) = self.0.map.get(&*page_id) {
            match location.compare_exchange(old_location,
                                            new_location,
                                            Ordering::Acquire,
                                            Ordering::Relaxed) {
                Err(current) => {
                    //更新失败，则返回当前旧页面的块位置
                    Err(current)
                },
                Ok(_) => {
                    //更新成功，则返回当前旧页面的块位置
                    let log_uid = self
                        .0
                        .file
                        .append(LogMethod::PlainAppend,
                                (*page_id).to_le_bytes().as_slice(),
                                new_location.to_le_bytes().as_slice()); //写入虚拟页表文件的缓冲区
                    self.0.wait_flush.lock().push(log_uid); //将等待刷新的日志id写入等待刷新列表中

                    Ok(Some(old_location))
                },
            }
        } else {
            //指定页面不存在，则返回空的块位置
            Ok(None)
        }
    }

    /// 从虚拟页表中注销指定普通页面，如果指定页面id的页面存在，则返回页面
    pub fn unregister(&self, page_id: &PageId) -> Option<(PageId, u64)> {
        if !page_id.is_normal() {
            //忽略保留页和内部页的注销
            return None;
        }

        if let Some((old_page_id, old_location)) = self.0.map.remove(&*page_id) {
            let log_uid = self
                .0
                .file
                .append(LogMethod::Remove,
                        (*page_id).to_le_bytes().as_slice(),
                        &[]); //写入虚拟页表文件的缓冲区
            self.0.wait_flush.lock().push(log_uid); //将等待刷新的日志id写入等待刷新列表中

            Some((old_page_id.into(), old_location.into_inner()))
        } else {
            None
        }
    }

    /// 获取虚拟页表的普通页迭代器
    pub fn normal_iter(&self) -> VirtualPageTableNormalPgaeIterator {
        let iterator = self.0.map.iter();
        let inner = Box::into_raw(Box::new(iterator)) as usize;

        VirtualPageTableNormalPgaeIterator {
            inner,
            is_finish: false,
        }
    }

    /// 获取虚拟页表的内部页迭代器
    pub fn internal_iter(&self) -> VirtualPageTableInternalPgaeIterator {
        let iterator = self.0.internal_map.iter();
        let inner = Box::into_raw(Box::new(iterator)) as usize;

        VirtualPageTableInternalPgaeIterator {
            inner,
            is_finish: false,
        }
    }

    /// 获取虚拟页表的迭代器
    pub fn iter(&self) -> VirtualPageTableIterator {
        let normal_iterator = self.normal_iter();
        let internal_iterator = self.internal_iter();
        VirtualPageTableIterator {
            internal_iterator,
            normal_iterator,
            is_finish: false,
        }
    }
}

/*
* 虚拟页表异步方法
*/
impl VirtualPageTable {
    /// 构建虚拟页表
    pub async fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                     path: P,
                                     init_page_uid: u64,
                                     init_internal_page_uid: u64,
                                     log_file_limit: usize,
                                     load_buf_len: u64,
                                     is_checksum: bool,
                                     delay_timeout: usize) -> Self {
        if init_page_uid as u128 == EMPTY_PAGE {
            //初始化页面唯一id小于等于空页面id，则立即抛出异常
            panic!("Create virtual page table failed, path: {:?}, reason: invalid init page uid",
                   path.as_ref());
        }

        match LogFile::open(rt.clone(),
                            path.as_ref().to_path_buf(),
                            DEFAULT_COMMIT_LOG_BLOCK_SIZE,
                            log_file_limit,
                            None).await {
            Err(e) => {
                //打开虚拟页表文件失败，则立即抛出异常
                panic!("Open virtual page table failed, path: {:?}, reason: {:?}",
                       path.as_ref(),
                       e);
            },
            Ok(file) => {
                //打开虚拟页表文件成功
                let inner = InnerVirtualPageTable {
                    rt,
                    file,
                    wait_flush: SpinLock::new(Vec::new()),
                    delay_timeout,
                    page_uid_allocator: AtomicU64::new(init_page_uid),
                    internal_page_uid_allocator: AtomicU64::new(init_internal_page_uid),
                    map: DashMap::new(),
                    internal_map: DashMap::new(),
                };
                let table = VirtualPageTable(Arc::new(inner));

                //加载虚拟页表文件
                let now = Instant::now();
                let mut loader = VirtualPageTableLoader::new(table.clone());
                if let Err(e) = table.0.file.load(&mut loader,
                                                  None,
                                                  load_buf_len,
                                                  is_checksum).await {
                    //加载指定的虚拟页表文件失败，则立即抛出异常
                    panic!("Load virtual page table failed, path: {:?}, reason: {:?}",
                           path.as_ref(),
                           e);
                }
                debug!("Load virtual page table ok, path: {:?}, files: {}, page_ids: {}, bytes: {}, time: {:?}",
                    path.as_ref(),
                    loader.log_files_len(),
                    loader.page_id_len(),
                    loader.bytes_len(),
                    now.elapsed());

                table
            },
        }
    }

    /// 刷新对虚拟页表的修改到虚拟页表文件中，并更新当前虚拟页表的元信息
    pub async fn flush(&self) -> Result<()> {
        //获取等待刷新的最大日志id
        let last_log_uid = {
            let mut locked = self.0.wait_flush.lock();

            //更新当前虚拟页表的元信息到超级页
            let mut bytes = Vec::with_capacity(16);
            bytes.put_u64_le(self.current_page_uid());
            bytes.put_u64_le(self.current_internal_page_uid());
            let log_uid = self
                .0
                .file
                .append(LogMethod::PlainAppend,
                        SUPER_PAGE_ID.to_le_bytes().as_slice(),
                        bytes.as_slice()); //写入虚拟页表文件的缓冲区
            locked.push(log_uid); //将等待刷新的日志id写入等待刷新列表中

            locked.sort();
            locked.pop().unwrap()
        };

        let timeout = Some(self.0.delay_timeout);
        if let Err(e) = self.0.file.commit(last_log_uid,
                                           true,
                                           false,
                                           timeout).await {
            //刷新失败，则还原等待刷新的日志id，并返回错误原因
            error!("Flush virtual page table failed, last_log_uid: {:?}, reason: {:?}",
                last_log_uid,
                e);
            self.0.wait_flush.lock().push(last_log_uid);
            return Err(e);
        }

        Ok(())
    }
}

// 内部虚拟页表
struct InnerVirtualPageTable {
    rt:                             MultiTaskRuntime<()>,       //异步运行时
    file:                           LogFile,                    //虚拟页表文件
    wait_flush:                     SpinLock<Vec<usize>>,       //等待刷新的日志id列表
    delay_timeout:                  usize,                      //延迟刷新虚拟页表的时间，单位毫秒
    page_uid_allocator:             AtomicU64,                  //页面唯一id分配器
    internal_page_uid_allocator:    AtomicU64,                  //内部页面唯一id分配器
    map:                            DashMap<u128, AtomicU64>,   //虚拟页映射表
    internal_map:                   DashMap<u128, Vec<u8>>,     //内部虚拟页映射表
}

// 虚拟页表加载器
struct VirtualPageTableLoader {
    is_inited:  bool,                           //是否已初始化虚拟页表
    statistics: XHashMap<PathBuf, (u64, u64)>,  //加载统计信息，包括关键字数量和键值对的字节数
    removed:    XHashMap<Vec<u8>, ()>,          //已删除关键字表
    table:      VirtualPageTable,               //虚拟页表
}

impl PairLoader for VirtualPageTableLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        let id = u128::from_le_bytes(key.as_slice().try_into().unwrap());
        if id == 0 && self.is_inited {
            //已初始化虚拟页表，则忽略页面id为0的记录
            return false;
        }

        //不在已删除页面id表中，且不在虚拟页表中的页面id，才允许被加载
        !self
            .removed
            .contains_key(key)
            &&
            !self
                .table
                .0
                .map
                .contains_key(&id)
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if let Some(value) = value {
            //插入或更新指定页面id的值
            let id = u128::from_le_bytes(key.as_slice().try_into().unwrap());
            if id == 0 {
                //加载元信息
                if !self.is_inited {
                    //未加载虚拟页表的元信息，则立即加载
                    self.is_inited = true;
                    let mut bytes = value.as_slice();
                    let current_page_uid = bytes.get_u64_le();
                    let current_internal_page_uid = bytes.get_u64_le();

                    self
                        .table
                        .0
                        .page_uid_allocator
                        .store(current_page_uid,
                               Ordering::Release);
                    self
                        .table
                        .0
                        .internal_page_uid_allocator
                        .store(current_internal_page_uid,
                               Ordering::Release);
                }
            } else {
                //加载页面
                let page_id = PageId(id);
                if page_id.is_normal() {
                    //加载普通页
                    if let Some(path) = log_file {
                        match self.statistics.entry(path.clone()) {
                            HashMapEntry::Occupied(mut o) => {
                                //指定虚拟页表文件的统计信息存在，则继续统计
                                let statistics = o.get_mut();
                                statistics.0 += 1;
                                statistics.1 += (key.len() + value.len()) as u64;
                            },
                            HashMapEntry::Vacant(v) => {
                                //指定虚拟页表文件的统计信息不存在，则初始化统计
                                v.insert((1, (key.len() + value.len()) as u64));
                            },
                        }
                    }

                    let location = u64::from_le_bytes(value.as_slice().try_into().unwrap());
                    if location != 0 {
                        //当前页面未被释放，则加载到虚拟页表中
                        self.table.0.map.insert(id,
                                                AtomicU64::new(u64::from_le_bytes(value.as_slice().try_into().unwrap())));
                    } else {
                        //当前页面已释放，则不需要加载到虚拟页表中，并记录到已删除页面id表中
                        self.removed.insert(key, ());
                    }
                } else if page_id.is_internal() {
                    //加载内部页
                    if value.len() == 8
                        && u64::from_le_bytes(value.as_slice().try_into().unwrap_or([0; 8])) == 0 {
                        //当前页面已释放，则不需要加载到内部虚拟页表中，并记录到已删除页面id表中
                        self.removed.insert(key, ());
                        return;
                    }

                    if let Some(path) = log_file {
                        match self.statistics.entry(path.clone()) {
                            HashMapEntry::Occupied(mut o) => {
                                //指定虚拟页表文件的统计信息存在，则继续统计
                                let statistics = o.get_mut();
                                statistics.0 += 1;
                                statistics.1 += (key.len() + value.len()) as u64;
                            },
                            HashMapEntry::Vacant(v) => {
                                //指定虚拟页表文件的统计信息不存在，则初始化统计
                                v.insert((1, (key.len() + value.len()) as u64));
                            },
                        }
                    }

                    //当前页面未被释放，则加载到内部虚拟页表中
                    self
                        .table
                        .0
                        .internal_map
                        .insert(id, value);
                } else {
                    //加载保留页
                    unimplemented!()
                }
            }
        } else {
            //删除指定页面id的值，则不需要加载到虚拟页表中，并记录到已删除页面id表中
            self.removed.insert(key, ());
        }
    }
}

impl VirtualPageTableLoader {
    /// 构建一个虚拟页表的加载器
    pub fn new(table: VirtualPageTable) -> Self {
        VirtualPageTableLoader {
            is_inited:  false,
            statistics: XHashMap::default(),
            removed: XHashMap::default(),
            table,
        }
    }

    /// 获取已加载的文件数量
    pub fn log_files_len(&self) -> usize {
        self.statistics.len()
    }

    /// 获取已加载的页面id数量
    pub fn page_id_len(&self) -> u64 {
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.0;
        }

        len
    }

    /// 获取已加载的字节数
    pub fn bytes_len(&self) -> u64 {
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.1;
        }

        len
    }
}

