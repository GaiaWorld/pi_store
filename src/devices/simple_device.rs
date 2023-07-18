use std::mem;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::time::{Instant, SystemTime};
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU8, Ordering}};
use std::fmt::{Result as GenResult, Formatter, Debug};

use futures::future::{FutureExt, BoxFuture};
use parking_lot::Mutex;
use crossbeam_channel::{Sender, Receiver, unbounded};
use url::Url;
use bytes::{Buf, BufMut, buf::UninitSlice};
use crc32fast::Hasher;
use log::error;

use pi_guid::Guid;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime, multi_thread::MultiTaskRuntime}};
use pi_async_file::file::{AsyncFileOptions, AsyncFile, WriteOptions};

use crate::devices::{DeviceType, DeviceValueType, DeviceDetailMap, DeviceStatus, DeviceStatistics, BlockDevice, BlockLocation, EMPTY_BLOCK_LOCATION};

/// 块头由4B块负载长度 + 4字节校验码 + 8字节写入时间，单位ms
const DEFAULT_BLOCK_HEAD_LEN: usize = 16;

/// 默认的块单位长度，单位B
const DEFAULT_BLOCK_UNIT_LEN: u64 = 4096;

/// 最小的块单位长度，单位B
const MIN_BLOCK_UNIT_LEN: u64 = 1;

/// 最大的块单位长度，单位B
const MAX_BLOCK_UNIT_LEN: u64 = 65535;

/// 最大块单位数量，单位个
const MAX_BLOCK_UNIT_COUNT: u64 = 65535;

///
/// 简单块设备的线程安全的二进制数据
///
pub struct Binary(AtomicBool, Option<Arc<Vec<u8>>>);

unsafe impl Send for Binary {}
unsafe impl Sync for Binary {}

impl Drop for Binary {
    fn drop(&mut self) {
        if !self.0.load(Ordering::Relaxed) {
            //已复制过，则忽略二进制数据的释放
            mem::forget(self.1.take().unwrap());
        }
    }
}

impl Clone for Binary {
    fn clone(&self) -> Self {
        //将被复制的简单块设备的线程安全的二进制数据设置为不可写，复制二进制数据的共享指针，并将复制的简单块设备的线程安全的二进制数据设置为可写
        //每一个简单块设备的线程安全的二进制数据只允许被复制一次，被复制的简单块设备的线程安全的二进制数据只允许进行只读操作，而复制出的新简单块设备的线程安全的二进制数据可写
        if let Ok(true) = self.0.compare_exchange(true,
                                   false,
                                   Ordering::SeqCst,
                                   Ordering::Acquire) {
            //未复制过，则可以复制
            Binary(AtomicBool::new(true),
                   Some(unsafe { Arc::from_raw(Arc::as_ptr(self.1.as_ref().unwrap())) }))
        } else {
            //已复制过，则立即抛出异常
            panic!("Clone failed, reason: already be cloned");
        }
    }
}

impl Debug for Binary {
    fn fmt(&self, f: &mut Formatter<'_>) -> GenResult {
        write!(f,
               "Binary<{:?}, {:?}>",
               self.0.load(Ordering::Acquire), self.1.as_ref().unwrap())
    }
}

impl AsRef<[u8]> for Binary {
    fn as_ref(&self) -> &[u8] {
        self
            .1
            .as_ref()
            .unwrap()
            .as_slice()
    }
}

impl AsMut<[u8]> for Binary {
    fn as_mut(&mut self) -> &mut [u8] {
        if self.0.load(Ordering::Relaxed) {
            //未复制过，则可以获取可写引用
            Arc::get_mut(self
                .1
                .as_mut()
                .unwrap())
                .unwrap()
                .as_mut_slice()
        } else {
            //已复制过，则立即抛出异常
            panic!("Get mut failed, reason: already be cloned");
        }
    }
}

unsafe impl BufMut for Binary {
    fn remaining_mut(&self) -> usize {
        self
            .1
            .as_ref()
            .unwrap()
            .remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        if self.0.load(Ordering::Relaxed) {
            Arc::get_mut(self.1.as_mut().unwrap())
                .unwrap()
                .advance_mut(cnt);
        } else {
            //已复制过，则立即抛出异常
            panic!("Advance mut failed, reason: already be cloned");
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        if self.0.load(Ordering::Relaxed) {
            Arc::get_mut(self.1.as_mut().unwrap())
                .unwrap()
                .chunk_mut()
        } else {
            //已复制过，则立即抛出异常
            panic!("Chunk mut failed, reason: already be cloned");
        }
    }

    fn put<T: Buf>(&mut self, mut src: T) {
        if self.0.load(Ordering::Relaxed) {
            Arc::get_mut(self.1.as_mut().unwrap())
                .unwrap()
                .put(src);
        } else {
            //已复制过，则立即抛出异常
            panic!("Put to bytes failed, reason: already be cloned");
        }
    }

    fn put_slice(&mut self, src: &[u8]) {
        if self.0.load(Ordering::Relaxed) {
            Arc::get_mut(self.1.as_mut().unwrap())
                .unwrap()
                .put_slice(src);
        } else {
            //已复制过，则立即抛出异常
            panic!("Put slice to bytes failed, reason: already be cloned");
        }
    }
}

impl Binary {
    /// 构建简单块设备的线程安全的二进制数据，新构建的简单块设备的线程安全的二进制数据可写
    pub fn new(bin: Vec<u8>) -> Self {
        Binary(AtomicBool::new(true), Some(Arc::new(bin)))
    }

    /// 获取简单块设备的线程安全的二进制数据的长度
    pub fn len(&self) -> usize {
        self
            .1
            .as_ref()
            .unwrap()
            .len()
    }
}

///
/// 简单块设备
/// 块位置的最大寻址空间为0xffffffffffff，如果默认块单位长度为4KB，则单个简单块设备的最大容量为1023PB
/// 每个块最大容量为65535个块单位，默认块单位长度为4KB，则每个块的最大容量为255MB
///
pub struct SimpleDevice {
    rt:                 MultiTaskRuntime<()>,           //运行时
    path:               PathBuf,                        //文件路径
    file:               AsyncFile<()>,                  //文件
    frees:              Arc<Mutex<BTreeMap<u64, u64>>>, //不持久化的空闲列表，关键字为块在对齐后的字节大小，值为块在对齐后的偏移
    status:             AtomicU8,                       //状态
    block_unit_len:     u64,                            //块单位长度，单位字节
    statistics:         Vec<SpinLock<u128>>,            //统计数据
    statistics_recv:    Vec<Receiver<u128>>,            //统计数据接收器
    statistics_sent:    Vec<Sender<u128>>,              //统计数据采集器
    timeling:           Instant,                        //块设备运行时长
}

unsafe impl Send for SimpleDevice {}
unsafe impl Sync for SimpleDevice {}

impl BlockDevice for SimpleDevice {
    type Uid = Guid;
    type Status = DeviceStatus;
    type DetailKey = DeviceValueType;
    type DetailVal = DeviceValueType;
    type Detail = DeviceDetailMap;
    type Buf = Binary;

    fn is_full_free(&self) -> bool {
        self.file.get_size() == 0
    }

    fn is_local(&self) -> bool {
        true
    }

    fn is_persistent(&self) -> bool {
        true
    }

    fn is_security(&self) -> bool {
        false
    }

    fn is_safety(&self) -> bool {
        false
    }

    fn enable_compression(&self) -> bool {
        false
    }

    fn is_require_collect(&self) -> bool {
        true
    }

    fn capacity(&self) -> Option<u64> {
        None
    }

    fn avail_size(&self) -> Option<u64> {
        None
    }

    fn used_size(&self) -> u64 {
        self.file.get_size()
    }

    fn block_unit_len(&self) -> usize {
        self.block_unit_len as usize
    }

    fn max_block_size(&self) -> usize {
        self.block_unit_len as usize * MAX_BLOCK_UNIT_COUNT as usize
    }

    fn get_url(&self) -> Option<Url> {
        if let Ok(url) = Url::from_directory_path(&self.path) {
            Some(url)
        } else {
            None
        }
    }

    fn get_uid(&self) -> Option<Self::Uid> {
        None
    }

    fn get_info(&self) -> DeviceType<Self::Detail> {
        DeviceType::Disk(DeviceDetailMap::new())
    }

    fn get_status(&self) -> Self::Status {
        self
            .status
            .load(Ordering::Relaxed)
            .into()
    }

    fn statistics(&self) -> Option<DeviceStatistics> {
        //延迟接收采集的统计数据
        let mut index = 0;
        for receiver in &self.statistics_recv {
            if let Ok(r) = receiver.recv() {
                *self.statistics[index].lock() += r;
            } else {
                index += 1;
                continue;
            }
        }

        Some(DeviceStatistics::new(*self.statistics[0].lock(),
                                   *self.statistics[1].lock(),
                                   *self.statistics[2].lock(),
                                   *self.statistics[3].lock(),
                                   *self.statistics[4].lock(),
                                   *self.statistics[5].lock(),
                                   *self.statistics[6].lock(),
                                   *self.statistics[7].lock(),
                                   self.timeling.elapsed().as_millis()))
    }

    /// 简单块设备分配的块位置由块在文件中的偏移和用户指定的块大小组成，块大小的单位为4KB
    /// 一次最大可以分配255MB大小的块，即size为65535
    /// 块位置由2B块大小 + 6字节偏移组成
    fn alloc_block(&self, size: usize) -> BoxFuture<BlockLocation> {
        let url = self.get_url();
        let file = self.file.clone();
        let frees = self.frees.clone();
        let sender0 = self.statistics_sent[0].clone();
        let sender1 = self.statistics_sent[1].clone();

        async move {
            if size == 0 {
                //忽略0长度的块分配
                return EMPTY_BLOCK_LOCATION;
            }
            let block_align_size = block_align_size(size as u64 + DEFAULT_BLOCK_HEAD_LEN as u64, self.block_unit_len);

            {
                let mut locked = frees.lock();
                if let Some((max, _)) = locked.last_key_value() {
                    //当前空闲列表不为空
                    let mut key = None;
                    for (k, _) in locked.range((Included(&(block_align_size as u64)), Included(max))) {
                        //有适合大小的空闲块，则立即设置空闲块的位置
                        key = Some(*k);
                        break;
                    };
                    if let Some(key) = key {
                        //有空闲块的位置，则立即返回空闲块的位置
                        let val = locked.remove(&key).unwrap();
                        return to_location(val, block_align_count(block_align_size, self.block_unit_len));
                    }
                }
            }

            let top = file.get_size();
            let block_align_pos = block_align_pos(top, self.block_unit_len);
            if let Ok(inner) = file.get_inner() {
                //获取内部文件成功，则扩容当前文件大小
                if let Err(e) = inner.set_len(top + block_align_size) {
                    //扩容当前文件大小失败，则立即返回分配失败
                    error!("Alloc simple block failed, url: {:?}, top: {}, block_align_top: {}, size: {}, block_align_size: {}, reason: {:?}",
                        url,
                        top,
                        block_align_pos,
                        size,
                        block_align_size,
                        e);
                    return EMPTY_BLOCK_LOCATION;
                }

                //采集块分配的统计信息
                let _ = sender0.send(1);
                let _ = sender1.send(size as u128);

                //扩容当前文件大小成功，则立即返回分配的块地址
                to_location(block_align_pos, block_align_count(block_align_size, self.block_unit_len))
            } else {
                //获取内部文件失败，则立即返回分配失败
                error!("Alloc simple block failed, url: {:?}, top: {}, block_align_top: {}, size: {}, block_align_size: {}, reason: access inner file error",
                    url,
                    top,
                    block_align_pos,
                    size,
                    block_align_size);
                EMPTY_BLOCK_LOCATION
            }
        }.boxed()
    }

    /// 异步从块设备的指定块位置上读取数据
    /// 块数据由4B块负载长度 + 4字节校验码 + 8字节写入时间，单位ms + 负载组成
    fn read(&self, location: &BlockLocation) -> BoxFuture<Result<Self::Buf>> {
        let url = self.get_url();
        let file = self.file.clone();
        let block_unit_len = self.block_unit_len;
        let sender0 = self.statistics_sent[4].clone();
        let sender1 = self.statistics_sent[6].clone();

        let (block_align_pos, block_align_count) = to_pos_size(location);
        async move {
            if block_align_count == 0 {
                //忽略读取空块
                return Ok(Binary::new(Vec::new()));
            }

            let block_byte_pos = block_byte_pos(block_align_pos, block_unit_len);
            let block_align_size = block_align_count_to_block_align_size(block_align_count, block_unit_len) as usize;
            match file.read(block_byte_pos, block_align_size).await {
                Err(e) => {
                    error!("Read simple block failed, url: {:?}, block_byte_pos: {}, block_align_size: {}, reason: {:?}", url, block_byte_pos, block_align_size, e);
                    Err(e)
                },
                Ok(mut bin) => {
                    //读取成功，则采集块读取的统计信息，并返回读取的块数据
                    let _ = sender0.send(1);
                    let _ = sender1.send(block_align_count as u128);

                    let (payload_len, checksum, _time) = read_header(bin.as_mut_slice());
                    read_payload_checked(bin.as_slice(), payload_len as usize, checksum)
                }
            }
        }.boxed()
    }

    /// 异步从块设备的指定块位置上写入数据
    /// 块数据由4B块负载长度 + 4字节校验码 + 8字节写入时间，单位ms + 负载组成
    fn write(&self, location: &BlockLocation, buf: &Self::Buf) -> BoxFuture<Result<usize>> {
        let url = self.get_url();
        let file = self.file.clone();
        let block_unit_len = self.block_unit_len;
        let sender0 = self.statistics_sent[5].clone();
        let sender1 = self.statistics_sent[7].clone();
        let buf_copy = buf.clone();

        let (block_align_pos, block_align_count) = to_pos_size(location);
        async move {
            if block_align_count == 0 {
                //忽略写入空块
                return Ok(0);
            }

            let buf_len = buf_copy.len();
            let block_byte_pos = block_byte_pos(block_align_pos, block_unit_len);
            let block_align_size = block_align_count_to_block_align_size(block_align_count, block_unit_len) as usize;
            if block_align_size < DEFAULT_BLOCK_HEAD_LEN + buf_len as usize {
                //写入数据超过指定块的容量，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other, format!("Write simple block failed, url: {:?}, block_byte_pos: {}, block_align_size: {}, payload_len: {}, reason: out of block capacity", url, block_byte_pos, block_align_size, buf_len)));
            }

            let mut hasher = Hasher::new();
            let buffer = write_header(buf_copy, hasher, buf_len);
            match file.write(block_byte_pos,
                             buffer,
                             WriteOptions::Sync(true)).await {
                Err(e) => {
                    error!("Write simple block failed, url: {:?}, block_byte_pos: {}, block_align_size: {}, reason: {:?}", url, block_byte_pos, block_align_size, e);
                    Err(e)
                },
                Ok(size) => {
                    //写入成功，则采集块写入的统计信息，并返回写入的数据长度
                    let _ = sender0.send(1);
                    let _ = sender1.send(buf_len as u128);

                    Ok(size)
                },
            }
        }.boxed()
    }

    /// 异步释放指定块位置的块，释放未分配的块或释放被释放过的块是未定义行为
    /// 块位置由2B块大小 + 6字节偏移组成
    fn free_block(&self, location: &BlockLocation) -> BoxFuture<bool> {
        let (_block_align_pos, block_align_count) = to_pos_size(location);
        async move {
            if block_align_count == 0 {
                //忽略释放空块
                return true;
            }

            false
        }.boxed()
    }

    /// 异步整理已分配的所有块，并在内存中标记出空闲块
    fn collect_alloced_blocks(&self, alloced: &[BlockLocation]) -> BoxFuture<Result<usize>> {
        let rt = self.rt.clone();
        let file = self.file.clone();
        let url = self.get_url();
        let frees = self.frees.clone();
        let block_unit_len = self.block_unit_len;
        let mut locationes = Vec::with_capacity(alloced.len());
        for location in alloced {
            let (align_pos, align_count) = to_pos_size(location);
            locationes.push((align_pos, align_count));
        }

        async move {
            locationes.sort(); //根据pos，从小到大得排序

            let mut count = 0;
            let mut offset = 0;
            let mut top = 0;
            for (align_pos, align_count) in locationes {
                let block_byte_pos = block_byte_pos(align_pos, block_unit_len);
                let block_align_size = block_align_count_to_block_align_size(align_count, block_unit_len);

                count += 1;
                if count % 10000 == 0 {
                    //每整理10000个块，则异步休眠1ms
                    rt.timeout(1).await;
                }

                if block_byte_pos == 0 {
                    //如果当前已分配块是初始块，则计算并记录空闲块的位置和大小
                    let file_length = file.get_size();
                    if block_align_size > file_length {
                        //无效的块大小，则立即返回错误
                        return Err(Error::new(ErrorKind::Other, format!("Collect simple block device failed, url: {:?}, last_pos: {}, last_len: {}, block_byte_pos: {}, block_align_size: {}, reason: invalid block size", url, offset, top, block_byte_pos, block_align_size)));
                    }

                    frees.lock().insert(block_align_size, 0);

                    offset = block_byte_pos;
                    top += block_align_size;
                    continue;
                } else {
                    if block_byte_pos > top {
                        //如果当前已分配块的偏移大于顶指针
                        //表示当前分配块与上一个分配块之间存在空闲块，则计算并记录空闲块的位置和大小
                        frees
                            .lock()
                            .insert(block_byte_pos - top,
                                    block_align_pos(top, block_unit_len));

                        offset = block_byte_pos;
                        top += block_align_size;
                        continue;
                    } else if block_byte_pos == top {
                        //如果当前已分配块的偏移与顶指针相同
                        //表示当前分配块与上一个分配块之间没有空闲块，则继续整理下一个分配块
                        offset = block_byte_pos;
                        top += block_align_size;
                        continue;
                    } else {
                        //如果当前已分配块的偏移小于顶指针
                        //表示出现分配块交叠，则立即返回错误
                        return Err(Error::new(ErrorKind::Other, format!("Collect simple block device failed, url: {:?}, last_pos: {}, last_len: {}, block_byte_pos: {}, block_align_size: {}, reason: block overlap", url, offset, top, block_byte_pos, block_align_size)));
                    }
                }
            }

            Ok(frees.lock().len())
        }.boxed()
    }
}

impl SimpleDevice {
    /// 打开指定路径文件的简单块设备
    pub async fn open<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                      path: P,
                                      block_unit_len: Option<u64>) -> Result<Self> {
        let file = AsyncFile::open(rt.clone(), path.as_ref().to_path_buf(), AsyncFileOptions::ReadWrite).await?;
        let path = path.as_ref().to_path_buf();
        let frees = Arc::new(Mutex::new(BTreeMap::new()));
        let status = AtomicU8::new(DeviceStatus::Inited as u8);
        let block_unit_len = if let Some(len) = block_unit_len {
            //已设置块单位长度
            if len > MIN_BLOCK_UNIT_LEN {
                //块单位长度大于最小块单位长度
                if len < MAX_BLOCK_UNIT_LEN {
                    //块单位长度小于最大块单位长度
                    len
                } else {
                    //块单位长度大于等于最大块单位长度
                    MAX_BLOCK_UNIT_LEN
                }
            } else {
                //块单位长度小于等于最小块单位长度
                MIN_BLOCK_UNIT_LEN
            }
        } else {
            //未设置块单位长度
            DEFAULT_BLOCK_UNIT_LEN
        };
        let statistics = Vec::with_capacity(8);
        let mut statistics_recv = Vec::with_capacity(8);
        let mut statistics_sent = Vec::with_capacity(8);
        for _ in 0..8 {
            let (sender, receiver) = unbounded();
            statistics_recv.push(receiver);
            statistics_sent.push(sender);
        }
        let timeling = Instant::now();

        Ok(SimpleDevice {
            rt,
            file,
            path,
            frees,
            status,
            block_unit_len,
            statistics,
            statistics_recv,
            statistics_sent,
            timeling,
        })
    }
}

// 根据指定块在文件中的实际字节位置和指定的块单位长度，计算块在对齐后的块位置
#[inline]
fn block_align_pos(byte_pos: u64, block_unit_len: u64) -> u64 {
    byte_pos / block_unit_len
}

// 根据指定块在对齐后的块位置和指定的块单位长度，计算块在文件中的实际字节位置
#[inline]
fn block_byte_pos(align_pos: u64, block_unit_len: u64) -> u64 {
    align_pos * block_unit_len
}

// 根据指定块的头和负载字节大小和指定的块单位长度，计算块在对齐后的字节大小
// 外部必须保证byte_size和block_unit_len不为0
#[inline]
fn block_align_size(header_payload_size: u64, block_unit_len: u64) -> u64 {
    let real_size = header_payload_size - 1;
    real_size + (block_unit_len - real_size % block_unit_len)
}

// 根据指定块在对齐后的字节大小和指定的块单位长度，计算块在对齐后的块单位数量
// 外部必须保证align_size和block_unit_len不为0
#[inline]
fn block_align_count(align_size: u64, block_unit_len: u64) -> u64 {
    align_size / block_unit_len
}

// 根据指定块在对齐后的块单位数量和指定的块单位长度，计算块在对齐后的字节大小
// 外部必须保证align_count和block_unit_len不为0
#[inline]
fn block_align_count_to_block_align_size(align_count: u64, block_unit_len: u64) -> u64 {
    align_count * block_unit_len
}

// 将块在对齐后的偏移和块在对齐后的块单位数量转换成块位置
// 注意偏移不是字节偏移，大小不是负载大小或块的字节大小
#[inline]
fn to_location(pos: u64, size: u64) -> BlockLocation {
    BlockLocation::new((pos << 48) | (size & 0xffff))
}

// 将块位置转换为块在对齐后的偏移和块在对齐后的块单位数量
// 注意偏移不是字节偏移，大小不是负载大小或块的字节大小
#[inline]
fn to_pos_size(location: &BlockLocation) -> (u64, u64) {
    let inner = *location.as_ref();
    ((inner >> 48) & 0xffffffffffff, inner & 0xffff)
}

//获取当前系统时间，错误返回0，单位ms
fn now_unix_epoch() -> u64 {
    if let Ok(time) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        return time.as_millis() as u64;
    }

    0
}

//从缓冲区读取日志头，结构: 4B块负载长度 + 4字节校验码 + 8字节写入时间，单位ms
#[inline]
fn read_header(mut buf: &[u8]) -> (u32, u32, u64) {
    //读取负载长度
    let len = buf.get_u32_le();

    //读取校验码
    let checksum = buf.get_u32_le();

    //读取写入时的系统时间
    let time = buf.get_u64_le();

    (len, checksum, time)
}

//从缓冲区读取经过校验的负载
#[inline]
fn read_payload_checked(buf: &[u8], len: usize, checksum: u32) -> Result<Binary> {
    let payload = &buf[DEFAULT_BLOCK_HEAD_LEN..DEFAULT_BLOCK_HEAD_LEN + len];

    //验证负载校验码
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let hash = hasher.finalize();
    if hash != checksum {
        //验证负载校验码失败，则立即返回错误原因
        return Err(Error::new(ErrorKind::Other, format!("Read payload failed, checksum: {}, real: {}, reason: invalid checksum", checksum, hash)));
    }

    Ok(Binary::new(payload.to_vec()))
}

//将日志头写入缓冲区，结构: 4B块负载长度 + 4字节校验码 + 8字节写入时间，单位ms
#[inline]
fn write_header(bin: Binary,
                mut hasher: Hasher,
                len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(DEFAULT_BLOCK_HEAD_LEN + len);

    //填充负载长度
    buf.put_u32_le(len as u32);

    //计算并填充校验码
    let slice = bin.as_ref();
    hasher.update(slice);
    let hash = hasher.finalize();
    buf.put_u32_le(hash);

    //填充当前系统时间
    buf.put_u64_le(now_unix_epoch());

    //填充负载
    buf.put_slice(slice);

    buf
}

