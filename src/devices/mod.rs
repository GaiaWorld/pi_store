use std::sync::Arc;
use std::fmt::Debug;
use std::ops::Deref;
use std::hash::Hash;
use std::io::{SeekFrom, Result};

use futures::future::BoxFuture;
use url::Url;

use dashmap::DashMap;

pub mod simple_device;

///
/// 块设备中的空块，用于表示不存在、不可用的块或还未分配的块位置
///
pub const EMPTY_BLOCK: u64 = 0;

///
/// 空的块位置，用于表示不存在、不可用或还未分配的块位置
///
pub const EMPTY_BLOCK_LOCATION: BlockLocation = BlockLocation(EMPTY_BLOCK);

///
/// 块设备的类型
///
pub enum DeviceType<D: DeviceDetail> {
    VM(D),      //易失内存
    NVM(D),     //非易失内存
    HDD(D),     //硬盘
    SSD(D),     //固定硬盘
    Disk(D),    //未知硬盘
}

unsafe impl<D: DeviceDetail> Send for DeviceType<D> {}
unsafe impl<D: DeviceDetail> Sync for DeviceType<D> {}

///
/// 块设备详细信息
///
pub trait DeviceDetail: Send + Sync + 'static {
    type Key: Debug + Clone + Hash + Eq + Send + Sync + 'static;
    type Val: Debug + Clone + Hash + Eq + Send + Sync + 'static;

    /// 获取块设备的指定信息
    fn get(&self, key: &Self::Key) -> Option<Self::Val>;
}

///
/// 设备值类型
///
#[derive(Debug, Clone, Hash)]
pub enum DeviceValueType {
    Integer(i128),  //整数
    Str(String),    //字符串
}

unsafe impl Send for DeviceValueType {}
unsafe impl Sync for DeviceValueType {}

impl Eq for DeviceValueType {}
impl PartialEq for DeviceValueType {
    fn eq(&self, other: &Self) -> bool {
        match self {
            DeviceValueType::Integer(x) => {
                match other {
                    DeviceValueType::Integer(y) => {
                        x.to_string().eq(&y.to_string())
                    },
                    DeviceValueType::Str(y) => {
                        x.to_string().eq(y)
                    }
                }
            },
            DeviceValueType::Str(x) => {
                match other {
                    DeviceValueType::Integer(y) => {
                        x.eq(&y.to_string())
                    },
                    DeviceValueType::Str(y) => {
                        x.eq(y)
                    }
                }
            },
        }
    }
}

///
/// 块设备详细信息表
///
pub struct DeviceDetailMap {
    inner:  Arc<DashMap<DeviceValueType, DeviceValueType>>, //共享的块设备详细信息表
}

unsafe impl Send for DeviceDetailMap {}
unsafe impl Sync for DeviceDetailMap {}

impl DeviceDetail for DeviceDetailMap {
    type Key = DeviceValueType;
    type Val = DeviceValueType;

    fn get(&self, key: &Self::Key) -> Option<Self::Val> {
        if let Some(val) = self.inner.get(&key) {
            Some(val.value().clone())
        } else {
            None
        }
    }
}

impl DeviceDetailMap {
    /// 构建块设备详细信息表
    pub fn new() -> Self {
        DeviceDetailMap {
            inner: Arc::new(DashMap::new()),
        }
    }
}

/// 块设备状态
#[derive(Debug, Clone, Hash)]
pub enum DeviceStatus {
    Inited = 0, //已初始化
    Running,    //正在运行
    Busy,       //繁忙
    Pended,     //已挂起
    Closing,    //正在关闭
    Closed,     //已关闭
}

unsafe impl Send for DeviceStatus {}
unsafe impl Sync for DeviceStatus {}

impl From<u8> for DeviceStatus {
    fn from(src: u8) -> Self {
        match src {
            0 => DeviceStatus::Inited,
            1 => DeviceStatus::Running,
            2 => DeviceStatus::Busy,
            3 => DeviceStatus::Pended,
            4 => DeviceStatus::Closing,
            _ => DeviceStatus::Closed,
        }
    }
}

///
/// 块设备统计信息
///
pub struct DeviceStatistics {
    alloc_blocks:       u128,           //累计分配块数量
    alloc_bytes:        u128,           //累计分配大小，单位B
    free_blocks:        u128,           //累计释放块数量
    free_bytes:         u128,           //累计释放大小，单位B
    blocks_in:          u128,           //累计输入块数量
    blocks_out:         u128,           //累计输出块数量
    bytes_in:           u128,           //累计输入大小，单位B
    bytes_out:          u128,           //累计输出大小，单位B
    time:               u128,           //块设备本次运行时长，单位ms
    slow_alloc_count:   Option<u64>,    //块设备慢分配次数
    slow_free_count:    Option<u64>,    //块设备慢释放次数
    slow_in_count:      Option<u64>,    //块设备慢输入次数
    slow_out_count:     Option<u64>,    //块设备慢输出次数
    min_alloc_bytes:    Option<u64>,    //最小分配大小，单位B
    max_alloc_bytes:    Option<u64>,    //最大分配大小，单位B
    min_alloc_time:     Option<u64>,    //最小分配时长，单位ms
    max_alloc_time:     Option<u64>,    //最大分配时长，单位ms
    min_free_time:      Option<u64>,    //最小释放时长，单位ms
    max_free_time:      Option<u64>,    //最大释放时长，单位ms
    min_in_time:        Option<u64>,    //最小输入时长，单位ms
    max_in_time:        Option<u64>,    //最大输入时长，单位ms
    min_out_time:       Option<u64>,    //最小输出时长，单位ms
    max_out_time:       Option<u64>,    //最大输出时长，单位ms
    soft_faults:        Option<u64>,    //软故障次数
    hard_faults:        Option<u64>,    //硬故障次数
}

impl DeviceStatistics {
    /// 构建块设备统计信息
    pub fn new(alloc_blocks: u128,
               alloc_bytes: u128,
               free_blocks: u128,
               free_bytes: u128,
               blocks_in: u128,
               blocks_out: u128,
               bytes_in: u128,
               bytes_out: u128,
               time: u128) -> Self {
        DeviceStatistics {
            alloc_blocks,
            alloc_bytes,
            free_blocks,
            free_bytes,
            blocks_in,
            blocks_out,
            bytes_in,
            bytes_out,
            time,
            slow_alloc_count: None,
            slow_free_count: None,
            slow_in_count: None,
            slow_out_count: None,
            min_alloc_bytes: None,
            max_alloc_bytes: None,
            min_alloc_time: None,
            max_alloc_time: None,
            min_free_time: None,
            max_free_time: None,
            min_in_time: None,
            max_in_time: None,
            min_out_time: None,
            max_out_time: None,
            soft_faults: None,
            hard_faults: None,
        }
    }
}

///
/// 块设备
///
pub trait BlockDevice: Send + Sync + 'static {
    type Uid: Debug + Clone + Hash + Ord + Send + Sync + 'static;
    type Status: Debug + Clone + Hash + Send + Sync + 'static;
    type DetailKey: Debug + Clone + Hash + Eq + Send + Sync + 'static;
    type DetailVal: Debug + Clone + Hash + Eq + Send + Sync + 'static;
    type Detail: DeviceDetail<Key = Self::DetailKey, Val = Self::DetailVal>;
    type Buf: AsRef<[u8]> + Clone + Send + Sync + 'static;

    /// 判断当前块设备是否完全空闲
    fn is_full_free(&self) -> bool;

    /// 判断当前是否是本地块设备
    fn is_local(&self) -> bool;

    /// 判断当前是否是持久化块设备
    fn is_persistent(&self) -> bool;

    /// 判断当前是否是访问安全的块设备
    fn is_security(&self) -> bool;

    /// 判断当前是否是数据稳固的块设备
    fn is_safety(&self) -> bool;

    /// 判断当前块设备是否支持压缩
    fn enable_compression(&self) -> bool;

    /// 判断是否需要外部调用整理方法
    fn is_require_collect(&self) -> bool;

    /// 获取当前块设备的容量，返回空表示无限制容量，单位B
    fn capacity(&self) -> Option<u64>;

    /// 获取当前块设备的可用容量，返回空表示有无限制的可用容量，单位B
    fn avail_size(&self) -> Option<u64>;

    /// 获取当前块设备的已用容量，单位B
    fn used_size(&self) -> u64;

    /// 获取块单位长度，单位B
    fn block_unit_len(&self) -> usize;

    /// 获取最大块大小，单位字节
    fn max_block_size(&self) -> usize;

    /// 获取当前块设备的URL
    fn get_url(&self) -> Option<Url>;

    /// 获取当前块设备的唯一id
    fn get_uid(&self) -> Option<Self::Uid>;

    /// 获取块设备的信息
    fn get_info(&self) -> DeviceType<Self::Detail>;

    /// 获取块设备的状态
    fn get_status(&self) -> Self::Status;

    /// 获取块设备的当前统计信息
    fn statistics(&self) -> Option<DeviceStatistics>;

    /// 异步分配足够容纳指定大小的块，块的实际大小不一定等于指定大小，成功返回块位置，失败返回空块
    /// 分配的块，必须初始化后再使用
    fn alloc_block(&self, size: usize) -> BoxFuture<BlockLocation>;

    /// 异步从块设备的指定块位置上读取数据
    fn read(&self, location: &BlockLocation) -> BoxFuture<Result<Self::Buf>>;

    /// 异步从块设备的指定块位置上写入数据
    fn write(&self, location: &BlockLocation, buf: &Self::Buf) -> BoxFuture<Result<usize>>;

    /// 异步释放指定块位置的块，释放未分配的块或释放被释放过的块是未定义行为
    fn free_block(&self, location: &BlockLocation) -> BoxFuture<bool>;

    /// 异步整理已分配的所有块
    fn collect_alloced_blocks(&self, alloced: &[BlockLocation]) -> BoxFuture<Result<usize>>;
}

///
/// 块位置，指向块设备内的指定物理地址
/// 块位置可以描述的最大块单位数量为0xffff
/// 块位置可以描述的最大寻址空间为0xffffffffffff
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockLocation(u64);

unsafe impl Send for BlockLocation {}
unsafe impl Sync for BlockLocation {}

impl From<u64> for BlockLocation {
    fn from(src: u64) -> Self {
        BlockLocation::new(src)
    }
}

impl From<BlockLocation> for u64 {
    fn from(BlockLocation(locaction): BlockLocation) -> Self {
        locaction
    }
}

impl Deref for BlockLocation {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<u64> for BlockLocation {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl BlockLocation {
    /// 创建块位置
    #[inline]
    pub(crate) fn new(location: u64) -> Self {
        BlockLocation(location)
    }

    /// 创建空块
    #[inline]
    pub fn empty() -> Self {
        BlockLocation(EMPTY_BLOCK)
    }

    /// 判断当前块位置是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}