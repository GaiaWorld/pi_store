use std::ops::Deref;
use std::collections::VecDeque;
use std::io::{Error, Result as IOResult, ErrorKind};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::sync::atomic::AtomicU64;

use async_channel::{Sender as AsyncSender, Receiver as AsyncReceiver, bounded as async_bounded};
use bytes::{Buf, BufMut};

use pi_async_rt::lock::spin_lock::SpinLock;

pub mod page_manager;
pub mod page_pool;
pub mod page_table;
pub mod page_cache;

///
/// 虚拟页表中的空页，用于表示不存在或不可用的页，在持久化时用于描述虚拟页表的元信息所在的页
///
pub const EMPTY_PAGE: u128 = 0;

///
/// 虚拟页管理器的块设备编号，即虚拟页管理器是一个无法持久化的默认块设备
///
const VIRTUAL_PAGE_MANAGER_DEVICES_INDEX: u8 = 0;

///
/// 写指令的空编号，用于表示不存在或不可用的写指令
///
const EMPTY_WRITE_INDEX: u64 = 0;

///
/// 虚拟页的写增量
/// 所有对虚拟页的写操作，都由写增量表达，写增量之间是有顺序的
///
pub trait VirtualPageWriteDelta: Send + 'static {
    type Content: Send + Sized + 'static;  //写增量内容

    /// 获取写增量的大小，单位字节
    fn size(&self) -> usize;

    /// 获取写增量所在写指令的编号
    fn get_cmd_index(&self) -> u64;

    /// 设置写增量所在写指令的编号
    fn set_cmd_index(&mut self, cmd_index: u64);

    /// 获取写增量对应的原始的虚拟页的唯一id
    /// 表示当前写增量是相对于指定的原始虚拟页的增量，也就是增量不需要更改虚拟页唯一id
    /// 原始的虚拟页的唯一id与复制的虚拟页的唯一id相同，则表示是同一个虚拟页
    fn get_origin_page_id(&self) -> PageId;

    /// 获取写增量对应的复制的虚拟页的唯一id
    /// 表示当前写增量是作用于指定目标虚拟页的增量，也就是增量需要更改虚拟页唯一id
    /// 复制的虚拟页的唯一id与原始的虚拟页的唯一id相同，则表示是同一个虚拟页
    fn get_copied_page_id(&self) -> PageId;

    /// 获取写增量的类型
    fn get_type(&self) -> usize;

    /// 获取写增量内容
    fn inner(self) -> Self::Content;
}

///
/// 写时复制的虚拟页缓冲
/// 用于虚拟页池中页缓冲的基页，以及封装对基页的写操作
///
pub trait VirtualPageBuf: Clone + Send + Sync + 'static {
    type Content: Send + 'static; //写增量的内容
    type Delta: VirtualPageWriteDelta<Content = Self::Content>; //写增量
    type Output: Send + 'static; //读输出
    type Bin: BufMut + AsRef<[u8]> + AsMut<[u8]> + Clone + Send + Sync + 'static; //序列化输出

    /// 使用指定的原始的虚拟页的唯一id，复制的虚拟页的唯一id和页类型，构建一个写时复制的虚拟页缓冲
    /// 刚构建的虚拟页缓冲，必须保证不缺页
    fn with_page_type(origin_page_id: PageId,
                      copied_page_id: PageId,
                      page_type: Option<usize>) -> Self;

    /// 获取原始的虚拟页的唯一id
    fn get_original_page_id(&self) -> PageId;

    /// 获取复制的虚拟页的唯一id
    fn get_copied_page_id(&self) -> PageId;

    /// 判断当前虚拟页缓冲是否缺页
    fn is_missing_pages(&self) -> bool;

    /// 获取当前虚拟页缓冲的类型
    fn get_page_type(&self) -> usize;

    /// 获取当前虚拟页缓冲的大小，单位B
    fn page_size(&self) -> usize;

    /// 读虚拟页缓冲为指定的输出
    fn read_page(&self) -> Self::Output;

    /// 写增量，以修改虚拟页缓冲
    fn write_page_delta(&mut self, delta: Self::Delta) -> Result<(), String>;

    /// 将二进制数据反序列化为虚拟页缓冲
    fn deserialize_page<Input>(&mut self, bin: Input)
        where Input: AsRef<[u8]> + Send + Sized + 'static;

    /// 将虚拟页缓冲序列化为二进制数据
    fn serialize_page(self) -> Self::Bin;
}

///
/// 页面id，是指向虚拟块地址的指针
/// 页面id的最高位字节，作为块设备号，即虚拟页管理器支持最多挂载255个块设备，0号设备为当前虚拟页管理器
/// 页面id分配后，将不会改变，只会在确认释放后被回收
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageId(u128);

unsafe impl Send for PageId {}
unsafe impl Sync for PageId {}

impl From<u128> for PageId {
    fn from(src: u128) -> Self {
        PageId::new(src)
    }
}

impl From<PageId> for u128 {
    fn from(src: PageId) -> Self {
        src.0
    }
}

impl Deref for PageId {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<u128> for PageId {
    fn as_ref(&self) -> &u128 {
        &self.0
    }
}

impl PageId {
    /// 创建页面id
    #[inline]
    pub(crate) fn new(id: u128) -> Self {
        PageId(id)
    }

    /// 创建空页
    #[inline]
    pub fn empty() -> Self {
        PageId(EMPTY_PAGE)
    }

    /// 判断是否是空页
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0 == EMPTY_PAGE
    }

    /// 获取页面所属的虚拟页管理器的唯一id
    #[inline]
    pub fn owner_uid(&self) -> u32 {
        (self.0 >> 96) as u32
    }

    /// 获取页面所属的块设备在虚拟页管理器中的偏移
    #[inline]
    pub fn device_offset(&self) -> u32 {
        (self.0 >> 64) as u32
    }

    /// 获取页面所属的虚拟页表的虚拟页唯一id
    #[inline]
    pub fn page_uid(&self) -> u64 {
        self.0 as u64
    }
}

///
/// 虚拟页写指令，用于批量处理关联的多个虚拟页写增量，
/// 虚拟页管理器在逻辑上会一次处理一个写指令
/// 虚拟页写指令的任意写增量执行失败，则整个写指令执行失败
///
pub struct VirtualPageWriteCmd<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
>(Arc<InnerVirtualPageWriteCmd<C, D>>);

unsafe impl<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> Send for VirtualPageWriteCmd<C, D> {}
unsafe impl<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> Sync for VirtualPageWriteCmd<C, D> {}

impl<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> Clone for VirtualPageWriteCmd<C, D> {
    fn clone(&self) -> Self {
        VirtualPageWriteCmd(self.0.clone())
    }
}

/*
* 虚拟页写指令同步方法
*/
impl<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> VirtualPageWriteCmd<C, D> {
    /// 构建一个空的虚拟页写指令
    pub fn new() -> Self {
        VirtualPageWriteCmd::with_capacity(8, 1)
    }

    /// 构建一个指定初始容量的空的虚拟页写指令
    pub fn with_capacity(capacity: usize,
                         followup_capacity: usize) -> Self {
        let deltas = SpinLock::new(VecDeque::with_capacity(capacity));
        let followups = SpinLock::new(VecDeque::with_capacity(followup_capacity));
        let deltas_count = AtomicUsize::new(0);
        let follow_up_count = AtomicUsize::new(0);
        let (sender, receiver) = async_bounded(capacity + followup_capacity);

        let inner = InnerVirtualPageWriteCmd {
            index: AtomicU64::new(0), //默认的写指令编号
            deltas,
            followups,
            deltas_count,
            follow_up_count,
            sender,
            receiver,
        };

        VirtualPageWriteCmd(Arc::new(inner))
    }

    /// 判断写指令的写增量缓冲是否已同步完成
    pub fn is_synced_deltas(&self) -> bool {
        self.0.deltas_count.load(Ordering::Relaxed) == 0
    }

    /// 判断写指令的后续写增量缓冲是否已同步完成
    pub fn is_synced_followup_deltas(&self) -> bool {
        self.0.follow_up_count.load(Ordering::Relaxed) == 0
    }

    /// 获取写增量队列的长度
    pub fn deltas_len(&self) -> usize {
        self
            .0
            .deltas
            .lock()
            .len()
    }

    /// 获取后续写增量队列的长度
    pub fn followup_len(&self) -> usize {
        self
            .0
            .followups
            .lock()
            .len()
    }

    /// 获取当前写指令的编号
    pub fn get_index(&self) -> u64 {
        self.0.index.load(Ordering::Relaxed)
    }

    /// 设置当前写指令的编号
    pub fn set_index(&mut self, index: u64) {
        self.0.index.store(index, Ordering::Relaxed);
    }

    /// 从写指令的写增量队列头弹出一个写增量，写增量队列为空，则返回空
    pub fn pop_front(&self) -> Option<D> {
        let mut delta = self
            .0
            .deltas
            .lock()
            .pop_front();

        if let Some(inner) = delta.as_mut() {
            //为写增量设置写指令编号
            inner.set_cmd_index(self.get_index());
        }

        delta
    }

    /// 追加指定的虚拟页写增量到虚拟页写指令的写增量队列
    pub fn append(&self, mut delta: D) {
        //追加写增量
        self
            .0
            .deltas
            .lock()
            .push_back(delta);

        //增加写增量计数
        self.0.deltas_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 从写指令的后续写增量队列头弹出一个写增量，后续写增量队列为空，则返回空
    pub fn pop_front_from_followup(&self) -> Option<D> {
        let mut delta = self
            .0
            .followups
            .lock()
            .pop_front();

        if let Some(inner) = delta.as_mut() {
            //为写增量设置写指令编号
            inner.set_cmd_index(self.get_index());
        }

        delta
    }

    /// 追加指定的虚拟页写增量到虚拟页写指令的后续写增量队列
    /// 后续写增量队列中的写增量会在写增量队列全部执行完成后，再执行
    pub fn follow_up(&self, mut delta: D) {
        //追加写增量
        self
            .0
            .followups
            .lock()
            .push_back(delta);

        //增加后续执行的写增量计数
        self.0.follow_up_count.fetch_add(1, Ordering::Relaxed);
    }
}

/*
* 虚拟页写指令异步方法
*/
impl<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> VirtualPageWriteCmd<C, D> {
    /// 异步写增量到块设备后的回调
    pub async fn callback_by_sync(&self, result: IOResult<u64>) -> Result<(), String> {
        if let Ok(count) = &result {
            let sync_count = *count as usize;

            let current_deltas_count = self
                .0
                .deltas_count
                .load(Ordering::Relaxed);
            if current_deltas_count == 0 {
                //写指令的所有写增量成功同步到块设备
                let current_follow_up_count = self
                    .0
                    .follow_up_count
                    .load(Ordering::Relaxed);

                self
                    .0
                    .follow_up_count
                    .store(current_follow_up_count
                               .checked_sub(sync_count)
                               .unwrap_or(0),
                           Ordering::Relaxed); //减少当前写指令的后续写增量的待同步数量
            } else {
                //写指令的写增量还未全部成功同步到块设备
                self
                    .0
                    .deltas_count
                    .store(current_deltas_count
                               .checked_sub(sync_count)
                               .unwrap_or(0),
                           Ordering::Relaxed); //减少当前写指令的写增量的待同步数量
            }
        }

        if let Err(e) = self.0.sender.send(result).await {
            return Err(format!("Callbak by sync failed, reason: {:?}", e));
        }

        Ok(())
    }

    /// 异步等待写指令的写操作同步完成，成功返回写指令编号
    pub async fn wait_write_sync(&self) -> IOResult<u64> {
        loop {
            match self
                .0
                .receiver
                .recv()
                .await {
                Err(e) => {
                    //等待写指令写操作完成失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other, format!("Wait write through failed, deltas_count: {}, reason: {:?}", self.0.deltas_count.load(Ordering::Relaxed), e)));
                },
                Ok(Err(e)) => {
                    //写指令的写增量同步到块设备失败，则表示写指令的写操作失败，并立即返回
                    return Err(Error::new(ErrorKind::Other, format!("Wait write through failed, deltas_count: {}, reason: {:?}", self.0.deltas_count.load(Ordering::Relaxed), e)));
                },
                Ok(Ok(_count)) => {
                    //指定数量的写增量成功同步到块设备
                    if self
                        .0
                        .deltas_count
                        .load(Ordering::Relaxed) == 0 {
                        //写指令的所有写增量成功同步到块设备，则表示写指令的写操作已完成，并立即返回
                        break;
                    }
                },
            }
        }

        Ok(self.0.index.load(Ordering::Relaxed))
    }

    /// 异步等待写指令的后续写操作同步完成，成功返回写指令编号
    pub async fn wait_write_follow_up_sync(&self) -> IOResult<u64> {
        loop {
            match self
                .0
                .receiver
                .recv()
                .await {
                Err(e) => {
                    //等待写指令写操作完成失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other, format!("Wait write through failed, deltas_count: {}, reason: {:?}", self.0.deltas_count.load(Ordering::Relaxed), e)));
                },
                Ok(Err(e)) => {
                    //写指令的写增量同步到块设备失败，则表示写指令的写操作失败，并立即返回
                    return Err(Error::new(ErrorKind::Other, format!("Wait write through failed, deltas_count: {}, reason: {:?}", self.0.deltas_count.load(Ordering::Relaxed), e)));
                },
                Ok(Ok(_count)) => {
                    //指定数量的写增量成功同步到块设备
                    if self
                        .0
                        .follow_up_count
                        .load(Ordering::Relaxed) == 0 {
                        //写指令的所有后续执行的写增量也成功同步到块设备，则表示写指令的后续写操作已完成，并立即返回
                        break;
                    }
                },
            }
        }

        Ok(self.0.index.load(Ordering::Relaxed))
    }
}

// 内部虚拟页写指令
pub struct InnerVirtualPageWriteCmd<
    C: Send + 'static,
    D: VirtualPageWriteDelta<Content = C>,
> {
    index:              AtomicU64,                      //写指令编号
    deltas:             SpinLock<VecDeque<D>>,          //需要执行的写增量队列
    followups:          SpinLock<VecDeque<D>>,          //需要后续执行的写增量队列
    deltas_count:       AtomicUsize,                    //写增量计数
    follow_up_count:    AtomicUsize,                    //后续执行的写增量计数
    sender:             AsyncSender<IOResult<u64>>,     //写增量同步结果发送器
    receiver:           AsyncReceiver<IOResult<u64>>,   //写增量同步结果接收器
}

///
/// 写编号
///
#[derive(Debug, Clone)]
pub struct WriteIndex(u64);

unsafe impl Send for WriteIndex {}
unsafe impl Sync for WriteIndex {}

impl From<u64> for WriteIndex {
    fn from(src: u64) -> Self {
        WriteIndex::new(src)
    }
}

impl From<WriteIndex> for u64 {
    fn from(src: WriteIndex) -> Self {
        src.0
    }
}

impl Deref for WriteIndex {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<u64> for WriteIndex {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl WriteIndex {
    /// 创建写编号
    #[inline]
    pub(crate) fn new(index: u64) -> Self {
        WriteIndex(index)
    }

    /// 创建空的写编号
    #[inline]
    pub fn empty() -> Self {
        WriteIndex(EMPTY_WRITE_INDEX)
    }

    /// 判断是否是空的写编号
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0 == EMPTY_WRITE_INDEX
    }
}
